# Remove Debezium; introduce typed NATS streams + CDC consumer in fluxaorm

## Context

The previous round migrated Kafka → NATS but kept Debezium (Debezium Server sinking MySQL binlog into NATS). Live verification surfaced real friction:

- **Debezium Server 2.7's NATS sink doesn't propagate record keys as NATS headers** (verified empirically). fluxaorm falls back to parsing `after.ID` / `before.ID` from the JSON envelope. The runtime needs Redis-backed offset/history, captures every MySQL table change (not just registered entities), and consumers get `map[string]any` records disconnected from the typed entity model.
- **Out-of-fluxaorm subscribers** — every CDC handler in droplet-backend has to `switch event.Source.Table` and `debezium.StringFromPayload(event.After, "Status")` to do anything with the events. The ORM already emits typed Provider methods (`Fields`, getters); we should leverage them.

This plan removes Debezium entirely and introduces two clean fluxaorm primitives:

1. **Typed NATS streams + generic `StreamConsumer`.** User declares stream names as typed `NatsStreamName` constants and registers stream config explicitly. A small `StreamConsumer` exposes `Consume(ctx, batch, timeout) error` — the caller decides the loop, fluxaorm auto-acks on `nil` handler return.
2. **`CDCConsumer`.** A specialization built on top of `StreamConsumer` that consumes change-data-capture streams populated by fluxaorm. Entities tagged `orm:"dirty=streamName[,...]"` get their inserts/updates/deletes published as typed `*DirtyEvent[T]` records. A generated per-stream typed builder dispatches by entity to user handlers.

The cron-job lifecycle wrapper lives entirely in the application (droplet-backend), not in fluxaorm.

## User-locked design

| Decision | Choice |
|---|---|
| Stream name type | `type NatsStreamName string`. Generator emits typed `CDCStreamRef[B]` values per discovered CDC stream so Go type inference resolves the right typed builder at `NewCDCConsumer` call sites. |
| 1 stream = 1 subject (locked) | CDC subjects: `fluxa.dirty.<streamName>`. All entities tagged into a stream publish here; per-entity dispatch via NATS header `Dirty-Entity` (entity Go type name). |
| 1 stream = 1 consumer group (locked) | JetStream durable auto-derived as `<streamName>-workers`. To run an *independent* consumer of the same change events, declare a second stream (`orm:"dirty=streamA,streamB"`). |
| Entity tag | `orm:"dirty=streamA[,streamB,...]"`. Comma-separated; entity publishes into N streams; multiple entities can share a stream. |
| Payload | Typed `DirtyEvent[T]{Op, ID, Before *T, After *T, TsMs}` via Go generics. |
| Generic stream API | `fluxaorm.NewStreamConsumer(engine, name, handler, opts...) StreamConsumer` with `StreamConsumer.Consume(ctx, batch int, timeout time.Duration) error`. Auto-acks on `nil` handler return; leaves unacked on error (redeliver via JetStream `AckWait`). |
| CDC API | `fluxaorm.NewCDCConsumer(engine, gen.StreamX, opts...)` → typed per-stream builder; `.OnEntity(h, opts...).Build()` → `StreamConsumer`. |
| Stream registration | **Explicit, user-written.** Generator emits typed refs (and per-entity publisher inits). User writes `registry.RegisterCDCStream(gen.StreamX, opts)` for CDC streams (subjects auto-set) and `registry.RegisterNatsStream(name, opts)` for non-CDC streams (user-specified subjects). |
| Validate cross-checks | (a) every entity tagged `dirty=Y` must reference a registered CDC stream `Y`; (b) every registered CDC stream must be referenced by at least one entity; (c) every dirty-tagged entity must have a `RegisterDirtyPublisher[T]` entry. Each is an error at `Validate()`. |
| Lifecycle wrapper | Out of fluxaorm. droplet-backend wraps `StreamConsumer` in its own `cron.Job` (Name/Description/Run/Cleanup), calling `Consume(ctx, 32, 5*time.Second)` in a loop. |

## Approach

### Part A — Remove Debezium

Delete:
- [debezium.go](../../debezium.go), [debezium_schema.go](../../debezium_schema.go)
- [debezium_test.go](../../debezium_test.go), [debezium_schema_test.go](../../debezium_schema_test.go)
- [test_generate/debezium_test.go](../../test_generate/debezium_test.go)
- [docker/debezium-server/](../../docker/debezium-server/)

Modify (removal-only):
- [nats.go](../../nats.go) — drop `NatsBatch.EachDebeziumEvent`
- [nats_schema.go](../../nats_schema.go) — drop per-MySQL-pool `FLUXA_DBZ_<pool>` stream creation
- [registry.go](../../registry.go) — drop `RegisterDebeziumServer`, `debeziumNatsPools`, `debeziumOptions`, the debezium auto-ignore-subjects block
- [engine.go](../../engine.go) — drop `debeziumNatsPools`, `debeziumOptions`
- [entity_schema.go](../../entity_schema.go) — drop `debeziumNatsPool` field and `orm:"debezium=..."` parsing
- [generate_provider.go](../../generate_provider.go) — drop `DebeziumSubjectName(ctx)` emission
- [test.go](../../test.go) — drop `PrepareTablesWithDebezium`
- [docker/docker-compose.yml](../../docker/docker-compose.yml) — drop `debezium-server` service + `orm_volume_debezium`
- [CLAUDE.md](../../CLAUDE.md) — update Key Supporting Files section
- [documentation/MIGRATION-kafka-to-nats.md](../../documentation/MIGRATION-kafka-to-nats.md) — rewrite Debezium section as "removed; superseded by CDC"

### Part B — Build the typed NATS stream + CDC system

#### B1. Core CDC types in new file `cdc_stream.go`

```go
type DirtyOp uint8

const (
    DirtyInsert DirtyOp = 1
    DirtyUpdate DirtyOp = 2
    DirtyDelete DirtyOp = 3
)

// DirtyEvent is the typed change envelope. Generator emits per-entity aliases:
//   type OrdersDirtyEvent = fluxaorm.DirtyEvent[Orders]
type DirtyEvent[T any] struct {
    Op     DirtyOp `json:"op"`
    ID     uint64  `json:"id"`
    Before *T      `json:"before,omitempty"` // nil for Insert
    After  *T      `json:"after,omitempty"`  // nil for Delete
    TsMs   int64   `json:"ts_ms"`
}

// HasFieldChanges: Insert/Delete → always true; Update → true iff at least one
// listed field differs between Before and After.
func (e *DirtyEvent[T]) HasFieldChanges(fields ...Field) bool { ... }
```

Constants (internal):

- `dirtyStreamPrefix = "FLUXA_DIRTY_"` (JetStream stream name format).
- `dirtySubjectPrefix = "fluxa.dirty."` (subject format).
- NATS headers (no reserved `Nats-` prefix): `Dirty-Entity`, `Dirty-Op`, `Dirty-Stream`.
- `Dirty-Entity` value = `reflect.TypeOf((*T)(nil)).Elem().Name()`, precomputed once at publisher registration.

#### B2. Generic stream primitives in new file `stream_consumer.go`

```go
// NatsStreamName is the universal typed handle for any fluxaorm-managed JetStream stream.
type NatsStreamName string

// NatsStreamOptions: full control for non-CDC streams (user-specified subjects, retention, etc.).
type NatsStreamOptions struct {
    Subjects        []string        // required for non-CDC
    Storage         StorageType     // default File
    Retention       RetentionPolicy // default Limits
    Replicas        int             // default 1
    MaxAge          time.Duration   // default 7d
    DuplicateWindow time.Duration   // default 10m
    MaxAckPending   int             // default 256
    AckWait         time.Duration   // default 30s
    MaxDeliver      int             // default -1
    NatsPool        string          // default DefaultPoolCode
}

// StreamHandler is the user's per-message callback. Return nil to ack; return
// err to leave unacked (JetStream redelivers after AckWait).
type StreamHandler func(ctx Context, msg NatsMessage) error

// StreamConsumer is the low-level pull primitive. Callers loop over Consume()
// in whatever lifecycle harness they use (cron.Job, fx lifecycle, goroutine).
// fluxaorm intentionally does NOT provide a blocking Run() method — the
// application owns the loop, retry policy, and shutdown semantics.
type StreamConsumer interface {
    // Consume fetches up to `batch` messages with `timeout` deadline,
    // invokes the handler per message, acks on nil error, leaves unacked on err.
    // Returns the first transport-level error (connection lost, stream gone).
    // Per-message handler errors are logged via fluxaorm's NATS query-logger
    // and do not return from Consume — they only block ack.
    Consume(ctx context.Context, batch int, timeout time.Duration) error
}

type StreamConsumerOption func(*streamConsumerConfig)

func WithDescription(d string) StreamConsumerOption // metrics/log label only

// NewStreamConsumer creates a generic consumer for any registered stream.
// JetStream durable auto-derived as `<streamName>-workers`. Multiple processes
// constructing a consumer for the same stream share workload automatically.
func NewStreamConsumer(engine Engine, stream NatsStreamName, handler StreamHandler, opts ...StreamConsumerOption) StreamConsumer
```

Internals: on first `Consume()` call, the implementation creates (or reuses) the JetStream pull subscription using the engine's NATS pool. Subsequent calls reuse the subscription. Subscription closed via context cancellation in `Consume`.

#### B3. CDC layer on top in `cdc_stream.go`

```go
// CDCStream is the untyped interface used by registration helpers.
type CDCStream interface {
    Name() NatsStreamName
    Subject() string  // fluxa.dirty.<name>
    Durable() string  // <name>-workers
}

// CDCStreamRef ties a stream name to its per-stream generated typed builder.
// `B` is the generated builder type; type inference at NewCDCConsumer's call
// site resolves the right typed return.
type CDCStreamRef[B any] struct {
    name       NatsStreamName
    newBuilder func(core *CDCBuilder) *B
}

func (r CDCStreamRef[B]) Name() NatsStreamName    { return r.name }
func (r CDCStreamRef[B]) Subject() string         { return string(dirtySubjectPrefix) + string(r.name) }
func (r CDCStreamRef[B]) Durable() string         { return string(r.name) + "-workers" }

// Generator-facing factory; called from generated entities/dirty_streams.go.
func NewCDCStreamRef[B any](name NatsStreamName, newBuilder func(*CDCBuilder) *B) CDCStreamRef[B] { ... }

// CDCBuilder is the shared state struct embedded by every generated per-stream
// builder. Exported because the generated code (in the entities package) calls
// AddDispatch on it. Build() returns the wrapped StreamConsumer.
type CDCBuilder struct {
    engine   Engine
    stream   CDCStream
    dispatch map[string]CDCDispatch // entityTypeName -> dispatch closure
    opts     []StreamConsumerOption
}

func (b *CDCBuilder) AddDispatch(entityTypeName string, d CDCDispatch) { b.dispatch[entityTypeName] = d }
func (b *CDCBuilder) Build() StreamConsumer { return newCDCStreamConsumer(b) }

// CDCDispatch is a generic dispatch closure produced by BuildCDCDispatch[T].
type CDCDispatch func(ctx Context, msg NatsMessage) error

// NewCDCConsumer is the single typed constructor. Type parameter B is inferred
// from the stream ref. JetStream durable is the auto-derived `<name>-workers`.
func NewCDCConsumer[B any](engine Engine, stream CDCStreamRef[B], opts ...StreamConsumerOption) *B {
    core := &CDCBuilder{engine: engine, stream: stream, dispatch: map[string]CDCDispatch{}, opts: opts}
    return stream.newBuilder(core)
}

// BuildCDCDispatch wraps a typed handler into the generic CDCDispatch closure.
// Generated OnX methods call it: BuildCDCDispatch[Orders](handler, opts...)
func BuildCDCDispatch[T any](handler func(ctx Context, ev *DirtyEvent[T]) error, opts ...CDCHandlerOption) CDCDispatch { ... }

// CDCEntityName returns the wire/dispatch key for an entity type.
func CDCEntityName[T any]() string  { return reflect.TypeOf((*T)(nil)).Elem().Name() }

type CDCHandlerOption func(*cdcHandlerConfig)

// WatchFields: skip Update events where none of the listed fields differ.
// Insert/Delete always fire. Mirrors today's debezium.WithWatchFields.
func WatchFields(fields ...Field) CDCHandlerOption
```

`newCDCStreamConsumer(b *CDCBuilder) StreamConsumer`: returns a `StreamConsumer` whose handler reads the `Dirty-Entity` header, looks up the dispatch in `b.dispatch`, and invokes it. Unknown entity name → debug log + ack (forward-compatible; new entity added after consumer deployed).

#### B4. Registry surface in `registry.go`

```go
// Registry interface adds:
RegisterNatsStream(name NatsStreamName, opts NatsStreamOptions)
RegisterCDCStream(stream CDCStream, opts CDCStreamOptions)

// CDCStreamOptions is the subset of NatsStreamOptions the user can tune for
// CDC streams (subjects, retention, and dedup window are fluxaorm-controlled).
type CDCStreamOptions struct {
    Storage         StorageType   // default File
    Replicas        int           // default 1
    MaxAge          time.Duration // default 7d
    DuplicateWindow time.Duration // default 10m
    MaxAckPending   int           // default 256
    AckWait         time.Duration // default 30s
    MaxDeliver      int           // default -1
    NatsPool        string        // default DefaultPoolCode
}
```

Both methods populate `registryImpl.natsStreams map[NatsStreamName]*resolvedStreamConfig`. `RegisterCDCStream` fills the subject list as `[ref.Subject()]`, retention as `LimitsPolicy`, and sets `isCDC=true` on the entry.

At `Validate()`:
1. For every entity with `len(dirtyStreams) > 0`, every named stream must be registered AND `isCDC=true` — else error: *"entity 'X' tagged `dirty=Y` but stream Y is not a registered CDC stream; call `registry.RegisterCDCStream(gen.StreamY, ...)`."*
2. For every registered CDC stream, at least one entity must reference it — else error: *"CDC stream Y is registered but no entity is tagged `dirty=Y`."*
3. For every dirty-tagged entity, a `RegisterDirtyPublisher[T]` entry must exist — else error: *"entity 'X' tagged dirty= but no publisher is registered; run `make generate`."*

#### B5. Stream reconciliation in `nats_schema.go`

`collectDesiredStreams` walks `registry.natsStreams`. For each entry, materialize a JetStream stream:
- Name: `dirtyStreamPrefix + string(name)` for CDC, otherwise `string(name)`.
- Subjects, Storage, Retention, Replicas, Duplicates, MaxAge: from options.

Drop the existing per-MySQL-pool `FLUXA_DBZ_<pool>` stream construction.

#### B6. Entity tag parsing in `entity_schema.go`

Replace the deleted `debezium=...` block with:

```go
dirtyTag := e.getTag("dirty", "", "")
if dirtyTag != "" {
    streams := splitAndTrim(dirtyTag, ",")
    if len(streams) == 0 {
        return fmt.Errorf("orm:\"dirty=...\" on entity '%s' is empty", entityType.Name())
    }
    seen := make(map[NatsStreamName]bool, len(streams))
    e.dirtyStreams = make([]NatsStreamName, 0, len(streams))
    for _, s := range streams {
        if !validNatsStreamName(s) {
            return fmt.Errorf("invalid stream name '%s' on entity '%s' (must match [a-zA-Z0-9_-]+)", s, entityType.Name())
        }
        name := NatsStreamName(s)
        if seen[name] {
            return fmt.Errorf("duplicate stream '%s' on entity '%s'", s, entityType.Name())
        }
        seen[name] = true
        e.dirtyStreams = append(e.dirtyStreams, name)
    }
}
```

New field on `entitySchema`: `dirtyStreams []NatsStreamName`. Order preserved (publishes happen in declared order).

#### B7. Capture & publish on the sync path in `flush.go`

After DB+Redis pipelines succeed, before `PrivateFlushed()` clears origin/bind state:

```go
for each tracked entity e:
    schema := engineSchemaFor(e)
    if len(schema.dirtyStreams) == 0 { continue }
    op, beforeOrigin := e.PrivateFlushEvent()
    if op == 0 { continue }
    if err := engine.publishDirty(orm, schema, e, op, beforeOrigin); err != nil { return err }
```

`engine.publishDirty`:
1. Look up publisher entry by `cacheIndex` (engine-internal map populated at `Validate()`). Entry caches the precomputed `entityTypeName`.
2. Call `entry.buildEvent(*T, op, beforeOrigin) *DirtyEvent[T]` (framework adapts `Entity → *T`).
3. JSON-marshal once.
4. For each `name` in `schema.dirtyStreams`: publish to `fluxa.dirty.<name>` with headers `Dirty-Entity` (entityTypeName), `Dirty-Op` (numeric), `Dirty-Stream` (name), fresh `Nats-Msg-Id`.
5. First error short-circuits.

DELETE pre-loading: not needed sync path — `originDatabaseValues` intact at publish time.

At-least-once: NATS outage → `Flush()` returns error, MySQL is committed. Operator retries; 10m dedup window covers retries within window.

#### B8. Capture & publish on the async path in `flush_async.go`

The async-SQL consumer already loads entities post-execution for hook firing. For UPDATE, `AsyncEntityEvent.Changes` carries before-column-values; for DELETE, the consumer pre-loads.

Extend `AsyncEntityEvent`:
```go
DirtyStreams []NatsStreamName // copied from schema at producer-side; consumer doesn't re-look-up
```

In the consumer's per-message loop, after MySQL succeeds and before user `On*` hooks fire:

```go
for _, ev := range op.Events:
    if len(ev.DirtyStreams) == 0 { continue }
    engine.publishDirtyAsync(consumeCtx, ev, preLoadedDeleteEntities[ev.EntityID])
```

The publisher reconstructs the typed `*T` from loader output and overlays `ev.Changes` to build Before.

#### B9. Code generation

Generated artifacts:

**(a) Per-entity event type alias** — emitted by `generate_provider.go` when `len(schema.dirtyStreams) > 0`:

```go
type OrdersDirtyEvent = fluxaorm.DirtyEvent[Orders]
```

**(b) Typed stream refs** — emitted to `entities/dirty_streams.go`:

```go
// Code generated by fluxaorm; DO NOT EDIT.
package entities

import "github.com/latolukasz/fluxaorm/v2"

var (
    StreamOrderIndexer = fluxaorm.NewCDCStreamRef[OrderIndexerCDCBuilder]("order-indexer", newOrderIndexerCDCBuilder)
    StreamOrderCreated = fluxaorm.NewCDCStreamRef[OrderCreatedCDCBuilder]("order-created", newOrderCreatedCDCBuilder)
)
```

**No `init()` block here.** Stream JetStream config is registered explicitly by the user via `registry.RegisterCDCStream(gen.StreamX, opts)` in their app code. If they forget, `Validate()` fails clearly.

**(c) Per-entity publisher init() block** — emitted into the per-entity generated file:

```go
func init() {
    fluxaorm.RegisterDirtyPublisher[Orders](
        []fluxaorm.NatsStreamName{StreamOrderIndexer.Name(), StreamOrderCreated.Name()},
        func(entity *Orders, op fluxaorm.DirtyOp, beforeOrigin map[string]any) *OrdersDirtyEvent {
            ev := &OrdersDirtyEvent{Op: op, ID: entity.GetID(), TsMs: time.Now().UnixMilli()}
            switch op {
            case fluxaorm.DirtyInsert:
                ev.After = snapshotAfter(entity)
            case fluxaorm.DirtyUpdate:
                ev.Before = snapshotFromOrigin(entity, beforeOrigin)
                ev.After  = snapshotAfter(entity)
            case fluxaorm.DirtyDelete:
                ev.Before = snapshotAfter(entity)
            }
            return ev
        },
    )
}
```

Public API in `cdc_stream.go`:

```go
func RegisterDirtyPublisher[T any](
    streams []NatsStreamName,
    buildEvent func(entity *T, op DirtyOp, beforeOrigin map[string]any) *DirtyEvent[T],
)
```

Internally stored in a package-level `sync.Map` keyed by `reflect.TypeOf((*T)(nil)).Elem()`. Engine `Validate()` copies entries into the engine-internal `cacheIndex → entry` map. `cacheIndex` and `Entity` adaptation stay entirely internal.

`snapshotAfter` and `snapshotFromOrigin` are private helpers generated per entity in the same file (typed getters + `privateGetOriginalColumnValue`).

**(d) Per-stream typed builder** — one file per discovered stream, e.g. `entities/order_indexer_cdc.go`. Composes the public `CDCBuilder`; the generated builder is *the registration interface* for that stream's entity set (compile-checked):

```go
type OrderIndexerCDCBuilder struct {
    *fluxaorm.CDCBuilder // embedded: Build() inherited
}

func newOrderIndexerCDCBuilder(core *fluxaorm.CDCBuilder) *OrderIndexerCDCBuilder {
    return &OrderIndexerCDCBuilder{CDCBuilder: core}
}

// One OnX method per entity tagged `dirty=order-indexer`:
func (b *OrderIndexerCDCBuilder) OnOrders(
    handler func(ctx context.Context, ormCtx fluxaorm.Context, ev *OrdersDirtyEvent) error,
    opts ...fluxaorm.CDCHandlerOption,
) *OrderIndexerCDCBuilder {
    b.AddDispatch(fluxaorm.CDCEntityName[Orders](), fluxaorm.BuildCDCDispatch[Orders](handler, opts...))
    return b
}
```

#### B10. Engine plumbing

`engineImplementation` gets:
- `dirtyPublishersByCacheIndex map[uint64]*dirtyPublisherEntry`
- Helpers (not exported): `publishDirty(ctx, schema, entity, op, beforeOrigin) error`, `publishDirtyAsync(ctx, ev, preLoaded) error`.

Drop debezium fields.

#### B11. Logging

Reuse fluxaorm's existing query-logger (`source: "nats"`). No new `DirtyLogger` interface. Publish failures and dispatch errors flow through the standard pipeline. droplet-backend can register its own `LogHandler` to push these into its zap logger.

#### B12. Test helpers in `test.go`

`PrepareTablesWithCDC(t, registry, entities...)`:
- Registers entities; auto-discovers stream names from tags.
- **Test convenience only**: auto-registers each referenced CDC stream with `CDCStreamOptions{}` defaults so tests don't have to mirror production registration verbatim.
- Runs `prepareTables` + `applyNatsAlters` so streams exist + are purged before tests publish.

Production code is expected to call `RegisterCDCStream` explicitly.

#### B13. Example droplet-backend integration (out of fluxaorm)

In droplet-backend (NOT in fluxaorm itself):

```go
// Step 1 — register CDC streams in the cron module setup (or any startup hook):
fluxaorm.RegisterCDCStream(registry, gen.StreamProductIndexer, fluxaorm.CDCStreamOptions{
    MaxAge: 30 * 24 * time.Hour,
})

// Step 2 — build the CDC consumer:
consumer := fluxaorm.NewCDCConsumer(engine, gen.StreamProductIndexer,
    fluxaorm.WithDescription("Product elasticsearch indexer"),
).
    OnProducts(job.handleProduct).
    OnVariants(job.handleVariant).
    OnOffers(job.handleOffer).
    Build()  // *fluxaorm.StreamConsumer

// Step 3 — wrap in cron.Job (droplet-backend code, fluxaorm-agnostic):
type ProductIndexListenerJob struct {
    consumer fluxaorm.StreamConsumer
    ormCtx   fluxaorm.Context
}

func (j *ProductIndexListenerJob) Name() string            { return "product_index_listener_job" }
func (j *ProductIndexListenerJob) Description() string     { return "Product elasticsearch indexer" }
func (j *ProductIndexListenerJob) Interval() time.Duration { return 0 }
func (j *ProductIndexListenerJob) Unique() bool            { return false }
func (j *ProductIndexListenerJob) HasSideEffect() bool     { return false }
func (j *ProductIndexListenerJob) Cleanup()                {}

func (j *ProductIndexListenerJob) Run(ctx context.Context) error {
    for ctx.Err() == nil {
        if err := j.consumer.Consume(j.ormCtx, 32, 5*time.Second); err != nil {
            return err
        }
    }
    return nil
}

func (j *ProductIndexListenerJob) handleProduct(ctx context.Context, ormCtx fluxaorm.Context, ev *gen.ProductsDirtyEvent) error {
    if ev.Op == fluxaorm.DirtyDelete { return j.productSearch.Delete(ctx, ev.ID) }
    return j.reindex(ctx, ormCtx, ev.ID)
}
```

The `switch event.Source.Table` block is gone — routing is compile-checked. `ev.Before` and `ev.After` are typed `*gen.Products` / `*gen.Variants` / `*gen.Offers`. `WatchFields` callers like [show_lifecycle_listener.go:47](../../../droplet-backend/internal/cron/jobs/shows/show_lifecycle_listener.go#L47) become `.OnShows(job.handle, fluxaorm.WatchFields(gen.ShowsProvider.Fields.Status))`.

## Critical files to modify or add

Delete:
- [debezium.go](../../debezium.go), [debezium_schema.go](../../debezium_schema.go)
- [debezium_test.go](../../debezium_test.go), [debezium_schema_test.go](../../debezium_schema_test.go)
- [test_generate/debezium_test.go](../../test_generate/debezium_test.go)
- [docker/debezium-server/](../../docker/debezium-server/)

Add:
- [cdc_stream.go](../../cdc_stream.go) — `DirtyEvent[T]`, `DirtyOp`, `CDCStream`, `CDCStreamRef[B]`, `CDCBuilder`, `NewCDCConsumer[B]`, `BuildCDCDispatch[T]`, `CDCEntityName[T]`, `WatchFields`, `RegisterDirtyPublisher[T]`, `CDCStreamOptions`
- [stream_consumer.go](../../stream_consumer.go) — `NatsStreamName`, `NatsStreamOptions`, `StreamHandler`, `StreamConsumer`, `NewStreamConsumer`, `WithDescription`
- [generate_cdc_stream.go](../../generate_cdc_stream.go) — generator extension
- [test_generate/cdc_stream_test.go](../../test_generate/cdc_stream_test.go) — E2E

Modify:
- [registry.go](../../registry.go) — `RegisterNatsStream`, `RegisterCDCStream`; cross-checks in `Validate()`; drop `RegisterDebeziumServer`
- [engine.go](../../engine.go) — publisher map + helpers; drop debezium fields
- [entity_schema.go](../../entity_schema.go) — `dirtyStreams []NatsStreamName`, parse `orm:"dirty=..."`; drop debezium parsing
- [nats.go](../../nats.go) — drop `EachDebeziumEvent`
- [nats_schema.go](../../nats_schema.go) — drive stream creation from `registry.natsStreams`; drop FLUXA_DBZ_*
- [flush.go](../../flush.go) — invoke `publishDirty` after SQL+Redis success
- [flush_async.go](../../flush_async.go) — `AsyncEntityEvent.DirtyStreams`; publish in consumer
- [generate_provider.go](../../generate_provider.go) — drop `DebeziumSubjectName`; emit `DirtyEvent` type alias + `RegisterDirtyPublisher` init per dirty-tagged entity
- [generate.go](../../generate.go) — emit `entities/dirty_streams.go` (typed refs) + per-stream `<stream>_cdc.go` builder file
- [test.go](../../test.go) — `PrepareTablesWithCDC`; drop `PrepareTablesWithDebezium`
- [docker/docker-compose.yml](../../docker/docker-compose.yml) — drop debezium-server + volume
- [documentation/MIGRATION-kafka-to-nats.md](../../documentation/MIGRATION-kafka-to-nats.md) — rewrite Debezium section as "removed"
- [CLAUDE.md](../../CLAUDE.md) — update architecture description
- [test_generate/generate_test.go](../../test_generate/generate_test.go) — rename `generateEntityDebezium` → `generateEntityDirty` with `orm:"dirty=test_stream"`; update assertions

## Risks

1. **At-least-once + post-MySQL publish.** NATS outage → `Flush()` returns error after MySQL is committed. Consumers must tolerate replay; 10m dedup window covers retries within window. Operator-driven replays after long outages bypass dedup — document.
2. **Stream registration responsibility shifts to user.** Forgetting `RegisterCDCStream` for a tagged entity = `Validate()` error. Caught at startup, not in production. Document in migration guide.
3. **Generated code churn.** Dirty-tagged entities regenerate with new `init()` blocks; new `dirty_streams.go` and per-stream builder files. Document that `make generate` is required after upgrade.
4. **Snapshot size.** Before+After doubles payload vs column-diff. Flag for large-blob entities; per-field skip annotation is a future addition, not v1.
5. **Routing on shared streams.** Multiple entities funnel into one stream; dispatch by `Dirty-Entity` header. Handlers absent on the builder = debug log + ack (safer than panic for forward-compat).
6. **JetStream durable lifetime.** The auto-derived `<stream>-workers` durable persists across deploys. Replays of unacked messages can flood after a long outage. droplet-backend should set sensible `MaxDeliver`/backoff via `CDCStreamOptions` (default `-1` may not be appropriate for all CDC consumers).

## Verification

1. **Build & lint**: `go vet ./...`, `make check` clean; `go mod tidy` no diff.
2. **Unit tests** (root pkg):
   - JSON round-trip for `DirtyEvent[T]` insert/update/delete shapes.
   - `NatsStreamName` validation rejects invalid characters.
   - `RegisterDirtyPublisher[T]` registers under the reflect Type; duplicate registration errors.
   - Entity-schema parser: rejects empty tag, invalid stream names, duplicate streams in same tag.
   - `Validate()` rejects: (a) entity tagged for unregistered CDC stream; (b) CDC stream registered with no entity; (c) entity tagged but no publisher entry.
   - `WatchFields`: false for Update with no relevant changes; true for Insert/Delete.
   - `NewStreamConsumer` with unregistered stream → error on first `Consume()`.
3. **Generator tests** (`test_generate/`):
   - `TestGenerate` regenerates `generateEntityDirty` (replacing `generateEntityDebezium`); generated file contains `GenerateEntityDirtyDirtyEvent`, `init()` calling `RegisterDirtyPublisher[GenerateEntityDirty]`, the `dirty_streams.go` typed-refs file, and `<stream>_cdc.go` builder file.
   - Generated builder compiles and exposes `OnGenerateEntityDirty(handler, opts...)`.
4. **End-to-end** (`test_generate/cdc_stream_test.go`):
   - **Sync path**: insert → `{Op:Insert, Before:nil, After:*Entity}`; update → both populated; delete → before populated, after nil.
   - **Async path**: same operations via `FlushAsync` → consumer eventually receives equivalent events.
   - **Multi-entity dispatch**: two entities sharing one stream → both `OnX` and `OnY` fire on respective rows.
   - **Multi-stream per entity**: `dirty=a,b` → two consumers each receive every event for that entity. Stream-a sees only entities tagged into it.
   - **WatchFields filtering**: handler skipped when Update doesn't touch watched columns.
   - **Multi-worker scale-out**: two `StreamConsumer`s built from the same `gen.StreamX` ref → each receives a non-overlapping subset; together they consume the full set (validates JetStream consumer-group semantics via the shared auto-derived durable).
   - **Independent consumers of same events**: entity tagged `dirty=a,b` → consumer-A and consumer-B each receive every event (each stream has its own durable).
   - **Auto-ack contract**: handler returning `err` → message redelivered after `AckWait`; handler returning `nil` → message acked once.
5. **Live integration** (no Debezium Server):
   - `docker compose up -d mysql redis nats` (debezium-server is gone).
   - `make test` green.
   - `nats stream get FLUXA_DIRTY_test_stream 1 --json`: subject `fluxa.dirty.test_stream`, headers `Dirty-Entity` + `Dirty-Op` + `Dirty-Stream`, JSON body `{op, id, before, after, ts_ms}`.
   - `nats consumer info FLUXA_DIRTY_test_stream test_stream-workers`: durable exists with auto-derived name.
6. **Documentation**: `grep -rli "debezium" .` returns no hits in production code or current docs (allowed only in migration changelog context).
