# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Sync

Every change to the ORM code must be accompanied by a corresponding update to this file. This includes API changes, new features, removed features, behavior changes, and bug fixes that affect documented behavior.

## Commands

```bash
make test          # Run all tests (go test -race -p 1 ./...)
make check         # Run linting (revive, gocyclo, format check, no fmt.Print*/spew.Dump)
make format        # Format code with goimports
make cover         # Run tests with coverage → resources/cover/cover.out
make cover-html    # Generate and open HTML coverage report
make tidy          # Run go mod tidy
```

To run a single test:
```bash
go test -race -p 1 -run TestName ./...
```

Tests require running MySQL, Redis (8.2 or newer), ClickHouse and NATS with JetStream enabled. Use `docker/docker-compose.yml` with `docker/.env` for setup (env vars `LOCAL_IP`, `MYSQL_PORT`, `REDIS_PORT`, `CLICKHOUSE_PORT`, `CLICKHOUSE_HTTP_PORT`, `NATS_PORT`, `NATS_HTTP_PORT`). The tests dial the ports from `docker/.env`: MySQL `localhost:3397` (database `test`, `root:root`), Redis `localhost:6395` (db 0 and db 1), ClickHouse `localhost:9942` (HTTP `9943`), NATS `nats://localhost:9944` (monitoring `9945`).

`test_generate/get_by_ids_benchmark_test.go` adds `BenchmarkGetByIDs10` with `ContextCacheHit`, `RedisCacheHit` and `MySQL` cases. It fetches 10 existing IDs from equivalent 27-column fixtures with all nullable fields populated; one operation is the whole batch. Only `GetByIDs` is timed: fixture/connection setup, context creation, getters and SQL/Redis path checks are outside the measurement. The Redis and MySQL cases disable the context cache; MySQL uses the fixture without Redis caching, excluding Redis miss/fill work.

After generating `test_generate/entities/`, point the benchmark at dedicated MySQL and Redis services:
```bash
FLUXAORM_BENCH_MYSQL_DSN='root:root@tcp(localhost:13397)/test' \
FLUXAORM_BENCH_REDIS_ADDR='localhost:16395' \
go test ./test_generate -run '^$' -bench '^BenchmarkGetByIDs10$' -benchmem -count=5
```
Both variables are required (otherwise the benchmark skips); `FLUXAORM_BENCH_REDIS_DB` defaults to `0`. It creates missing fixture tables, fails on an incompatible existing fixture schema, and cleans up only its inserted rows and exact Redis keys. Created tables remain. Use a normal build for allocation measurements; `-race` adds instrumentation overhead.

`test_generate/get_by_id_benchmark_test.go` adds `BenchmarkGetByID1` with `ContextCacheHit`, `RedisCacheHit` and `MySQL` cases for one existing entity. It shares the 27-column fixture setup and service environment variables with `BenchmarkGetByIDs10`; setup still seeds ten rows, while each timed operation fetches only the first ID. Context creation, warm-up, getters and query-path checks are outside timing. Redis and MySQL disable the context cache; the Redis case verifies one direct `LRANGE`, and MySQL uses the equivalent fixture without Redis caching. Run with `go test ./test_generate -run '^$' -bench '^BenchmarkGetByID1$' -benchmem -count=5` and the same explicit `FLUXAORM_BENCH_MYSQL_DSN` / `FLUXAORM_BENCH_REDIS_ADDR` configuration as above. `B/op` and `allocs/op` describe one entity read.

Linting is configured in `revive.toml`. Cyclomatic complexity threshold is 100. `fmt.Print*` and `spew.Dump` calls are banned by `make check`.

Pushes to `v2` and release tags matching `v2.*` run `.github/workflows/release.yml` (also available manually). Verify the branch commit before creating its release tag. It reads Go from `go.mod`, starts disposable MySQL on 3397, Redis on 6395, ClickHouse on 9942/9943 and NATS JetStream on 9944, regenerates test entities, then runs `go build ./...`, the full race-enabled test suite, `go vet ./...` and the targeted CDC delivery tests. The workflow requires only `contents: read` and does not publish a GitHub release; publication follows successful verification.

## Architecture

FLUXA ORM is a **code-generation-based** Go ORM targeting MySQL + Redis 8.2+ (`Validate()` rejects older Redis). Go 1.25. The main package is everything at the root level (`github.com/latolukasz/fluxaorm/v2`).

### Core Flow

1. **Registry** (`registry.go`) — configure entity types, connection pools (MySQL, Redis, ClickHouse, NATS), ClickHouse tables, NATS streams/consumers, `ConsumerDef`s, tasks and metrics via `NewRegistry()`. There is no plugin system.
2. **Engine** (`engine.go`) — immutable runtime object created via `registry.Validate()`; holds connection pools and all entity schemas
3. **Context** (`orm.go`) — created from Engine per-request (`Context` interface / `ormImplementation`); the main API surface for ORM operations

### Entity Definition

Entities are plain Go structs with an `ID` field and optional `orm:` struct tags. Field types and caching are declared on the struct; schema metadata is extracted at `Validate()` time via reflection.

Key struct tags:
- `orm:"redisCache"` — enable Redis List cache for this entity
- `orm:"cached"` — additionally cache the *set of all ids* for this entity and generate `GetAll(ctx)`. Requires `redisCache` on the same tag (registration fails otherwise). Meant for small, rarely written reference tables
- `orm:"required"` — NOT NULL / required field
- Indexes are defined via interfaces: `EntityIndexes` (non-unique), `EntityUniqueIndexes` (unique), `EntityCachedUniqueIndexes` (cached unique). Each returns `[][]string` (list of column groups). Index names are auto-generated by joining columns with `_`.
- `orm:"enum=a,b,c"` / `orm:"set=a,b,c"` — MySQL ENUM/SET column
- `orm:"enumName=TypeName"` — share an enum type across fields
- `orm:"time"` — store as DATETIME (default is DATE for `time.Time` fields)
- `orm:"redisSearch=poolName"` — enable Redis Search indexing for this entity (on ID field only; uses `DefaultPoolCode` if omitted)
- `orm:"searchable"` — include field in Redis Search index
- `orm:"sortable"` — add SORTABLE flag to Redis Search index field (requires `searchable`)

Entity registration: `registry.RegisterEntity(&MyEntity{})`, then call `registry.Validate()`.

### Code Generation

`Generate(engine Engine, outputDirectory string)` produces one `.go` file per entity in the given directory. The generator is split across:

- `generate.go` — entry point, file I/O, `codeGenerator` struct
- `generate_entity.go` — `XxxEntity` struct body
- `generate_entity_struct.go` — entity struct scaffolding
- `generate_write_state.go` — per-write snapshots, SQL baseline advancement and rollback rebasing
- `generate_fields.go` — per-field SQL row and Redis serialisation helpers
- `generate_getters.go` / `generate_getters_nullable.go` — typed getters & setters
- `generate_provider.go` — `XxxProvider` singleton, `XxxSQLRow`, `redisValues()`
- `generate_typed_fields.go` — the typed `Fields` descriptor struct on the provider
- `generate_query.go` — `GetByID`, `GetByIDs`, `Search*`, `New`, etc.
- `generate_providers.go` — `providers.go` with `AllProviders`
- `generate_consumer.go` — per-entity CDC publisher block, `consumers.go` and one `<name>_consumer.go` per declared consumer (only when consumers/tasks are registered)

**Generated output structure (per entity):**

- **`XxxSQLRow` struct** — flat struct with fields `F0`, `F1`, `F2`... for reflection-free `Scan()`
- **`XxxProvider` singleton** — holds static metadata (tableName, dbCode, redisCode, cacheIndex, redisCachePrefix, stamp, TTL, and `redisAllKey` when the entity is tagged `cached`) and exposes all query methods. `cacheIndex` is the fully qualified entity type name as a string (e.g. `"app/entities.User"`); it keys `afterInsertHandlers`/`afterUpdateHandlers`/`afterDeleteHandlers` and `ormImplementation.cachedEntities`. Using the type name keeps the value stable across registrations and self-documenting in payloads and logs.
- **`XxxEntity` struct** — user-facing entity; holds `ctx`, `id`, `new`, `deleted`, `removed` (hard-delete tombstone), `snapshot` (read-only callback view), `originDatabaseValues` (SQLRow)

### Caching (Two Tiers)

1. **Context cache (identity map)** — per-Context in-memory map inside `ormImplementation`; **enabled by default**, and it **never expires**: one row is one `*Entity` for the whole life of the Context. Populated only by `GetByID`, `GetByIDs` and `New` (not by `Search*`). Disable with `ctx.DisableContextCache()`. Low-level hooks `GetFromContextCache` / `SetInContextCache` are exported on the `Context` interface so generated code can use them across packages. Use `ctx.Reload(entities...)` to make a handle current — see below.
2. **Redis cache** (`redis_cache.go`) — entity stored as a Redis List; index 0 = struct hash stamp, indices 1..N = serialised field values. Writes invalidate; they never write back.

**All-ids cache (`orm:"cached"`).** An entity tagged `cached` also gets its full id set cached, and a generated `GetAll(ctx) ([]*Entity, error)` to read it. The set is one Redis List at `redisAllKey` = `<cacheKey>:all` — index 0 is the same struct hash stamp, indices 1..N are decimal ids in `ID` order. It lives under the row-cache prefix on purpose, so `ClearRedisCache` wipes it along with the rows and `validateRedisKeyNamespaces` already covers it.

`GetAll` reads the set, then hands the ids to `GetByIDs`. So a warm call touches MySQL zero times: one `LRange` for the set, one pipelined batch for the rows. On a miss it runs `SELECT \`ID\` FROM \`table\` [WHERE \`FakeDelete\` = 0] ORDER BY \`ID\``, caches the result and continues. An empty table caches as a stamp-only list, which is what tells a genuinely empty table apart from a cold key.

The set is **invalidated, never patched**. All three write paths (insert, update, delete) register `redisAllKey` with `InvalidateCacheKey` alongside the row key, so it inherits the existing guarantees for free: the double delete around the statement, and the discard on rollback. Update is included because a soft delete changes membership without deleting a row. The cost is that any write to the table forces the next `GetAll` to re-`SELECT` the ids — acceptable for what the tag is for, and the reason it should not be put on a hot table.

Two guards: `GetAll` skips Redis entirely inside a transaction (`!ctx.InTransaction()`, same gate as `GetByID` — a cached set would hide the in-flight write and a fill would publish uncommitted ids), and it refuses to cache a set larger than `MaxCachedAllRows` (10000, `cache_invalidation.go`). Past that limit it still answers correctly, straight from MySQL.

There is no local (per-process) cache. One was implemented but no read path ever consulted it, so it was removed.

The identity map used to expire on a 1-second clock. That made "is this handle current?" depend on how slow the request was, which is untestable and inverted under load — a lock that re-read the row got fresh data only when contention was high enough to push the wait past the TTL. The clock is gone. Because nothing expires, no dirty set is needed either: `Track`, `untrack` and `trackedEntities` were removed with it.

### Batched Reads (`GetByIDs`)

Generated `GetByIDs` returns one slice in first-occurrence ID order and omits missing rows. Small batches use stack scratch for deduplication and pending result positions; larger batches use an ID-to-position map. Context, Redis and SQL results fill the same result slice. SQL query construction reuses a digit buffer, and one Scan argument array is reused across rows while each returned entity retains its own SQL row storage. Redis pipeline result handles are stored by value. These changes reduce temporary allocations without sharing returned entity buffers or changing cache behavior. Regenerate providers to adopt the optimized implementation; the method signature is unchanged.

Generated batch readers pass numeric Scan destinations through `SQLScanTarget`. Its pointer adapters avoid temporary strings on successful numeric conversions and preserve `database/sql` NULL, overflow and conversion-error behavior through standard fallbacks. FLOAT values retain the standard decimal float32-to-float64 conversion. Each returned entity still owns its SQL row storage.

Redis batch readers use `LRangeBatchInto` with caller-owned result handles. Commands and arguments are allocated together for the batch and queued as native go-redis `StringSliceCmd` values; normal LRANGE decoding, errors, logging and metrics remain in use. Decoded entity data remains independent of the temporary command batch. Regenerate providers with the matching updated ORM runtime to use both helpers.

### Single Reads (`GetByID`)

Generated `GetByID` returns context-cache hits before constructing query or Redis buffers. Other reads format the uint64 ID with `strconv.AppendUint` using stack scratch. Schemas without MySQL FLOAT columns use the decimal ID directly in SELECT, avoiding a transient prepared statement. Schemas with `float32` or `*float32` fields without a `decimal` tag retain the parameterized query: MySQL text replies can round FLOAT values differently from binary replies. The decision uses flattened field metadata, including nested fields. Both paths use `SQLScanTarget` while retaining QueryRow logging, metrics, error propagation and independent entity row storage.

The Redis `LRange` runtime keeps its native `StringSliceCmd` and argument array in one allocation and uses `Client.Process`, preserving native hooks, decoding, retry, logging and metrics. Command/result storage is fresh per call. Regenerate application providers with the updated ORM runtime to adopt the generated GetByID changes; the method signature and row-cache format are unchanged.

### Reload (`reload.go`)

`ctx.Reload(entities ...Entity) error` re-reads each entity **in place**. The pointer is unchanged, so every holder of the row — including the identity map — sees the fresh values. That is the point: evicting and re-reading would instead produce a *second* handle on one row, with its own origin snapshot and bind, which is the lost-update the identity map exists to prevent.

- Bypasses the Redis row cache, always MySQL. A cached row is exactly what a caller reloading under a lock refuses to trust.
- Inside a transaction it reads through that transaction, so it sees the transaction's own writes.
- Refuses an entity with unsaved changes (`ErrEntityUnsavedChanges`) — reload discards them, and doing that silently to a handle an outer caller is still mutating is the same bug in another shape.
- Refuses a `new` entity (`ErrEntityNotPersisted`); reports a deleted row as `ErrEntityVanished`.
- Backed by generated `PrivateReload()`, which is on the `Entity` interface — adding it forces every consumer to regenerate before upgrading.

### Dirty Tracking in Generated Code

- Getters read from (priority): `databaseBind` → `originRedisValues` → `originDatabaseValues`
- Setters compare new value against current origin; no-op if unchanged; otherwise update bind maps
- `PrivateFlush()` enqueues INSERT/UPDATE/DELETE on the DB pipeline. It does **not** write the row cache back: `Save` invalidates the stale keys instead, before the statement and again after commit.
- `PrivateReload()` replaces `originDatabaseValues` from MySQL and clears `originRedisValues`, so a getter cannot keep answering from a stale redis-loaded row.

### Repeated Saves and Transaction State

Every successful `Save` advances the live entity's baseline to the values actually written by that call, including inside an explicit transaction. A newly inserted entity can be updated or deleted by a later call in the same transaction; an unchanged save emits no SQL or event. Returning a field to an earlier value after a successful save is a real change against the latest written baseline.

`PrivateSnapshot()` freezes the SQL origin, bind and event metadata after before-callbacks. CDC, outbox and after-callbacks use one snapshot per SQL write and still run only after commit. After-callbacks receive a distinct read-only entity view: `Save`, `Delete`, `ForceDelete` and `Reload` reject it with `ErrEntityReadOnly`. Reference getters retain the originating context. `PrivateAdvance(snapshot)` preserves edits made after the captured statement, including mutations by a later entity's before-callback in a batch. Those edits remain unsaved after commit, including when post-commit work fails.

A transaction retains the first baseline snapshot for each entity. On rollback, panic or failed commit, `PrivateRollback(snapshot)` restores the original persisted baseline while retaining the latest saved and unsaved field values for a retry. A rolled-back insert becomes new again; a rolled-back hard delete remains pending for retry. In a partial multi-pool commit, only uncommitted pools are restored; already committed entities retain their durable baseline. A failed `Save` discards pending queues and marks an explicit transaction rollback-only even if the caller swallows its error.

Generated code must be regenerated for this behavior. `Save` rejects older entities with `ErrEntityNeedsRegeneration` before staging SQL. The required generated capability comprises `PrivateSnapshot`, `PrivateAdvance`, `PrivateRollback`, `PrivateIsSnapshot`, `PrivateIsDeleted` (latest saved deletion state) and `PrivateDatabasePool`. Historical delete events only evict an identity-map entry when that live handle is still saved as deleted and still occupies the entry; restoring a soft delete or replacing a deleted handle cannot evict a newer live object.

### ID Generation

Snowflake (`newSnowflakeGenerator`). Reference columns are rejected at registration if their declared ID type is too narrow to hold a snowflake ID.

### Redis Search

Entities opt in to Redis Search (FT.SEARCH) indexing via struct tags on the ID field and individual fields. The ORM manages index lifecycle and hash document writes automatically.

- `redis_search.go` — `RedisSearchWhere` query builder, `RedisSearchAlter`, `GetRedisSearchAlters(ctx)` (returns pending FT.CREATE operations, similar to `GetAlters`)
- Index name = `<tableName>_<8-char FNV hash of field definitions>` — index is recreated when schema changes
- Hash key prefix = `<5-char FNV hash of "<tableName>:search">:h:` + entity ID
- `PrivateFlush()` maintains Redis hashes: Del+HSet on INSERT/UPDATE, Del on DELETE; soft-deleted entities are removed from the index
- Generated query methods: `SearchOneInRedis`, `SearchManyInRedis`, `SearchManyInRedisWithTotal`, `ReindexRedisSearch`
- Type mapping: numeric types + bool + time.Time + Reference → NUMERIC; string → TEXT; enum/set → TAG

### Key Supporting Files

- `entity_schema.go` — `entitySchema` struct; all per-entity metadata (columns, indexes, caching, enums, references, struct hash). **The row-cache prefix names only `(pool, table)`** — 8 hex of sha256. It used to include the column list, so any schema change moved the key space and two versions in a rolling deploy could not see each other's invalidations. The stamp (element 0 of the cached value list) is sha256 of the full column list at 16 hex, and is now the only thing separating two layouts under one prefix
- `redis_key_namespace.go` — rejects two entities claiming one Redis prefix, in both validators; row keys and search documents share a keyspace and `ClearRedisCache` deletes by SCANning the prefix
- `unique_index_key.go` — `UniqueIndexKeySegment` (index name + its column list) and `UniqueIndexKeyHash` (sha256/64 of the looked-up values). Every read and every invalidation builds keys through these two, so the paths cannot drift. A cache hit is verified against the loaded row before it is returned — see `docs/entity-cache-keys.md` in droplet
- `alter.go` — `AlterSafety` (destructive is the zero value), `AlterKind`, `SplitAlters`, kind-ordered sort
- `save.go` — the write API: `ctx.Save` / `ctx.Delete` / `ctx.ForceDelete`, dirty-set preparation, post-commit work
- `db.go` — MySQL abstraction (`DB` interface, `DBTransaction`, metrics)
- `schema.go` — DDL diff and classification. One `Alter` per unit of work rather than one merged `ALTER TABLE` per table, so the forward-compatible half can be applied while the rest waits for a uniform fleet. The column diff is keyed on name, never position
- `nats.go` / `nats_schema.go` — NATS+JetStream pool, `NatsStreamBuilder`, `NatsConsumerBuilder`, `GetNatsAlters` reconciler
- `subject.go` — `Subject`, `ConsumerName`, `EntitySubject`, `ReplaySubject`, `EntityStreamName`. The subject is the identity of what happened; a consumer selects the subset it wants with a subject filter. That is the whole model, and it is why one write is one message however many consumers read the entity — fan-out happens at subscribe time, not by duplicating bytes at publish time.
- `consumer_def.go` — `ConsumerDef` (declaration), `EntityStreamOptions`, `resolveConsumers`. An entity tagged `orm:"cdc"` publishes on `fluxa.entity.<table>` and knows nothing about its readers; each consumer declares the entities it wants. `Validate()` rejects a duplicate name, an unregistered or untagged entity, a consumer declaring the same entity twice (JetStream rejects overlapping filters), and a `cdc`-tagged entity no consumer reads — every one of those is a mistake whose runtime symptom is silence.
- `consumer.go` — `StreamConsumer`, `ConsumerRef[B]`, `ConsumerBuilder`, `NewConsumer[B]`, `Dispatch`/`BatchDispatch`, `ConsumerPending`, `Enqueue` (replay), and the task retry ladder. `Consume` groups a fetched batch by subject, so `fluxa.entity.orders` and `fluxa.replay.<consumer>.orders` both reach the Orders handler and a replay drives the exact code path a real change drives. **One consumer, two kinds:** entity consumers filter `FLUXA_ENTITY`, task consumers filter `FLUXA_TASK`, and a `ConsumerDef` declaring both is rejected because a JetStream consumer belongs to one stream.
- `entity_event.go` — `DirtyEvent[T]`, `DirtyOp`, `RegisterEntityPublisher[E]`, `WatchFields`, `BuildDispatch`, `newEntityMessage`. The `Nats-Msg-Id` is scoped by subject and payload only; it used to carry the stream name, which is exactly what made the same change publish once per reader.
- `cdc_outbox.go` — transactional outbox. CDC publishes *after* commit, so a NATS outage or a process death between `COMMIT` and `PublishBatch` loses the event. Tagging an entity `orm:"outbox"` writes a `CDCOutboxEntity` row inside that entity's own write transaction, so "row committed" and "event will be delivered" become the same fact.
  - `cdc` + `outbox` → row written `pending`, published post-commit as usual, flipped to `dispatched` on success. `outbox` alone → row written terminal as `stored`, a change log with no subject.
  - The application registers `fluxaorm.CDCOutboxEntity{}` like any entity (it generates a provider and its table alter), then runs two crons over `RelayCDCOutbox` (republish rows left pending) and `PurgeCDCOutbox` (drop terminal rows past retention). `CDCOutboxBacklog` feeds the alert. Relay and purge live here rather than in the application because they must agree with the write side on statuses, columns and the deterministic `Nats-Msg-Id` that JetStream dedups on.
  - Invariants: the relay's grace period must stay well under the stream's `DuplicateWindow`, or a republish stops being deduped; the purge never touches `pending` at any age, because that row is an undelivered event; and the event is serialised **once** per write and shared between the stored row and the published message, because `buildEvent` stamps `TsMs` and a second serialisation would give the replay a different `Nats-Msg-Id`.
  - A row records `EntityName` and nothing about where the event was going: one entity is one subject, so the relay derives it, and the old `Streams`/`NatsPool` columns are gone. `Validate()` still rejects a tagged entity whose MySQL pool differs from the outbox table's, because that row would not be in the same transaction.
- `task.go` / `task_dispatch.go` / `job_run.go` — the task half of the same messaging concept: dispatch a task struct, consume it with a typed handler. Entity events carry *changes*; tasks carry work no row change implies ("send this email"), so the application needs no bespoke table plus poller per kind of async job.
  - The application declares a plain struct and registers it: `registry.RegisterTask(tasks.SendWelcomeEmail{}, fluxaorm.TaskOptions{})`. It must also register `fluxaorm.JobRunEntity{}`; `Validate()` rejects a task without it, because untracked work is exactly what the run table exists to prevent.
  - **A task's queue comes from the type, not from options.** `Queued` (`Queue() Queue`) is optional and probed on both the pointer and the value receiver, the same house pattern as `Indexes()`; absent means `DefaultQueue`. `Subjected` (`Subject() Subject`) is the full wire-name override, whose one real use is pinning the old subject across a Go rename — `Validate()` rejects one outside `fluxa.task.`, because the task stream would not capture it and the server would drop the message with no error at the publisher. `TaskOptions` therefore holds only `MaxAttempts`/`BaseBackoff`/`MaxBackoff`: deployment tuning, never identity.
  - **A queue is a routing label with exactly one consumer.** `ConsumerDef.Queues` declares which queues a consumer drains; two consumers on one queue is a `Validate()` error, because each would `Term()` the other's tasks as unknown. A queue with tasks and no consumer is also an error — that is the fall-through case, where a task with no `Queue()` lands on `default` and would otherwise queue forever in silence.
  - Codegen emits `gen.Consumer<Name>`, its builder with one `On<Task>` per task on its queues, and `gen.Dispatch<Task>`. This is the **only** generated file that imports application packages (it names the task struct), so a task package must never import the generated one — that closes a cycle. Package identifiers come from `reflect.Type.String()`, not the import path's last segment, which is wrong for any `/vN` path.
  - **The ack policy is the one place the two kinds diverge, and the divergence is deliberate.** An entity handler's error leaves the message unacked to be redelivered forever; a task's climbs a ladder — ack on success; unacked on `context.Canceled` (shutdown, so no attempt is burned); `Term()` immediately on `ErrPayloadUndecodable`, since those bytes will never improve; `NakWithDelay` with doubling backoff under the attempt cap; `Term()` at the cap. `Build()` likewise panics on a task with no handler, which is stricter than the entity side's skip-and-ack: a skipped projection refresh is recoverable by a replay, work that silently never runs is not.
  - `JobRunEntity` records every dispatch (`pending` → `running` → `succeeded`/`failed`/`deduplicated`). Unlike the outbox functions, `MarkJobRun*` error on a missing row rather than swallowing it — a lost outcome must surface — and `PurgeJobRuns` takes the status it may delete and accepts only a terminal one, so no caller can reach a `pending` row. `JobRunBacklog` is the stranded-row alarm, not a queue-depth gauge: `ConsumerPending` is depth, while a row `pending` past its task's whole backoff ladder means the message was never delivered.
  - `DispatchTask` refuses to publish inside a transaction (`ErrDispatchInTransaction`): the publish cannot roll back with it, so the task would run against data that never existed. It writes the run row *before* publishing so the row id can ride on the message, which makes `PublishWithAck` load-bearing — a publish the stream deduplicated away is closed out as `deduplicated` instead of being left `pending` forever for a message that will never arrive.
- `locker.go` — distributed locking via `bsm/redislock`
- `metrics.go` — Prometheus metrics for queries, cache hits/misses
- `where.go` — typed WHERE clause builder
- `test.go` — test utilities (`PrepareTables`, `PrepareTablesWithNats`, `PrepareTablesWithConsumers`, mock structures)

### Test Fixtures

`test_generate/` contains:
- `fixtures.go` — entity structs, `ConsumerDef`s and task fixtures used as generator input (`FixtureEntities`, `FixtureConsumers`, `FixtureTasks`, `FixtureTaskConsumers`, `FixtureRegistry`). They live outside `_test.go` so the generator can be re-run without compiling the test binary
- `models/` — JSON struct types referenced by fixture fields
- `genboot/main.go` — `go run ./test_generate/genboot` validates the fixtures against the local services, applies the alters and regenerates `test_generate/entities/`
- `entities/` — generated output, **git-ignored** (`.gitignore`); regenerate it with `genboot` before running the `test_generate` tests
- `generate_test.go` and the other `*_test.go` files — exercise the generated output
- `test_fixtures/jobtasks/` and `test_fixtures/media/jobtasks/` (repo root) — task structs used by the task/consumer fixtures

### Concurrency

Uses `puzpuzpuz/xsync/v2` concurrent maps for thread-safe metadata in Engine and EntitySchema. All tests run with `-race`.
