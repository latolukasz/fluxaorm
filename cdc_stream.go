package fluxaorm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DirtyOp identifies the kind of database mutation that produced a CDC event.
type DirtyOp uint8

const (
	DirtyInsert DirtyOp = 1
	DirtyUpdate DirtyOp = 2
	DirtyDelete DirtyOp = 3
)

// String returns the lowercase name (used in logs / wire header).
func (op DirtyOp) String() string {
	switch op {
	case DirtyInsert:
		return "insert"
	case DirtyUpdate:
		return "update"
	case DirtyDelete:
		return "delete"
	}
	return "unknown"
}

// dirtyOpLabel parses the Dirty-Op header value into a human-readable op name
// for metrics. Falls back to "unknown" for missing or unparseable headers so
// the metric label never collapses to an empty string.
func dirtyOpLabel(headerValue string) string {
	if headerValue == "" {
		return "unknown"
	}
	n, err := strconv.ParseUint(headerValue, 10, 8)
	if err != nil {
		return "unknown"
	}
	return DirtyOp(n).String()
}

// DirtyEvent is the typed change envelope published when an entity tagged
// `orm:"dirty=streamName"` is flushed. The generator emits per-entity aliases:
//
//	type OrdersDirtyEvent = fluxaorm.DirtyEvent[Orders]
type DirtyEvent[T any] struct {
	Op     DirtyOp `json:"op"`
	ID     uint64  `json:"id"`
	Before *T      `json:"before,omitempty"` // nil for Insert
	After  *T      `json:"after,omitempty"`  // nil for Delete
	TsMs   int64   `json:"ts_ms"`
}

const (
	// dirtyStreamPrefix is the prefix prepended to a CDC stream's logical name to
	// form the JetStream stream name (`FLUXA_DIRTY_<streamName>`).
	dirtyStreamPrefix = "FLUXA_DIRTY_"
	// dirtySubjectPrefix is prepended to the logical stream name to form the
	// single JetStream subject the stream listens on (`fluxa.dirty.<streamName>`).
	dirtySubjectPrefix = "fluxa.dirty."

	// HeaderDirtyEntity carries the entity Go type name (e.g. "Orders") so
	// consumers route a message to the right typed dispatch.
	HeaderDirtyEntity = "Dirty-Entity"
	// HeaderDirtyOp carries the numeric DirtyOp value as a decimal string.
	HeaderDirtyOp = "Dirty-Op"
	// HeaderDirtyStream carries the logical CDC stream name.
	HeaderDirtyStream = "Dirty-Stream"
)

var dirtyStreamNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func validDirtyStreamName(name string) bool {
	return dirtyStreamNameRegex.MatchString(name)
}

// CDCStream is the untyped interface satisfied by every CDCStreamRef[B].
// Used by registry methods that don't need to know the builder type.
type CDCStream interface {
	Name() NatsStreamName
	Subject() string
	JetStreamName() string
	Durable() string
}

// CDCStreamRef ties a CDC stream's name to its generated per-stream typed builder.
// `B` is the generated builder type; type inference at NewCDCConsumer's call
// site resolves the right typed return.
//
// Constructed by generated code via NewCDCStreamRef; user code references the
// generated value (e.g. `gen.StreamOrderIndexer`) and never instantiates this directly.
type CDCStreamRef[B any] struct {
	name       NatsStreamName
	newBuilder func(core *CDCBuilder) *B
}

// Name returns the logical CDC stream name.
func (r CDCStreamRef[B]) Name() NatsStreamName { return r.name }

// Subject returns the single JetStream subject this CDC stream listens on.
func (r CDCStreamRef[B]) Subject() string { return dirtySubjectPrefix + string(r.name) }

// JetStreamName returns the underlying JetStream stream name (with FLUXA_DIRTY_ prefix).
func (r CDCStreamRef[B]) JetStreamName() string { return dirtyStreamPrefix + string(r.name) }

// Durable returns the auto-derived JetStream durable consumer name.
func (r CDCStreamRef[B]) Durable() string { return DurableForStream(r.name) }

// Enqueue publishes a synthetic DirtyUpdate event for the given entity to this
// stream only, without any database mutation. Use it to trigger downstream
// consumers (indexers, listeners) to re-process an entity that hasn't otherwise
// been flushed.
//
// The published event mirrors the entity's currently-loaded state for both
// Before and After snapshots, so consumers using WatchFields correctly observe
// zero changes — only consumers without WatchFields (full reindexers etc.) act
// on it.
//
// All entities are published in one batch, so replaying a page of rows costs one
// round-trip rather than one per entity.
//
// Returns an error if any entity has no registered dirty publisher or isn't
// tagged for this CDC stream.
func (r CDCStreamRef[B]) Enqueue(orm Context, entities []Entity) error {
	return enqueueDirtyEvents(orm, entities, r.name)
}

// NewCDCStreamRef is the generator-facing factory. Called from generated
// `entities/dirty_streams.go` to bind a stream name to its builder constructor.
func NewCDCStreamRef[B any](name NatsStreamName, newBuilder func(core *CDCBuilder) *B) CDCStreamRef[B] {
	return CDCStreamRef[B]{name: name, newBuilder: newBuilder}
}

// untypedCDCStream is a minimal CDCStream implementation used by test helpers
// and `RegisterCDCStreamByName` — handy when no typed builder is needed (e.g.,
// `TestGenerate` registers the stream metadata without consuming from it).
type untypedCDCStream struct {
	name NatsStreamName
}

func (s *untypedCDCStream) Name() NatsStreamName { return s.name }
func (s *untypedCDCStream) Subject() string      { return dirtySubjectPrefix + string(s.name) }
func (s *untypedCDCStream) JetStreamName() string {
	return dirtyStreamPrefix + string(s.name)
}
func (s *untypedCDCStream) Durable() string { return DurableForStream(s.name) }

// NewCDCStreamByName returns a CDCStream value by name only, without binding a
// typed builder. Useful for test fixtures, schema-only operations, or
// administrative tools where the consumer side isn't being constructed.
func NewCDCStreamByName(name NatsStreamName) CDCStream {
	return &untypedCDCStream{name: name}
}

// CDCDispatch is the generic dispatch closure stored per entity in a CDCBuilder.
// Generated typed handlers wrap themselves into this type via BuildCDCDispatch[T].
type CDCDispatch func(ctx Context, msg *NatsMessage) error

// CDCBatchDispatch handles every message for one entity within a single fetched
// batch. Returning nil acks the whole group; returning err leaves the whole group
// unacked for redelivery, so batch handlers must be idempotent.
type CDCBatchDispatch func(ctx Context, msgs []*NatsMessage) error

// CDCBuilder is the shared state struct embedded by every generated per-stream
// builder. Exported so generated code in the entities package can call AddDispatch.
//
// Users never construct a CDCBuilder directly — `NewCDCConsumer` does it via the
// typed stream ref's factory closure.
type CDCBuilder struct {
	engine        Engine
	stream        CDCStream
	dispatch      map[string]CDCDispatch
	batchDispatch map[string]CDCBatchDispatch
}

// AddDispatch registers a typed dispatch closure for an entity type name.
// Called from generated OnX methods. Not for direct use.
func (b *CDCBuilder) AddDispatch(entityTypeName string, d CDCDispatch) {
	b.dispatch[entityTypeName] = d
}

// AddBatchDispatch registers a typed batch dispatch closure for an entity type
// name. Called from generated OnXBatch methods. Not for direct use.
func (b *CDCBuilder) AddBatchDispatch(entityTypeName string, d CDCBatchDispatch) {
	b.batchDispatch[entityTypeName] = d
}

// Stream returns the underlying CDCStream — used by Build() and tests.
func (b *CDCBuilder) Stream() CDCStream { return b.stream }

// Build wraps the accumulated dispatch table into a StreamConsumer ready to be
// driven by the user's lifecycle harness (cron.Job / fx / goroutine).
//
// Panics if an entity has both a per-message and a batch handler: that is a
// wiring mistake, and silently preferring one would hide a dropped handler.
func (b *CDCBuilder) Build() StreamConsumer {
	for entityName := range b.batchDispatch {
		if _, both := b.dispatch[entityName]; both {
			panic(fmt.Sprintf(
				"cdc stream '%s': entity '%s' has both a per-message and a batch handler registered; keep one",
				b.stream.Name(), entityName))
		}
	}
	return &cdcStreamConsumerImpl{
		base: &streamConsumerImpl{
			engine:      b.engine,
			streamName:  b.stream.Name(),
			durableName: b.stream.Durable(),
		},
		dispatch:      b.dispatch,
		batchDispatch: b.batchDispatch,
	}
}

// NewCDCConsumer is the single typed constructor for CDC consumers. Type
// parameter B is inferred from the stream ref; the returned builder exposes
// `OnX` methods only for entities tagged into that stream.
//
//	consumer := fluxaorm.NewCDCConsumer(orm, gen.StreamOrderIndexer).
//	    OnOrders(handleOrder).
//	    Build()
func NewCDCConsumer[B any](engine Engine, stream CDCStreamRef[B]) *B {
	core := &CDCBuilder{
		engine:        engine,
		stream:        stream,
		dispatch:      make(map[string]CDCDispatch),
		batchDispatch: make(map[string]CDCBatchDispatch),
	}
	return stream.newBuilder(core)
}

// cdcStreamConsumerImpl wraps a base streamConsumerImpl with CDC-aware per-message
// dispatch by `Dirty-Entity` header. Unknown entity names (e.g. a new entity added
// after this consumer was deployed) are skipped with a debug log + ack, never panic.
type cdcStreamConsumerImpl struct {
	base          *streamConsumerImpl
	dispatch      map[string]CDCDispatch
	batchDispatch map[string]CDCBatchDispatch
}

func (c *cdcStreamConsumerImpl) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if batch <= 0 {
		batch = 1
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ormCtx := c.base.engine.NewContext(ctx)
	cons, err := c.base.resolveStreamConsumer(ormCtx)
	if err != nil {
		return err
	}
	natsBatch := cons.Fetch(ormCtx, batch, timeout)
	if fetchErr := natsBatch.Error(); fetchErr != nil {
		return fetchErr
	}
	metrics, hasMetrics := c.base.engine.Registry().getMetricsRegistry()
	streamName := string(c.base.streamName)

	// Group by entity so entities with a batch handler get one call for the whole
	// fetch. Order of first appearance is kept so dispatch stays deterministic;
	// grouping only reorders across entities, never within one.
	order := make([]string, 0, len(c.batchDispatch)+1)
	grouped := make(map[string][]*NatsMessage)
	for _, msg := range natsBatch.Records() {
		entityName := msg.Headers.Get(HeaderDirtyEntity)
		if hasMetrics {
			metrics.cdcMessages.WithLabelValues(streamName, entityName, dirtyOpLabel(msg.Headers.Get(HeaderDirtyOp))).Inc()
			if !msg.Timestamp.IsZero() {
				metrics.streamLag.WithLabelValues(streamName).Observe(time.Since(msg.Timestamp).Seconds())
			}
		}
		if _, seen := grouped[entityName]; !seen {
			order = append(order, entityName)
		}
		grouped[entityName] = append(grouped[entityName], msg)
	}

	for _, entityName := range order {
		msgs := grouped[entityName]
		if batchDispatch, ok := c.batchDispatch[entityName]; ok {
			c.dispatchBatch(ormCtx, batchDispatch, msgs)
			continue
		}
		dispatch, ok := c.dispatch[entityName]
		if !ok {
			// No handler registered for this entity — skip + ack (forward-compat).
			for _, msg := range msgs {
				_ = msg.Ack()
			}
			continue
		}
		for _, msg := range msgs {
			if dispatchErr := dispatch(ormCtx, msg); dispatchErr != nil {
				c.base.logHandlerError(ormCtx, msg, dispatchErr)
				continue
			}
			if ackErr := msg.Ack(); ackErr != nil {
				c.base.logHandlerError(ormCtx, msg, fmt.Errorf("ack: %w", ackErr))
			}
		}
	}
	return nil
}

// dispatchBatch runs one batch handler and acks the whole group on success. On
// error nothing is acked, so JetStream redelivers the entire group.
func (c *cdcStreamConsumerImpl) dispatchBatch(ormCtx Context, dispatch CDCBatchDispatch, msgs []*NatsMessage) {
	if dispatchErr := dispatch(ormCtx, msgs); dispatchErr != nil {
		c.base.logHandlerError(ormCtx, msgs[0], fmt.Errorf("batch of %d: %w", len(msgs), dispatchErr))
		return
	}
	for _, msg := range msgs {
		if ackErr := msg.Ack(); ackErr != nil {
			c.base.logHandlerError(ormCtx, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}
}

// CDCHandlerOption configures a single typed CDC handler (filtering, etc).
type CDCHandlerOption func(*cdcHandlerConfig)

type cdcHandlerConfig struct {
	watchFields []string
}

// WatchFields skips Update events where none of the listed entity fields differ
// between Before and After. Insert and Delete events always fire. The Field type
// is the same one providers expose (e.g. gen.ShowsProvider.Fields.Status), so
// callers don't need column-name strings.
func WatchFields(fields ...Field) CDCHandlerOption {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.ColumnName())
	}
	return func(c *cdcHandlerConfig) { c.watchFields = append(c.watchFields, names...) }
}

// CDCEntityName returns the canonical wire/dispatch key for an entity type.
// Equivalent to `reflect.TypeOf((*T)(nil)).Elem().Name()` but typed-generic.
func CDCEntityName[T any]() string {
	var zero T
	return reflect.TypeOf(zero).Name()
}

// BuildCDCDispatch wraps a typed user handler into the generic CDCDispatch closure.
// Generated OnX methods call this:
//
//	b.AddDispatch(fluxaorm.CDCEntityName[Orders](),
//	    fluxaorm.BuildCDCDispatch[Orders](handler, opts...))
//
// BuildCDCBatchDispatch wraps a typed user batch handler into the generic
// CDCBatchDispatch closure. Generated OnXBatch methods call this.
//
// Every message in the group is decoded up front and filtered through
// WatchFields; if nothing survives the filter the handler is skipped and the
// group acks. A decode failure fails the whole group rather than silently
// dropping a message.
func BuildCDCBatchDispatch[T any](
	handler func(ctx Context, evs []*DirtyEvent[T]) error, opts ...CDCHandlerOption,
) CDCBatchDispatch {
	cfg := &cdcHandlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return func(ctx Context, msgs []*NatsMessage) error {
		evs := make([]*DirtyEvent[T], 0, len(msgs))
		for _, msg := range msgs {
			ev, err := decodeDirtyEvent[T](msg)
			if err != nil {
				return err
			}
			if len(cfg.watchFields) > 0 && ev.Op == DirtyUpdate && !dirtyEventHasFieldChanges(ev, cfg.watchFields) {
				continue
			}
			evs = append(evs, ev)
		}
		if len(evs) == 0 {
			return nil
		}
		return handler(ctx, evs)
	}
}

// decodeDirtyEvent unmarshals one CDC message into its typed envelope.
//
// UseNumber keeps numeric Before/After snapshot values (T == map[string]any) as
// json.Number rather than float64. Snapshots carry uint64 entity IDs and FK
// references that exceed float64's 2^53 exact range, which a float decode would
// silently round.
func decodeDirtyEvent[T any](msg *NatsMessage) (*DirtyEvent[T], error) {
	ev := &DirtyEvent[T]{}
	dec := json.NewDecoder(bytes.NewReader(msg.Data))
	dec.UseNumber()
	if err := dec.Decode(ev); err != nil {
		return nil, fmt.Errorf("dirty event unmarshal: %w", err)
	}
	return ev, nil
}

func BuildCDCDispatch[T any](handler func(ctx Context, ev *DirtyEvent[T]) error, opts ...CDCHandlerOption) CDCDispatch {
	cfg := &cdcHandlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return func(ctx Context, msg *NatsMessage) error {
		ev, err := decodeDirtyEvent[T](msg)
		if err != nil {
			return err
		}
		if len(cfg.watchFields) > 0 && ev.Op == DirtyUpdate {
			if !dirtyEventHasFieldChanges(ev, cfg.watchFields) {
				return nil
			}
		}
		return handler(ctx, ev)
	}
}

// dirtyEventHasFieldChanges returns true iff at least one of `fields` differs
// between Before and After. For non-Update events it returns true unconditionally
// (Insert/Delete always pass field filtering).
func dirtyEventHasFieldChanges[T any](ev *DirtyEvent[T], fields []string) bool {
	if ev.Op != DirtyUpdate {
		return true
	}
	if ev.Before == nil || ev.After == nil {
		return true
	}
	beforeVal := reflect.ValueOf(*ev.Before)
	afterVal := reflect.ValueOf(*ev.After)
	for _, f := range fields {
		b, a := fieldValue(beforeVal, f), fieldValue(afterVal, f)
		if b.IsValid() != a.IsValid() {
			return true
		}
		if !b.IsValid() {
			continue
		}
		if !reflect.DeepEqual(b.Interface(), a.Interface()) {
			return true
		}
	}
	return false
}

// fieldValue extracts a snapshot field for the WatchFields comparison.
// Handles both the MVP map[string]any snapshot shape and the eventual typed
// struct case, returning an invalid Value when the field is absent so the
// caller can distinguish "missing" from "zero value".
func fieldValue(v reflect.Value, name string) reflect.Value {
	switch v.Kind() {
	case reflect.Map:
		return v.MapIndex(reflect.ValueOf(name))
	case reflect.Struct:
		return v.FieldByName(name)
	default:
		return reflect.Value{}
	}
}

// ----- Publisher registration -----

// dirtyPublisherEntry holds the per-entity publisher closure plus a cached
// wire/dispatch key. Stored in package-level registry; copied into the engine
// at Validate() time keyed by reflect.Type.
type dirtyPublisherEntry struct {
	streams      []NatsStreamName
	outbox       bool
	entityName   string
	buildEvent   func(entity Entity, op DirtyOp, beforeOrigin map[string]any) ([]byte, error)
	reflectType  reflect.Type
	registeredAt time.Time
}

var dirtyPublishersMu sync.RWMutex
var dirtyPublishers = make(map[reflect.Type]*dirtyPublisherEntry)

// RegisterDirtyPublisher records a per-entity publisher closure under the entity's
// reflect.Type. Called from generated `init()` blocks. The closure receives the
// entity + op + before-state and returns the marshaled wire payload — the generator
// emits the typed snapshot/event construction inline so the entity type and the
// JSON payload type stay independent.
//
// Duplicate registrations for the same E silently replace (so re-running tests
// with hot-reload doesn't error).
func RegisterDirtyPublisher[E any](
	streams []NatsStreamName,
	buildEvent func(entity *E, op DirtyOp, beforeOrigin map[string]any) ([]byte, error),
) {
	var zero E
	t := reflect.TypeOf(zero)
	if t == nil {
		panic("RegisterDirtyPublisher: E must be a concrete struct type")
	}
	wrapped := func(e Entity, op DirtyOp, beforeOrigin map[string]any) ([]byte, error) {
		typed, ok := any(e).(*E)
		if !ok {
			return nil, fmt.Errorf("RegisterDirtyPublisher[%s]: entity is %T", t.Name(), e)
		}
		return buildEvent(typed, op, beforeOrigin)
	}
	dirtyPublishersMu.Lock()
	dirtyPublishers[t] = &dirtyPublisherEntry{
		streams:      streams,
		entityName:   t.Name(),
		buildEvent:   wrapped,
		reflectType:  t,
		registeredAt: time.Now(),
	}
	dirtyPublishersMu.Unlock()
}

// getDirtyPublisher returns the registered publisher for the given reflect.Type, if any.
// Engine.Validate copies entries into engineImplementation; this lookup is used during
// generation/test bootstrap as well.
func getDirtyPublisher(t reflect.Type) (*dirtyPublisherEntry, bool) {
	dirtyPublishersMu.RLock()
	defer dirtyPublishersMu.RUnlock()
	entry, ok := dirtyPublishers[t]
	return entry, ok
}

// getDirtyPublisherByEntityName returns the publisher whose generated entity
// struct has the given Go name. This is the lookup `resolveDirtyStreams` uses
// because the schema's reflect.Type points at the user-supplied source struct
// (e.g. ProductEntity) while the publisher init() registers under the generated
// entity struct (e.g. Products) — they're distinct types in distinct packages,
// so a reflect.Type lookup would always miss. The generated name follows the
// `capitalizeFirst(tableName)` convention used by the code generator.
func getDirtyPublisherByEntityName(name string) (*dirtyPublisherEntry, bool) {
	dirtyPublishersMu.RLock()
	defer dirtyPublishersMu.RUnlock()
	for _, entry := range dirtyPublishers {
		if entry.entityName == name {
			return entry, true
		}
	}
	return nil, false
}

// withSchemaConfig copies a publisher and folds in the schema-derived settings.
// Only outbox routing today.
// The copy matters: getDirtyPublisher returns a pointer into the package-global
// registry that every engine in the process shares, so writing to it would both
// leak one engine's configuration into another and race two concurrent
// Validate() calls.
func withSchemaConfig(entry *dirtyPublisherEntry, schema *entitySchema) *dirtyPublisherEntry {
	scoped := *entry
	scoped.outbox = schema.outbox

	return &scoped
}

// generatedEntityName produces the Go identifier for the entity struct that
// fluxaorm.Generate emits from a schema's tableName. Mirrors codeGenerator's
// capitalizeFirst (split on `_`, capitalize each part's first letter) so the
// publisher-name lookup at Validate() time matches the name the generator used.
func generatedEntityName(tableName string) string {
	parts := strings.Split(tableName, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		b := []byte(part)
		if b[0] >= 'a' && b[0] <= 'z' {
			b[0] = b[0] - ('a' - 'A')
		}
		parts[i] = string(b)
	}
	return strings.Join(parts, "")
}

// ----- Registration helpers used by the registry -----

// CDCStreamOptions tunes a CDC stream's JetStream config. Subjects, retention and
// dedup-window-presence are fluxaorm-controlled (CDC always uses LimitsPolicy, a
// single subject `fluxa.dirty.<name>`, and dedup keyed on Nats-Msg-Id).
type CDCStreamOptions struct {
	Storage         jetstream.StorageType
	Replicas        int
	MaxAge          time.Duration
	DuplicateWindow time.Duration
	MaxAckPending   int
	AckWait         time.Duration
	MaxDeliver      int
	NatsPool        string
}

// resolvedDirtyStream is the canonical registry record for a CDC stream after
// applying defaults and option overrides.
type resolvedDirtyStream struct {
	stream  CDCStream
	options CDCStreamOptions
}

// streamRegistryEntry tracks where a logical stream lives (which NATS pool) so
// StreamConsumer can resolve its durable at runtime without re-walking the
// registry's slice form.
type streamRegistryEntry struct {
	poolCode  string
	isCDC     bool
	cdcStream CDCStream // non-nil iff isCDC
}

// applyCDCStreamDefaults fills zero-valued options with documented defaults.
func applyCDCStreamDefaults(opts CDCStreamOptions) CDCStreamOptions {
	if opts.NatsPool == "" {
		opts.NatsPool = DefaultPoolCode
	}
	if opts.Replicas <= 0 {
		opts.Replicas = 1
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 7 * 24 * time.Hour
	}
	if opts.DuplicateWindow <= 0 {
		opts.DuplicateWindow = 10 * time.Minute
	}
	if opts.MaxAckPending == 0 {
		opts.MaxAckPending = 256
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 30 * time.Second
	}
	if opts.MaxDeliver == 0 {
		opts.MaxDeliver = -1
	}
	return opts
}

// newDirtyMessage builds the message both publish paths use - the post-commit
// flush and the outbox relay - including the deterministic Nats-Msg-Id that
// JetStream dedups on. One builder, so a replay can never drift from the
// original and lose its dedup.
func newDirtyMessage(stream NatsStreamName, entityName string, op DirtyOp, payload []byte) *NatsMessage {
	msg := NewNatsMessage(dirtySubjectPrefix + string(stream))
	msg.Data = payload
	msg.Headers = nats.Header{}
	msg.Headers.Set(HeaderDirtyEntity, entityName)
	msg.Headers.Set(HeaderDirtyOp, fmt.Sprintf("%d", op))
	msg.Headers.Set(HeaderDirtyStream, string(stream))
	msg.Headers.Set("Nats-Msg-Id", fmt.Sprintf("%s:%s:%d", stream, entityName, msgIDHash(payload)))
	return msg
}

// dirtyOpFromFlushType maps the entity's internal flush-type code (1/2/3) to DirtyOp.
func dirtyOpFromFlushType(t uint8) DirtyOp {
	switch t {
	case 1:
		return DirtyInsert
	case 2:
		return DirtyUpdate
	case 3:
		return DirtyDelete
	}
	return 0
}

// pendingDirtyMessages accumulates CDC messages across a whole Flush, keyed by
// NATS pool, so each pool is published in one batch instead of one round-trip
// per entity per stream.
type pendingDirtyMessages map[string][]*NatsMessage

// buildDirtyEvent serialises the event once and appends one message per configured
// CDC stream to `pending`, grouped by the stream's NATS pool.
func buildDirtyEvent(
	orm Context, pending pendingDirtyMessages, publisher *dirtyPublisherEntry,
	w *pendingWrite, op DirtyOp, beforeOrigin map[string]any,
) error {
	// Outbox staging already serialised this event. Reuse those exact bytes:
	// buildEvent stamps TsMs, so a second call would publish a payload the
	// stored row does not match, and the relay could no longer replay it under
	// the same dedup id.
	payload := w.dirtyPayload
	if payload == nil {
		var err error
		payload, err = publisher.buildEvent(w.entity, op, beforeOrigin)
		if err != nil {
			return fmt.Errorf("build dirty event for %s: %w", publisher.entityName, err)
		}
	}
	if payload == nil {
		return nil
	}
	reg := orm.Engine().Registry().(*engineRegistryImplementation)
	for _, streamName := range publisher.streams {
		ds, ok := reg.dirtyStreams[streamName]
		if !ok {
			return fmt.Errorf("entity '%s' references unregistered CDC stream '%s'", publisher.entityName, streamName)
		}
		if pool := orm.Engine().Nats(ds.options.NatsPool); pool == nil {
			return fmt.Errorf("nats pool '%s' for CDC stream '%s' not configured", ds.options.NatsPool, streamName)
		}
		msg := newDirtyMessage(streamName, publisher.entityName, op, payload)
		pending[ds.options.NatsPool] = append(pending[ds.options.NatsPool], msg)
	}
	return nil
}

// flushDirtyMessages publishes each pool's accumulated messages in one batch.
// Called at the end of the Flush entity walk; Flush still waits for the acks, so
// "flush succeeded ⇒ event durably published" continues to hold.
func flushDirtyMessages(orm Context, pending pendingDirtyMessages) error {
	for poolCode, msgs := range pending {
		pool := orm.Engine().Nats(poolCode)
		if pool == nil {
			return fmt.Errorf("nats pool '%s' for CDC publish not configured", poolCode)
		}
		if err := pool.PublishBatch(orm, msgs); err != nil {
			return fmt.Errorf("publish %d dirty events to pool %s: %w", len(msgs), poolCode, err)
		}
	}
	return nil
}

// enqueueDirtyEvents publishes synthetic DirtyUpdate events for `entities` to a
// single named stream, in one batch. Unlike the Flush publisher it does not fan
// out to all of an entity's tagged streams and is not driven by a Flush — callers
// invoke it explicitly via CDCStreamRef.Enqueue to request a re-process.
//
// Validates that every entity has a registered publisher and that `streamName`
// is one of its tagged streams. Each message gets a unique Nats-Msg-Id so dedup
// never collapses repeated enqueues.
func enqueueDirtyEvents(orm Context, entities []Entity, streamName NatsStreamName) error {
	if len(entities) == 0 {
		return nil
	}
	reg := orm.Engine().Registry().(*engineRegistryImplementation)
	ds, ok := reg.dirtyStreams[streamName]
	if !ok {
		return fmt.Errorf("CDC stream '%s' is not registered", streamName)
	}
	pool := orm.Engine().Nats(ds.options.NatsPool)
	if pool == nil {
		return fmt.Errorf("nats pool '%s' for CDC stream '%s' not configured", ds.options.NatsPool, streamName)
	}

	msgs := make([]*NatsMessage, 0, len(entities))
	for _, entity := range entities {
		if entity == nil {
			return fmt.Errorf("enqueue: entity is nil")
		}
		t := reflect.TypeOf(entity)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		publisher, hasPub := reg.dirtyPublishers[t]
		if !hasPub {
			return fmt.Errorf("no dirty publisher registered for entity %s", t.Name())
		}
		tagged := false
		for _, s := range publisher.streams {
			if s == streamName {
				tagged = true
				break
			}
		}
		if !tagged {
			return fmt.Errorf("entity %s is not tagged for CDC stream %s", publisher.entityName, streamName)
		}
		payload, err := publisher.buildEvent(entity, DirtyUpdate, nil)
		if err != nil {
			return fmt.Errorf("build dirty event for %s: %w", publisher.entityName, err)
		}
		if payload == nil {
			continue
		}
		msg := newDirtyMessage(streamName, publisher.entityName, DirtyUpdate, payload)
		msg.Headers.Set("Nats-Msg-Id", fmt.Sprintf("%s:%s:enqueue:%d:%d", streamName, publisher.entityName, entity.GetID(), time.Now().UnixNano()))
		msgs = append(msgs, msg)
	}

	if err := pool.PublishBatch(orm, msgs); err != nil {
		return fmt.Errorf("enqueue %d events to stream %s: %w", len(msgs), streamName, err)
	}
	return nil
}

// msgIDHash hashes the payload bytes into a uint64 for use in Nats-Msg-Id.
// Combined with stream + entity name it scopes the dedup window correctly:
// two retries of the same event share an ID, but unrelated events don't collide.
func msgIDHash(b []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// resolveDirtyStreams runs at registry.Validate() time to:
//
//  1. Verify every entity tagged `dirty=Y` references a registered CDC stream Y.
//  2. Verify every registered CDC stream has at least one referencing entity.
//  3. Verify every dirty-tagged entity has a RegisterDirtyPublisher[T] entry.
//  4. Populate engineRegistryImplementation.dirtyStreams (resolved configs),
//     streamRegistry (stream-name → pool lookup for runtime consumer resolution),
//     and dirtyPublishers (reflect.Type → publisher entry for fast flush-path lookup).
func resolveDirtyStreams(r *registry, e *engineImplementation) error {
	reg := e.registry
	reg.dirtyStreams = make(map[NatsStreamName]*resolvedDirtyStream)
	reg.streamRegistry = make(map[NatsStreamName]*streamRegistryEntry)
	reg.dirtyPublishers = make(map[reflect.Type]*dirtyPublisherEntry)

	// Copy registered CDC streams.
	for name, ds := range r.dirtyStreams {
		reg.dirtyStreams[name] = ds
		reg.streamRegistry[name] = &streamRegistryEntry{
			poolCode:  ds.options.NatsPool,
			isCDC:     true,
			cdcStream: ds.stream,
		}
		// Auto-register the durable consumer settings on the pool so runtime
		// `pool.Consumer("<stream>-workers")` lookups succeed without requiring
		// a separate RegisterNatsConsumer call. The JetStream consumer itself
		// is reconciled by GetNatsAlters.
		if pool, ok := r.natsPools[ds.options.NatsPool]; ok {
			durable := DurableForStream(name)
			if _, has := pool.consumers[durable]; !has {
				pool.consumers[durable] = &NatsConsumerSettings{
					Name:           durable,
					FilterSubjects: []string{dirtySubjectPrefix + string(name)},
					AckWait:        ds.options.AckWait,
					MaxAckPending:  ds.options.MaxAckPending,
					MaxDeliver:     ds.options.MaxDeliver,
				}
			}
		}
	}

	// Track which streams are referenced by an entity (for orphan detection).
	referenced := make(map[NatsStreamName]bool)
	for _, schema := range reg.entitySchemas {
		// An `outbox`-only entity has no streams but still needs its publisher
		// indexed: the outbox row carries the same snapshot payload.
		if len(schema.dirtyStreams) == 0 && !schema.outbox {
			continue
		}
		// (1) Every named stream must be a registered CDC stream.
		for _, name := range schema.dirtyStreams {
			if _, ok := reg.dirtyStreams[name]; !ok {
				return fmt.Errorf("entity '%s' tagged `dirty=%s` but stream '%s' is not a registered CDC stream; call registry.RegisterCDCStream(<typed ref for %s>, ...)", schema.t.Name(), name, name, name)
			}
			referenced[name] = true
		}
		// (3) If a publisher entry already exists (init() block ran), index it
		// by schema index for O(1) flush-path lookup. Match by generated entity
		// name (capitalizeFirst of tableName) because the publisher init() runs
		// in the generated package keyed by the generated entity's reflect.Type
		// — distinct from the source struct the user passed to RegisterEntity.
		// Fall back to reflect.Type lookup for callers that register a publisher
		// for the same struct they passed to RegisterEntity. Missing publishers
		// are tolerated here so TestGenerate can run Validate before the freshly
		// generated init() blocks are compiled in.
		// Match publisher by generated entity name (capitalizeFirst of tableName)
		// because publishers register from the generated package using the generated
		// struct's reflect.Type, while schema.t is the source struct the user passed
		// to RegisterEntity (often a different type, e.g. ProductEntity vs Products).
		// Fall back to reflect.Type for callers that register a publisher for the
		// same struct they passed to RegisterEntity. Missing publishers are tolerated
		// here so TestGenerate can run Validate before the freshly generated init()
		// blocks compile in.
		if entry, ok := getDirtyPublisherByEntityName(generatedEntityName(schema.tableName)); ok {
			reg.dirtyPublishers[entry.reflectType] = withSchemaConfig(entry, schema)
		} else if entry, ok := getDirtyPublisher(schema.t); ok {
			reg.dirtyPublishers[entry.reflectType] = withSchemaConfig(entry, schema)
		}
	}

	// (2) Every registered CDC stream must be referenced by at least one entity.
	for name := range reg.dirtyStreams {
		if !referenced[name] {
			return fmt.Errorf("CDC stream '%s' is registered but no entity is tagged `dirty=%s`", name, name)
		}
	}
	return nil
}
