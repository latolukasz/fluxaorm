package fluxaorm

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
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

// CDCBuilder is the shared state struct embedded by every generated per-stream
// builder. Exported so generated code in the entities package can call AddDispatch.
//
// Users never construct a CDCBuilder directly — `NewCDCConsumer` does it via the
// typed stream ref's factory closure.
type CDCBuilder struct {
	engine   Engine
	stream   CDCStream
	dispatch map[string]CDCDispatch
}

// AddDispatch registers a typed dispatch closure for an entity type name.
// Called from generated OnX methods. Not for direct use.
func (b *CDCBuilder) AddDispatch(entityTypeName string, d CDCDispatch) {
	b.dispatch[entityTypeName] = d
}

// Stream returns the underlying CDCStream — used by Build() and tests.
func (b *CDCBuilder) Stream() CDCStream { return b.stream }

// Build wraps the accumulated dispatch table into a StreamConsumer ready to be
// driven by the user's lifecycle harness (cron.Job / fx / goroutine).
func (b *CDCBuilder) Build() StreamConsumer {
	return &cdcStreamConsumerImpl{
		base: &streamConsumerImpl{
			engine:      b.engine,
			streamName:  b.stream.Name(),
			durableName: b.stream.Durable(),
		},
		dispatch: b.dispatch,
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
		engine:   engine,
		stream:   stream,
		dispatch: make(map[string]CDCDispatch),
	}
	return stream.newBuilder(core)
}

// cdcStreamConsumerImpl wraps a base streamConsumerImpl with CDC-aware per-message
// dispatch by `Dirty-Entity` header. Unknown entity names (e.g. a new entity added
// after this consumer was deployed) are skipped with a debug log + ack, never panic.
type cdcStreamConsumerImpl struct {
	base     *streamConsumerImpl
	dispatch map[string]CDCDispatch
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
	for _, msg := range natsBatch.Records() {
		entityName := msg.Headers.Get(HeaderDirtyEntity)
		dispatch, ok := c.dispatch[entityName]
		if !ok {
			// No handler registered for this entity — skip + ack (forward-compat).
			_ = msg.Ack()
			continue
		}
		if dispatchErr := dispatch(ormCtx, msg); dispatchErr != nil {
			c.base.logHandlerError(ormCtx, msg, dispatchErr)
			continue
		}
		if ackErr := msg.Ack(); ackErr != nil {
			c.base.logHandlerError(ormCtx, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}
	return nil
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
func BuildCDCDispatch[T any](handler func(ctx Context, ev *DirtyEvent[T]) error, opts ...CDCHandlerOption) CDCDispatch {
	cfg := &cdcHandlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return func(ctx Context, msg *NatsMessage) error {
		ev := &DirtyEvent[T]{}
		if err := json.Unmarshal(msg.Data, ev); err != nil {
			return fmt.Errorf("dirty event unmarshal: %w", err)
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

// newDirtyMessage allocates a fresh NatsMessage carrying the JSON payload and
// the Dirty-* headers, ready to be Publish'd.
func newDirtyMessage(stream NatsStreamName, entityName string, op DirtyOp, payload []byte) *NatsMessage {
	msg := NewNatsMessage(dirtySubjectPrefix + string(stream))
	msg.Data = payload
	msg.Headers = nats.Header{}
	msg.Headers.Set(HeaderDirtyEntity, entityName)
	msg.Headers.Set(HeaderDirtyOp, fmt.Sprintf("%d", op))
	msg.Headers.Set(HeaderDirtyStream, string(stream))
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

// publishDirtyEvent serialises the event once and fans it out to all of the
// entity's configured CDC streams. Returns the first publish error.
func publishDirtyEvent(orm Context, publisher *dirtyPublisherEntry, e Entity, op DirtyOp, beforeOrigin map[string]any) error {
	payload, err := publisher.buildEvent(e, op, beforeOrigin)
	if err != nil {
		return fmt.Errorf("build dirty event for %s: %w", publisher.entityName, err)
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
		pool := orm.Engine().Nats(ds.options.NatsPool)
		if pool == nil {
			return fmt.Errorf("nats pool '%s' for CDC stream '%s' not configured", ds.options.NatsPool, streamName)
		}
		msg := newDirtyMessage(streamName, publisher.entityName, op, payload)
		msg.Headers.Set("Nats-Msg-Id", fmt.Sprintf("%s:%s:%d", streamName, publisher.entityName, msgIDHash(payload)))
		if err := pool.Publish(orm, msg); err != nil {
			return fmt.Errorf("publish %s to stream %s: %w", publisher.entityName, streamName, err)
		}
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
		if len(schema.dirtyStreams) == 0 {
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
			reg.dirtyPublishers[entry.reflectType] = entry
		} else if entry, ok := getDirtyPublisher(schema.t); ok {
			reg.dirtyPublishers[entry.reflectType] = entry
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
