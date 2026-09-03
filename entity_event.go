package fluxaorm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
)

// DirtyOp identifies the kind of database mutation that produced an event.
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
// `orm:"cdc"` is flushed. The generator emits per-entity aliases:
//
//	type OrdersDirtyEvent = fluxaorm.DirtyEvent[Orders]
type DirtyEvent[T any] struct {
	Op     DirtyOp `json:"op"`
	ID     uint64  `json:"id"`
	Before *T      `json:"before,omitempty"` // nil for Insert
	After  *T      `json:"after,omitempty"`  // nil for Delete
	TsMs   int64   `json:"ts_ms"`
}

// HeaderDirtyOp carries the numeric DirtyOp value as a decimal string. It is
// the only header on an entity message: the entity's identity is the subject,
// so duplicating it in a header would just be a second thing to keep in sync.
const HeaderDirtyOp = "Dirty-Op"

// HandlerOption configures a single typed handler (field filtering, etc).
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	watchFields []string
}

// WatchFields skips Update events where none of the listed entity fields differ
// between Before and After. Insert and Delete events always fire. The Field type
// is the same one providers expose (e.g. gen.ShowsProvider.Fields.Status), so
// callers don't need column-name strings.
//
// A replayed event carries identical Before and After, so a handler using
// WatchFields deliberately ignores replays - only full reindexers, which do not
// filter, act on them.
func WatchFields(fields ...Field) HandlerOption {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.ColumnName())
	}

	return func(c *handlerConfig) { c.watchFields = append(c.watchFields, names...) }
}

// BuildDispatch wraps a typed user handler into the generic Dispatch closure.
// Generated OnX methods call this.
func BuildDispatch[T any](handler func(ctx Context, ev *DirtyEvent[T]) error, opts ...HandlerOption) Dispatch {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx Context, msg *NatsMessage) error {
		ev, err := decodeDirtyEvent[T](msg)
		if err != nil {
			return err
		}
		if len(cfg.watchFields) > 0 && ev.Op == DirtyUpdate && !dirtyEventHasFieldChanges(ev, cfg.watchFields) {
			return nil
		}

		return handler(ctx, ev)
	}
}

// BuildBatchDispatch wraps a typed user batch handler into the generic
// BatchDispatch closure. Generated OnXBatch methods call this.
//
// Every message in the group is decoded up front and filtered through
// WatchFields; if nothing survives the filter the handler is skipped and the
// group acks. A decode failure fails the whole group rather than silently
// dropping a message.
func BuildBatchDispatch[T any](
	handler func(ctx Context, evs []*DirtyEvent[T]) error, opts ...HandlerOption,
) BatchDispatch {
	cfg := &handlerConfig{}
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

// decodeDirtyEvent unmarshals one message into its typed envelope.
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
// Handles both the map[string]any snapshot shape and the eventual typed
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

// dirtyPublisherEntry holds the per-entity publisher closure plus the identity
// its messages carry. Stored in a package-level registry; copied into the engine
// at Validate() time keyed by reflect.Type.
type dirtyPublisherEntry struct {
	subject     Subject
	tableName   string
	entityName  string
	cdc         bool
	outbox      bool
	buildEvent  func(entity Entity, op DirtyOp, beforeOrigin map[string]any) ([]byte, error)
	reflectType reflect.Type
}

var dirtyPublishersMu sync.RWMutex
var dirtyPublishers = make(map[reflect.Type]*dirtyPublisherEntry)

// RegisterEntityPublisher records a per-entity publisher closure under the
// entity's reflect.Type. Called from generated `init()` blocks. The closure
// receives the entity + op + before-state and returns the marshaled wire
// payload - the generator emits the typed snapshot construction inline so the
// entity type and the JSON payload type stay independent.
//
// The table name is the argument rather than the subject because the subject
// format is fluxaorm's to own; generated code names the table it already knows.
//
// Duplicate registrations for the same E silently replace, so re-running tests
// with hot-reload doesn't error.
func RegisterEntityPublisher[E any](
	table string,
	buildEvent func(entity *E, op DirtyOp, beforeOrigin map[string]any) ([]byte, error),
) {
	var zero E
	t := reflect.TypeOf(zero)
	if t == nil {
		panic("RegisterEntityPublisher: E must be a concrete struct type")
	}
	wrapped := func(e Entity, op DirtyOp, beforeOrigin map[string]any) ([]byte, error) {
		typed, ok := any(e).(*E)
		if !ok {
			return nil, fmt.Errorf("RegisterEntityPublisher[%s]: entity is %T", t.Name(), e)
		}

		return buildEvent(typed, op, beforeOrigin)
	}
	dirtyPublishersMu.Lock()
	dirtyPublishers[t] = &dirtyPublisherEntry{
		subject:     EntitySubject(table),
		tableName:   table,
		entityName:  t.Name(),
		buildEvent:  wrapped,
		reflectType: t,
	}
	dirtyPublishersMu.Unlock()
}

// getDirtyPublisher returns the registered publisher for the given reflect.Type, if any.
func getDirtyPublisher(t reflect.Type) (*dirtyPublisherEntry, bool) {
	dirtyPublishersMu.RLock()
	defer dirtyPublishersMu.RUnlock()
	entry, ok := dirtyPublishers[t]

	return entry, ok
}

// getDirtyPublisherByEntityName returns the publisher whose generated entity
// struct has the given Go name. This is the lookup resolveEntityPublishers uses
// because the schema's reflect.Type points at the user-supplied source struct
// (e.g. ProductEntity) while the publisher init() registers under the generated
// entity struct (e.g. Products) - distinct types in distinct packages, so a
// reflect.Type lookup would always miss.
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
//
// The copy matters: getDirtyPublisher returns a pointer into the package-global
// registry that every engine in the process shares, so writing to it would both
// leak one engine's configuration into another and race two concurrent
// Validate() calls.
func withSchemaConfig(entry *dirtyPublisherEntry, schema *entitySchema) *dirtyPublisherEntry {
	scoped := *entry
	scoped.cdc = schema.cdc
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

// ----- Publish -----

// newEntityMessage builds the message every publish path uses - the post-commit
// flush, the outbox relay and replay - including the deterministic Nats-Msg-Id
// that JetStream dedups on. One builder, so a relayed message can never drift
// from the original and lose its dedup.
//
// The id is scoped by subject and payload only. That is what makes one write one
// message: under the old per-consumer streams the id carried the stream name, so
// the same change published once per reader.
func newEntityMessage(subject Subject, op DirtyOp, payload []byte) *NatsMessage {
	msg := NewNatsMessage(string(subject))
	msg.Data = payload
	msg.Headers = nats.Header{}
	msg.Headers.Set(HeaderDirtyOp, strconv.FormatUint(uint64(op), 10))
	msg.Headers.Set("Nats-Msg-Id", fmt.Sprintf("%s:%d", subject, msgIDHash(payload)))

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

// pendingDirtyMessages accumulates messages across a whole Flush, keyed by NATS
// pool, so each pool is published in one batch instead of one round-trip per
// entity.
type pendingDirtyMessages map[string][]*NatsMessage

// buildDirtyEvent serialises the event once and appends exactly one message,
// whatever number of consumers read that entity.
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
	poolCode := reg.entityStream.NatsPool
	if pool := orm.Engine().Nats(poolCode); pool == nil {
		return fmt.Errorf("nats pool '%s' for the entity stream not configured", poolCode)
	}
	pending[poolCode] = append(pending[poolCode], newEntityMessage(publisher.subject, op, payload))

	return nil
}

// flushDirtyMessages publishes each pool's accumulated messages in one batch.
// Called at the end of the Flush entity walk; Flush still waits for the acks, so
// "flush succeeded ⇒ event durably published" continues to hold.
func flushDirtyMessages(orm Context, pending pendingDirtyMessages) error {
	for poolCode, msgs := range pending {
		pool := orm.Engine().Nats(poolCode)
		if pool == nil {
			return fmt.Errorf("nats pool '%s' for entity publish not configured", poolCode)
		}
		if err := pool.PublishBatch(orm, msgs); err != nil {
			return fmt.Errorf("publish %d entity events to pool %s: %w", len(msgs), poolCode, err)
		}
	}

	return nil
}

// msgIDHash hashes the payload bytes into a uint64 for use in Nats-Msg-Id.
// Combined with the subject it scopes the dedup window correctly: two retries
// of the same event share an id, but unrelated events don't collide.
func msgIDHash(b []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}

	return h
}

// resolveEntityPublishers runs at Validate() time to index every `cdc`-tagged
// entity's publisher by reflect.Type for the flush path.
//
// A missing publisher is tolerated: TestGenerate runs Validate before the
// freshly generated init() blocks are compiled in.
func resolveEntityPublishers(e *engineImplementation) {
	reg := e.registry
	reg.dirtyPublishers = make(map[reflect.Type]*dirtyPublisherEntry)

	for _, schema := range reg.entitySchemas {
		// An `outbox`-only entity has no consumer but still needs its publisher
		// indexed: the outbox row carries the same snapshot payload.
		if !schema.cdc && !schema.outbox {
			continue
		}
		if entry, ok := getDirtyPublisherByEntityName(generatedEntityName(schema.tableName)); ok {
			reg.dirtyPublishers[entry.reflectType] = withSchemaConfig(entry, schema)
		} else if entry, ok := getDirtyPublisher(schema.t); ok {
			reg.dirtyPublishers[entry.reflectType] = withSchemaConfig(entry, schema)
		}
	}
}
