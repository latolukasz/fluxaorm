package fluxaorm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// ErrPayloadUndecodable marks a payload that will never decode. A task
// consumer terminates such a message on its first attempt instead of retrying
// it to the cap, because the bytes on the stream are not going to improve.
var ErrPayloadUndecodable = errors.New("task payload cannot be decoded")

// StreamConsumer is the low-level pull primitive. Callers loop over Consume()
// in whatever lifecycle harness they use (cron.Job, fx lifecycle, plain
// goroutine). fluxaorm intentionally does NOT provide a blocking Run() - the
// application owns the loop, the retry policy and the shutdown semantics.
type StreamConsumer interface {
	// Consume fetches up to `batch` messages with `timeout` deadline, dispatches
	// each one, acks on nil error and leaves unacked on err. It returns the first
	// transport-level error (connection lost, stream gone, durable missing).
	// Handler errors are reported to fluxaorm's NATS log but do not return from
	// Consume - they only block the ack so JetStream can redeliver.
	Consume(ctx context.Context, batch int, timeout time.Duration) error
}

// Dispatch is a typed handler after type erasure. Generated code produces these
// through BuildDispatch; application code does not write them.
type Dispatch func(ctx Context, msg *NatsMessage) error

// BatchDispatch handles every message for one entity within a single fetched
// batch. Returning nil acks the whole group; returning err leaves the whole
// group unacked for redelivery, so batch handlers must be idempotent.
type BatchDispatch func(ctx Context, msgs []*NatsMessage) error

// ConsumerHandle is the untyped view of a generated consumer ref, for code that
// holds consumers in a table and does not know their builder types.
type ConsumerHandle interface {
	Name() ConsumerName
	Subjects() []Subject
	Enqueue(orm Context, entities []Entity) error
}

// ConsumerRef ties a declared consumer to its generated typed builder. `B` is
// the generated builder type, and inference at NewConsumer's call site resolves
// the right typed return.
//
// Constructed by generated code; application code references the generated
// value (e.g. gen.ConsumerOrderIndexer) and never instantiates this.
type ConsumerRef[B any] struct {
	name       ConsumerName
	subjects   []Subject
	newBuilder func(core *ConsumerBuilder) *B
}

// NewConsumerRef is the generator-facing factory.
func NewConsumerRef[B any](
	name ConsumerName, subjects []Subject, newBuilder func(core *ConsumerBuilder) *B,
) ConsumerRef[B] {
	return ConsumerRef[B]{name: name, subjects: subjects, newBuilder: newBuilder}
}

func (r ConsumerRef[B]) Name() ConsumerName  { return r.name }
func (r ConsumerRef[B]) Subjects() []Subject { return r.subjects }

// Enqueue publishes a synthetic DirtyUpdate event for each entity onto this
// consumer's private replay subject, without any database mutation. Use it to
// re-drive one consumer over rows that have not otherwise changed.
//
// The event mirrors the entity's currently-loaded state for both Before and
// After, so a handler using WatchFields correctly observes no change and skips
// it - only handlers without WatchFields (full reindexers) act on a replay.
//
// Every entity is published in one batch, so replaying a page of rows costs one
// round-trip rather than one per row.
func (r ConsumerRef[B]) Enqueue(orm Context, entities []Entity) error {
	return enqueueReplayEvents(orm, r.name, entities)
}

// ConsumerBuilder is the shared state every generated builder embeds. Exported
// because generated code in the application's package calls AddDispatch.
type ConsumerBuilder struct {
	engine        Engine
	consumer      *resolvedConsumer
	dispatch      map[string]Dispatch
	batchDispatch map[string]BatchDispatch
}

// AddDispatch registers a typed dispatch closure under a dispatch key - an
// entity's Go name, or a task's name. Called from generated OnX methods. Not
// for direct use.
func (b *ConsumerBuilder) AddDispatch(key string, d Dispatch) {
	b.dispatch[key] = d
}

// AddBatchDispatch registers a typed batch dispatch closure. Called from
// generated OnXBatch methods. Not for direct use.
func (b *ConsumerBuilder) AddBatchDispatch(entityName string, d BatchDispatch) {
	b.batchDispatch[entityName] = d
}

// Name is the consumer this builder is wiring.
func (b *ConsumerBuilder) Name() ConsumerName { return b.consumer.name }

// Build wraps the accumulated dispatch table into a StreamConsumer ready to be
// driven by the application's own loop.
//
// Panics if an entity has both a per-message and a batch handler: that is a
// wiring mistake, and silently preferring one would hide a dropped handler.
func (b *ConsumerBuilder) Build() StreamConsumer {
	for entityName := range b.batchDispatch {
		if _, both := b.dispatch[entityName]; both {
			panic(fmt.Sprintf(
				"consumer '%s': entity '%s' has both a per-message and a batch handler registered; keep one",
				b.consumer.name, entityName))
		}
	}

	// A task with no handler is refused outright, which is stricter than the
	// entity side's skip-and-ack. A skipped projection refresh is recoverable by
	// a replay; work that silently never runs is not.
	for _, schema := range b.consumer.tasks {
		if _, has := b.dispatch[string(schema.name)]; !has {
			panic(fmt.Sprintf(
				"consumer '%s': no handler registered for task '%s'", b.consumer.name, schema.name))
		}
	}

	return &consumerImplementation{
		engine:        b.engine,
		consumer:      b.consumer,
		dispatch:      b.dispatch,
		batchDispatch: b.batchDispatch,
	}
}

// NewConsumer starts building the consumer for one declared name. It panics
// when the name is not registered, because that means the code and the
// declaration table disagree - a wiring bug, not a runtime condition.
func NewConsumer[B any](engine Engine, ref ConsumerRef[B]) *B {
	resolved, err := resolveConsumerByName(engine, ref.Name())
	if err != nil {
		panic(err)
	}

	return ref.newBuilder(&ConsumerBuilder{
		engine:        engine,
		consumer:      resolved,
		dispatch:      make(map[string]Dispatch, len(resolved.entities)),
		batchDispatch: make(map[string]BatchDispatch, len(resolved.entities)),
	})
}

func resolveConsumerByName(engine Engine, name ConsumerName) (*resolvedConsumer, error) {
	reg, ok := engine.Registry().(*engineRegistryImplementation)
	if !ok {
		return nil, errors.New("consumer: unsupported engine")
	}
	resolved := reg.consumers[name]
	if resolved == nil {
		return nil, fmt.Errorf("consumer '%s' is not declared; add a fluxaorm.ConsumerDef for it", name)
	}

	return resolved, nil
}

type consumerImplementation struct {
	engine        Engine
	consumer      *resolvedConsumer
	dispatch      map[string]Dispatch
	batchDispatch map[string]BatchDispatch

	mu   sync.Mutex
	nats NatsConsumer
}

func (c *consumerImplementation) resolve(ctx Context) (NatsConsumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nats != nil {
		return c.nats, nil
	}
	pool := c.engine.Nats(c.consumer.options.NatsPool)
	if pool == nil {
		return nil, fmt.Errorf(
			"nats pool '%s' for consumer '%s' not configured", c.consumer.options.NatsPool, c.consumer.name)
	}
	nc, err := pool.Consumer(c.consumer.durable())
	if err != nil {
		return nil, err
	}
	c.nats = nc

	return nc, nil
}

// Consume fetches one batch and dispatches it, grouped by subject.
//
// The subject is the identity: a message on fluxa.entity.orders and one on
// fluxa.replay.<this consumer>.orders both resolve to the Orders handler, which
// is what makes a replay drive the exact code path a real change drives.
func (c *consumerImplementation) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if batch <= 0 {
		batch = 1
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	orm := c.engine.NewContext(ctx)
	nc, err := c.resolve(orm)
	if err != nil {
		return err
	}

	fetched := nc.Fetch(orm, batch, timeout)
	if fetchErr := fetched.Error(); fetchErr != nil {
		return fetchErr
	}

	metrics, hasMetrics := c.engine.Registry().getMetricsRegistry()

	// Group by entity so an entity with a batch handler gets one call for the
	// whole fetch. First-appearance order is kept so dispatch stays
	// deterministic; grouping only reorders across entities, never within one.
	order := make([]string, 0, len(c.batchDispatch)+1)
	grouped := make(map[string][]*NatsMessage)

	for _, msg := range fetched.Records() {
		entityName := c.consumer.dispatchKeyBySubject[Subject(msg.Subject)]
		if hasMetrics {
			metrics.cdcMessages.WithLabelValues(
				string(c.consumer.name), entityName, dirtyOpLabel(msg.Headers.Get(HeaderDirtyOp))).Inc()
			if !msg.Timestamp.IsZero() {
				metrics.streamLag.WithLabelValues(string(c.consumer.name)).Observe(time.Since(msg.Timestamp).Seconds())
			}
		}
		if _, seen := grouped[entityName]; !seen {
			order = append(order, entityName)
		}
		grouped[entityName] = append(grouped[entityName], msg)
	}

	for _, entityName := range order {
		c.dispatchGroup(orm, entityName, grouped[entityName])
	}

	return nil
}

func (c *consumerImplementation) dispatchGroup(orm Context, entityName string, msgs []*NatsMessage) {
	if c.consumer.isTaskConsumer() {
		for _, msg := range msgs {
			c.handleTask(orm, entityName, msg)
		}

		return
	}

	if batchDispatch, ok := c.batchDispatch[entityName]; ok {
		if err := batchDispatch(orm, msgs); err != nil {
			c.logHandlerError(orm, msgs[0], fmt.Errorf("batch of %d: %w", len(msgs), err))

			return
		}
		c.ackAll(orm, msgs)

		return
	}

	dispatch, ok := c.dispatch[entityName]
	if !ok {
		// An entity added to this consumer after it was deployed, or a subject
		// nothing here handles. Ack and move on: the alternative is a message
		// redelivering forever against code that will never handle it.
		c.ackAll(orm, msgs)

		return
	}

	for _, msg := range msgs {
		if err := dispatch(orm, msg); err != nil {
			c.logHandlerError(orm, msg, err)

			continue
		}
		if ackErr := msg.Ack(); ackErr != nil {
			c.logHandlerError(orm, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}
}

func (c *consumerImplementation) ackAll(orm Context, msgs []*NatsMessage) {
	for _, msg := range msgs {
		if ackErr := msg.Ack(); ackErr != nil {
			c.logHandlerError(orm, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}
}

func (c *consumerImplementation) logHandlerError(ctx Context, msg *NatsMessage, err error) {
	_, loggers := ctx.getNatsLoggers()
	if len(loggers) == 0 {
		return
	}
	fillLogFields(ctx, loggers, string(c.consumer.name), sourceNats, "CDC_HANDLE", "subject: "+msg.Subject, nil, false, err)
}

// enqueueReplayEvents publishes synthetic DirtyUpdate events onto one
// consumer's replay subjects, in one batch.
//
// Each message gets a unique Nats-Msg-Id so the dedup window never collapses
// repeated replays of the same row - an operator asking for a reindex twice
// means it.
func enqueueReplayEvents(orm Context, consumer ConsumerName, entities []Entity) error {
	if len(entities) == 0 {
		return nil
	}

	reg, ok := orm.Engine().Registry().(*engineRegistryImplementation)
	if !ok {
		return errors.New("enqueue: unsupported engine")
	}
	resolved := reg.consumers[consumer]
	if resolved == nil {
		return fmt.Errorf("consumer '%s' is not declared", consumer)
	}
	pool := orm.Engine().Nats(reg.entityStream.NatsPool)
	if pool == nil {
		return fmt.Errorf("nats pool '%s' for the entity stream not configured", reg.entityStream.NatsPool)
	}

	msgs := make([]*NatsMessage, 0, len(entities))
	for _, entity := range entities {
		if entity == nil {
			return errors.New("enqueue: entity is nil")
		}
		t := reflect.TypeOf(entity)
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		publisher, hasPub := reg.dirtyPublishers[t]
		if !hasPub {
			return fmt.Errorf("no dirty publisher registered for entity %s", t.Name())
		}
		if _, declared := resolved.dispatchKeyBySubject[publisher.subject]; !declared {
			return fmt.Errorf("consumer '%s' does not declare entity %s", consumer, publisher.entityName)
		}

		payload, err := publisher.buildEvent(entity, DirtyUpdate, nil)
		if err != nil {
			return fmt.Errorf("build dirty event for %s: %w", publisher.entityName, err)
		}
		if payload == nil {
			continue
		}

		msg := newEntityMessage(ReplaySubject(consumer, publisher.tableName), DirtyUpdate, payload)
		msg.Headers.Set("Nats-Msg-Id",
			fmt.Sprintf("replay:%s:%s:%d:%d", consumer, publisher.entityName, entity.GetID(), time.Now().UnixNano()))
		msgs = append(msgs, msg)
	}

	if err := pool.PublishBatch(orm, msgs); err != nil {
		return fmt.Errorf("enqueue %d replay events for consumer %s: %w", len(msgs), consumer, err)
	}

	return nil
}

// ConsumerPending reports how many messages a consumer still has to deliver.
//
// This is the backlog signal - "is work piling up on this consumer" - and it
// lives here because the durable name is fluxaorm's to decide, so an
// application asking JetStream directly would have to hardcode the convention.
func ConsumerPending(ctx Context, consumer ConsumerName) (uint64, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, errors.New("consumer pending: unsupported context")
	}
	resolved := orm.engine.registry.consumers[consumer]
	if resolved == nil {
		return 0, fmt.Errorf("consumer '%s' is not declared", consumer)
	}

	js, err := orm.engine.Nats(resolved.options.NatsPool).GetJetStream()
	if err != nil {
		return 0, err
	}
	nc, err := js.Consumer(ctx.Context(), resolved.stream, resolved.durable())
	if err != nil {
		return 0, err
	}
	info, err := nc.Info(ctx.Context())
	if err != nil {
		return 0, err
	}

	return info.NumPending, nil
}

// handleTask owns the whole retry ladder for one task message. Every path
// either acks, naks with a delay, or terminates - a message must never be left
// to time out silently except on shutdown, where redelivery is exactly what
// should happen.
func (c *consumerImplementation) handleTask(orm Context, taskName string, msg *NatsMessage) {
	runID, _ := strconv.ParseUint(msg.Headers.Get(HeaderJobRunID), 10, 64)
	orm.SetMetaData(MetricsMetaKey, taskName)

	reg := c.engine.Registry().(*engineRegistryImplementation)
	schema := reg.tasks[TaskName(taskName)]
	handler := c.dispatch[taskName]

	if schema == nil || handler == nil {
		// A rolled back deploy produces this. Terminating stops it spinning, and
		// closing the row out stops the backlog gauge reporting a task that has
		// in fact been dead-lettered.
		_ = msg.Term()
		c.finishRun(orm, runID, JobRunResult{Status: JobRunFailed, Err: "no handler for task " + taskName})
		c.logHandlerError(orm, msg,
			fmt.Errorf("consumer '%s': terminated message for unhandled task '%s'", c.consumer.name, taskName))

		return
	}

	attempt := uint8(min(msg.Deliveries, uint64(schema.maxAttempts)))
	if runID != 0 {
		if err := MarkJobRunStarted(orm, runID, attempt); err != nil {
			c.logHandlerError(orm, msg, err)
		}
	}

	start := time.Now()
	err := handler(orm, msg)
	duration := time.Since(start)

	switch {
	case err == nil:
		_ = msg.Ack()
		c.finishRun(orm, runID, JobRunResult{Status: JobRunSucceeded, Duration: duration})
	case errors.Is(err, context.Canceled):
		// Shutdown, not failure. Leaving it unacked hands it back to JetStream
		// without burning an attempt.
		return
	case errors.Is(err, ErrPayloadUndecodable):
		_ = msg.Term()
		c.finishRun(orm, runID, JobRunResult{Status: JobRunFailed, Err: err.Error(), Duration: duration})
		c.logHandlerError(orm, msg, err)
	case msg.Deliveries >= uint64(schema.maxAttempts):
		_ = msg.Term()
		c.finishRun(orm, runID, JobRunResult{Status: JobRunFailed, Err: err.Error(), Duration: duration})
		c.logHandlerError(orm, msg, err)
	default:
		_ = msg.NakWithDelay(taskBackoff(schema, msg.Deliveries))
		c.finishRun(orm, runID, JobRunResult{Status: JobRunPending, Err: err.Error(), Duration: duration})
		c.logHandlerError(orm, msg, err)
	}
}

func (c *consumerImplementation) finishRun(orm Context, runID uint64, res JobRunResult) {
	if runID == 0 {
		return
	}
	if err := MarkJobRunFinished(orm, runID, res); err != nil {
		_, loggers := orm.getNatsLoggers()
		if len(loggers) > 0 {
			fillLogFields(orm, loggers, string(c.consumer.name), sourceNats, "TASK_HANDLE", "", nil, false, err)
		}
	}
}

// taskBackoff doubles from BaseBackoff up to MaxBackoff, with jitter so a batch
// of tasks that failed together does not come back in lockstep.
func taskBackoff(schema *taskSchema, deliveries uint64) time.Duration {
	shift := deliveries
	if shift > 0 {
		shift--
	}
	if shift > 32 {
		shift = 32
	}

	delay := schema.baseBackoff << shift
	if delay > schema.maxBackoff || delay <= 0 {
		delay = schema.maxBackoff
	}

	jitter := time.Duration(rand.Int64N(int64(delay) / 4)) //nolint:gosec // jitter, not a secret

	return delay + jitter
}

// BuildTaskDispatch erases a typed task handler's payload type. A decode
// failure comes back wrapped in ErrPayloadUndecodable so the consumer can tell
// a bad message from a failing handler and dead-letter it immediately.
func BuildTaskDispatch[T any](handler func(ctx Context, task *T) error) Dispatch {
	return func(ctx Context, msg *NatsMessage) error {
		task := new(T)
		if err := json.Unmarshal(msg.Data, task); err != nil {
			return fmt.Errorf("%w: %s", ErrPayloadUndecodable, err.Error())
		}

		return handler(ctx, task)
	}
}
