package fluxaorm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// NatsStreamName is the universal typed handle for any fluxaorm-managed JetStream stream.
// CDC streams' names come from generated CDCStreamRef values; non-CDC streams' names are
// declared by the user at registration.
type NatsStreamName string

// StreamHandler is the user's per-message callback. Return nil to ack the message;
// return err to leave it unacked (JetStream redelivers after AckWait).
type StreamHandler func(ctx Context, msg *NatsMessage) error

// StreamConsumer is the low-level pull primitive. Callers loop over Consume() in
// whatever lifecycle harness they use (cron.Job, fx lifecycle, plain goroutine).
// fluxaorm intentionally does NOT provide a blocking Run() — the application owns
// the loop, retry policy, and shutdown semantics.
type StreamConsumer interface {
	// Consume fetches up to `batch` messages with `timeout` deadline, invokes the
	// handler per message, acks on nil error, leaves unacked on err. Returns the
	// first transport-level error (connection lost, stream gone, durable missing).
	// Per-message handler errors are reported to fluxaorm's NATS log but do not
	// return from Consume — they only block ack so JetStream can redeliver.
	Consume(ctx context.Context, batch int, timeout time.Duration) error
}

// StreamConsumerOption configures a StreamConsumer / CDCConsumer at construction.
type StreamConsumerOption func(*streamConsumerConfig)

type streamConsumerConfig struct {
	description string
}

// WithDescription attaches a human-readable description; used for metrics/log labels only.
func WithDescription(d string) StreamConsumerOption {
	return func(c *streamConsumerConfig) { c.description = d }
}

// NewStreamConsumer creates a generic consumer for any registered NATS stream.
// The JetStream durable used is the auto-derived `<streamName>-workers`. Multiple
// processes constructing a consumer for the same stream automatically share workload.
func NewStreamConsumer(engine Engine, stream NatsStreamName, handler StreamHandler, opts ...StreamConsumerOption) StreamConsumer {
	cfg := &streamConsumerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return &streamConsumerImpl{
		engine:      engine,
		streamName:  stream,
		durableName: DurableForStream(stream),
		handler:     handler,
		cfg:         cfg,
	}
}

// DurableForStream returns the auto-derived JetStream durable consumer name for a stream.
// Workers using the same durable share workload (JetStream consumer-group semantics).
func DurableForStream(stream NatsStreamName) string {
	return string(stream) + "-workers"
}

type streamConsumerImpl struct {
	engine      Engine
	streamName  NatsStreamName
	durableName string
	handler     StreamHandler
	cfg         *streamConsumerConfig

	mu       sync.Mutex
	consumer NatsConsumer
}

// resolveStreamConsumer returns the NATS pool and durable consumer for this stream,
// initialising it lazily on first call.
func (c *streamConsumerImpl) resolveStreamConsumer(ctx Context) (NatsConsumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumer != nil {
		return c.consumer, nil
	}
	reg := c.engine.Registry().(*engineRegistryImplementation)
	entry, ok := reg.streamRegistry[c.streamName]
	if !ok {
		return nil, fmt.Errorf("stream '%s' not registered", c.streamName)
	}
	pool := c.engine.Nats(entry.poolCode)
	if pool == nil {
		return nil, fmt.Errorf("nats pool '%s' for stream '%s' not configured", entry.poolCode, c.streamName)
	}
	cons, err := pool.Consumer(c.durableName)
	if err != nil {
		return nil, err
	}
	c.consumer = cons
	return cons, nil
}

func (c *streamConsumerImpl) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if batch <= 0 {
		batch = 1
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ormCtx := c.engine.NewContext(ctx)
	cons, err := c.resolveStreamConsumer(ormCtx)
	if err != nil {
		return err
	}
	natsBatch := cons.Fetch(ormCtx, batch, timeout)
	if fetchErr := natsBatch.Error(); fetchErr != nil {
		// Idle timeouts (no messages) are reported by Fetch already filtered; any
		// remaining error is a fatal transport-level error.
		return fetchErr
	}
	for _, msg := range natsBatch.Records() {
		if err := c.handler(ormCtx, msg); err != nil {
			// Per-message handler error: log and leave unacked.
			c.logHandlerError(ormCtx, msg, err)
			continue
		}
		if ackErr := msg.Ack(); ackErr != nil {
			c.logHandlerError(ormCtx, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}
	return nil
}

func (c *streamConsumerImpl) logHandlerError(ctx Context, msg *NatsMessage, err error) {
	_, loggers := ctx.getNatsLoggers()
	if len(loggers) == 0 {
		return
	}
	fillLogFields(ctx, loggers, string(c.streamName), sourceNats, "STREAM_HANDLE", "subject: "+msg.Subject, nil, false, err)
}

// errStreamUnregistered is the sentinel for streams not in registry.streamRegistry.
var errStreamUnregistered = errors.New("stream not registered")
