package fluxaorm

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type AsyncFlushOptions struct {
	StreamReplicas  int
	DuplicateWindow time.Duration
	MaxAckPending   int
	AckWait         time.Duration
	MaxDeliver      int
}

type NatsPoolOptions struct {
	ClientID             string
	MaxReconnects        int
	ReconnectWait        time.Duration
	ReconnectBufSize     int
	PingInterval         time.Duration
	ConnectTimeout       time.Duration
	RetryOnFailedConnect bool
	Auth                 *NatsAuthConfig
	IgnoredSubjects      []string
	IgnoredConsumers     []string
}

type NatsAuthConfig struct {
	Token     string
	User      string
	Password  string
	CredsFile string
	NKeySeed  string
}

type NatsConsumerSettings struct {
	Name           string
	FilterSubjects []string
	AckWait        time.Duration
	MaxAckPending  int
	MaxDeliver     int
	DeliverPolicy  jetstream.DeliverPolicy
}

type NatsMessage struct {
	Subject   string
	Data      []byte
	Headers   nats.Header
	Sequence  uint64
	Timestamp time.Time

	jsMsg jetstream.Msg
}

func NewNatsMessage(subject string) *NatsMessage {
	return &NatsMessage{Subject: subject, Headers: nats.Header{}}
}

func (m *NatsMessage) Ack() error {
	if m.jsMsg == nil {
		return nil
	}
	return m.jsMsg.Ack()
}

func (m *NatsMessage) Nak() error {
	if m.jsMsg == nil {
		return nil
	}
	return m.jsMsg.Nak()
}

func (m *NatsMessage) Term() error {
	if m.jsMsg == nil {
		return nil
	}
	return m.jsMsg.Term()
}

type NatsBatch struct {
	messages []*NatsMessage
	fetchErr error
}

func (b NatsBatch) Records() []*NatsMessage {
	return b.messages
}

func (b NatsBatch) EachRecord(fn func(*NatsMessage)) {
	for _, m := range b.messages {
		fn(m)
	}
}

func (b NatsBatch) EachError(fn func(error)) {
	if b.fetchErr != nil {
		fn(b.fetchErr)
	}
}

func (b NatsBatch) Error() error {
	return b.fetchErr
}

func (b NatsBatch) IsEmpty() bool {
	return len(b.messages) == 0
}

type Nats interface {
	GetCode() string
	GetURLs() []string
	GetPoolOptions() *NatsPoolOptions
	Ping() error
	Publish(ctx Context, msg *NatsMessage) error
	PublishBatch(ctx Context, msgs []*NatsMessage) error
	PublishAsync(ctx Context, msg *NatsMessage, callback func(*NatsMessage, error))
	Consumer(name string) (NatsConsumer, error)
	MustConsumer(name string) NatsConsumer
	ConsumerNames() []string
	GetJetStream() (jetstream.JetStream, error)
	GetConn() (*nats.Conn, error)
	Close()
}

type NatsConsumer interface {
	GetName() string
	GetSettings() *NatsConsumerSettings
	Fetch(ctx Context, batch int, maxWait time.Duration) NatsBatch
	Close()
}

type natsPoolConfig struct {
	code      string
	urls      []string
	options   *NatsPoolOptions
	consumers map[string]*NatsConsumerSettings
}

type natsPoolImplementation struct {
	config               *natsPoolConfig
	nc                   *nats.Conn
	js                   jetstream.JetStream
	producerMu           sync.Mutex
	hasRegisteredStreams bool
}

func buildNatsConnectOpts(cfg *natsPoolConfig) []nats.Option {
	opts := []nats.Option{
		nats.Name(cfg.options.ClientID),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if cfg.options.MaxReconnects != 0 {
		opts = append(opts, nats.MaxReconnects(cfg.options.MaxReconnects))
	}
	if cfg.options.ReconnectWait > 0 {
		opts = append(opts, nats.ReconnectWait(cfg.options.ReconnectWait))
	}
	if cfg.options.ReconnectBufSize > 0 {
		opts = append(opts, nats.ReconnectBufSize(cfg.options.ReconnectBufSize))
	}
	if cfg.options.PingInterval > 0 {
		opts = append(opts, nats.PingInterval(cfg.options.PingInterval))
	}
	if cfg.options.ConnectTimeout > 0 {
		opts = append(opts, nats.Timeout(cfg.options.ConnectTimeout))
	}
	if cfg.options.RetryOnFailedConnect {
		opts = append(opts, nats.RetryOnFailedConnect(true))
	}
	if auth := cfg.options.Auth; auth != nil {
		switch {
		case auth.CredsFile != "":
			opts = append(opts, nats.UserCredentials(auth.CredsFile))
		case auth.NKeySeed != "":
			kp, err := nats.NkeyOptionFromSeed(auth.NKeySeed)
			if err == nil {
				opts = append(opts, kp)
			}
		case auth.Token != "":
			opts = append(opts, nats.Token(auth.Token))
		case auth.User != "":
			opts = append(opts, nats.UserInfo(auth.User, auth.Password))
		}
	}
	return opts
}

// initProducer lazily creates the NATS connection and JetStream context on first use.
// Retries on subsequent calls if the previous attempt failed (transient outage).
// Thread-safe: concurrent callers serialize on producerMu.
func (p *natsPoolImplementation) initProducer() error {
	p.producerMu.Lock()
	defer p.producerMu.Unlock()

	if p.nc != nil && p.nc.Status() == nats.CONNECTED && p.js != nil {
		return nil
	}
	if p.nc != nil {
		p.nc.Close()
		p.nc = nil
		p.js = nil
	}

	urls := make([]string, 0, len(p.config.urls))
	for _, u := range p.config.urls {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return fmt.Errorf("nats pool '%s': no urls configured", p.config.code)
	}
	url := strings.Join(urls, ",")
	nc, err := nats.Connect(url, buildNatsConnectOpts(p.config)...)
	if err != nil {
		return fmt.Errorf("nats pool '%s': failed to connect to any of %d urls: %w", p.config.code, len(urls), err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("nats pool '%s': failed to create JetStream context: %w", p.config.code, err)
	}

	p.nc = nc
	p.js = js
	return nil
}

func (p *natsPoolImplementation) Ping() error {
	return p.initProducer()
}

func (p *natsPoolImplementation) GetCode() string {
	return p.config.code
}

func (p *natsPoolImplementation) GetURLs() []string {
	return p.config.urls
}

func (p *natsPoolImplementation) GetPoolOptions() *NatsPoolOptions {
	return p.config.options
}

func (p *natsPoolImplementation) GetJetStream() (jetstream.JetStream, error) {
	if err := p.initProducer(); err != nil {
		return nil, err
	}
	return p.js, nil
}

func (p *natsPoolImplementation) GetConn() (*nats.Conn, error) {
	if err := p.initProducer(); err != nil {
		return nil, err
	}
	return p.nc, nil
}

func (p *natsPoolImplementation) Publish(ctx Context, msg *NatsMessage) error {
	if err := p.initProducer(); err != nil {
		return err
	}
	hasLogger, _ := ctx.getNatsLoggers()
	start := time.Now()

	natsMsg := &nats.Msg{Subject: msg.Subject, Data: msg.Data, Header: msg.Headers}
	_, err := p.js.PublishMsg(ctx.Context(), natsMsg)

	duration := time.Since(start)
	if hasLogger {
		p.fillLogFields(ctx, "PUBLISH", "subject: "+msg.Subject, duration, err)
	}
	p.fillMetrics(ctx, duration, "publish", err)
	return err
}

// PublishBatch writes every message to the connection without waiting on each
// ack, then waits once for all of them. Publish order on the wire is preserved,
// so the server assigns stream sequences in the same order a Publish loop would;
// only ack completion is out of order. Returns the first ack error.
func (p *natsPoolImplementation) PublishBatch(ctx Context, msgs []*NatsMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := p.initProducer(); err != nil {
		return err
	}
	hasLogger, _ := ctx.getNatsLoggers()
	start := time.Now()

	futures := make([]jetstream.PubAckFuture, 0, len(msgs))
	var err error
	for _, msg := range msgs {
		natsMsg := &nats.Msg{Subject: msg.Subject, Data: msg.Data, Header: msg.Headers}
		future, publishErr := p.js.PublishMsgAsync(natsMsg)
		if publishErr != nil {
			err = publishErr
			break
		}
		futures = append(futures, future)
	}

	for _, future := range futures {
		select {
		case <-future.Ok():
		case ackErr := <-future.Err():
			if err == nil {
				err = ackErr
			}
		case <-ctx.Context().Done():
			if err == nil {
				err = ctx.Context().Err()
			}
		}
	}

	duration := time.Since(start)
	if hasLogger {
		p.fillLogFields(ctx, "PUBLISH_BATCH", fmt.Sprintf("messages: %d", len(msgs)), duration, err)
	}
	p.fillMetrics(ctx, duration, "publish", err)
	p.fillBatchMetrics(ctx, len(msgs))

	return err
}

func (p *natsPoolImplementation) PublishAsync(ctx Context, msg *NatsMessage, callback func(*NatsMessage, error)) {
	if err := p.initProducer(); err != nil {
		if callback != nil {
			callback(nil, err)
		}
		return
	}
	hasLogger, _ := ctx.getNatsLoggers()
	start := time.Now()

	natsMsg := &nats.Msg{Subject: msg.Subject, Data: msg.Data, Header: msg.Headers}
	future, err := p.js.PublishMsgAsync(natsMsg)
	if err != nil {
		duration := time.Since(start)
		if hasLogger {
			p.fillLogFields(ctx, "PUBLISH_ASYNC", "subject: "+msg.Subject, duration, err)
		}
		p.fillMetrics(ctx, duration, "publish_async", err)
		if callback != nil {
			callback(nil, err)
		}
		return
	}

	go func() {
		var ackErr error
		select {
		case <-future.Ok():
		case ackErr = <-future.Err():
		}
		duration := time.Since(start)
		if hasLogger {
			p.fillLogFields(ctx, "PUBLISH_ASYNC", "subject: "+msg.Subject, duration, ackErr)
		}
		p.fillMetrics(ctx, duration, "publish_async", ackErr)
		if callback != nil {
			callback(msg, ackErr)
		}
	}()
}

func (p *natsPoolImplementation) Consumer(name string) (NatsConsumer, error) {
	settings, ok := p.config.consumers[name]
	if !ok {
		return nil, fmt.Errorf("nats pool '%s': consumer '%s' not registered", p.config.code, name)
	}
	if err := p.initProducer(); err != nil {
		return nil, err
	}
	return &natsConsumerImplementation{
		poolCode: p.config.code,
		settings: settings,
		js:       p.js,
	}, nil
}

func (p *natsPoolImplementation) MustConsumer(name string) NatsConsumer {
	c, err := p.Consumer(name)
	if err != nil {
		panic(err)
	}
	return c
}

func (p *natsPoolImplementation) ConsumerNames() []string {
	names := make([]string, 0, len(p.config.consumers))
	for name := range p.config.consumers {
		names = append(names, name)
	}
	return names
}

func (p *natsPoolImplementation) fillMetrics(ctx Context, duration time.Duration, operation string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesNats.WithLabelValues(operation, p.config.code, ctx.getMetricsSourceTag(), "").Observe(duration.Seconds())
		if err != nil {
			metrics.queriesNatsErrors.WithLabelValues(p.config.code, ctx.getMetricsSourceTag(), "").Inc()
		}
	}
}

func (p *natsPoolImplementation) fillBatchMetrics(ctx Context, size int) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.natsPublishBatchSize.WithLabelValues(p.config.code, ctx.getMetricsSourceTag()).Observe(float64(size))
	}
}

func (p *natsPoolImplementation) fillLogFields(ctx Context, operation, message string, duration time.Duration, err error) {
	_, loggers := ctx.getNatsLoggers()
	fillLogFields(ctx, loggers, p.config.code, sourceNats, operation, message, &duration, false, err)
}

func (p *natsPoolImplementation) Close() {
	if p.nc != nil {
		p.nc.Close()
		p.nc = nil
		p.js = nil
	}
}

type natsConsumerImplementation struct {
	poolCode string
	settings *NatsConsumerSettings
	js       jetstream.JetStream
	consumer jetstream.Consumer
	mu       sync.Mutex
}

func (c *natsConsumerImplementation) GetName() string {
	return c.settings.Name
}

func (c *natsConsumerImplementation) GetSettings() *NatsConsumerSettings {
	return c.settings
}

func (c *natsConsumerImplementation) resolveConsumer(ctx Context) (jetstream.Consumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumer != nil {
		return c.consumer, nil
	}
	// Drain the entire stream lister before consulting Err() — the jetstream lister
	// writes to its error field from a background goroutine, so an early break would race.
	var streamNames []string
	streams := c.js.ListStreams(ctx.Context())
	for info := range streams.Info() {
		if info == nil {
			continue
		}
		streamNames = append(streamNames, info.Config.Name)
	}
	if err := streams.Err(); err != nil {
		return nil, err
	}
	for _, name := range streamNames {
		cons, err := c.js.Consumer(ctx.Context(), name, c.settings.Name)
		if err == nil {
			c.consumer = cons
			return cons, nil
		}
	}
	return nil, fmt.Errorf("nats pool '%s': durable consumer '%s' not found in any stream", c.poolCode, c.settings.Name)
}

func (c *natsConsumerImplementation) Fetch(ctx Context, batch int, maxWait time.Duration) NatsBatch {
	hasLogger, _ := ctx.getNatsLoggers()
	start := time.Now()

	cons, err := c.resolveConsumer(ctx)
	if err != nil {
		duration := time.Since(start)
		if hasLogger {
			c.fillLogFields(ctx, "FETCH", "resolve consumer", duration, err)
		}
		c.fillMetrics(ctx, duration, "fetch", err)
		return NatsBatch{fetchErr: err}
	}

	msgs, err := cons.Fetch(batch, jetstream.FetchMaxWait(maxWait))
	duration := time.Since(start)
	if err != nil {
		if hasLogger {
			c.fillLogFields(ctx, "FETCH", "0 messages fetched", duration, err)
		}
		c.fillMetrics(ctx, duration, "fetch", err)
		return NatsBatch{fetchErr: err}
	}

	var collected []*NatsMessage
	for m := range msgs.Messages() {
		nm := &NatsMessage{
			Subject: m.Subject(),
			Data:    m.Data(),
			Headers: m.Headers(),
			jsMsg:   m,
		}
		if meta, mErr := m.Metadata(); mErr == nil && meta != nil {
			nm.Sequence = meta.Sequence.Stream
			nm.Timestamp = meta.Timestamp
		}
		collected = append(collected, nm)
	}
	fetchErr := msgs.Error()
	// Treat context.DeadlineExceeded / no-responders as not-an-error (idle poll).
	if fetchErr != nil && errors.Is(fetchErr, nats.ErrTimeout) {
		fetchErr = nil
	}
	if hasLogger {
		c.fillLogFields(ctx, "FETCH", fmt.Sprintf("%d messages fetched", len(collected)), duration, fetchErr)
	}
	c.fillMetrics(ctx, duration, "fetch", fetchErr)
	return NatsBatch{messages: collected, fetchErr: fetchErr}
}

func (c *natsConsumerImplementation) Close() {
	// JetStream pull consumers do not require explicit close on the consumer object —
	// they're a thin handle over the parent connection. No-op for symmetry with Kafka API.
}

func (c *natsConsumerImplementation) fillMetrics(ctx Context, duration time.Duration, operation string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesNats.WithLabelValues(operation, c.poolCode, ctx.getMetricsSourceTag(), c.settings.Name).Observe(duration.Seconds())
		if err != nil {
			metrics.queriesNatsErrors.WithLabelValues(c.poolCode, ctx.getMetricsSourceTag(), c.settings.Name).Inc()
		}
	}
}

func (c *natsConsumerImplementation) fillLogFields(ctx Context, operation, message string, duration time.Duration, err error) {
	_, loggers := ctx.getNatsLoggers()
	poolLabel := c.poolCode + "/" + c.settings.Name
	fillLogFields(ctx, loggers, poolLabel, sourceNats, operation, message, &duration, false, err)
}
