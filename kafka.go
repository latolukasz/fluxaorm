package fluxaorm

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type AsyncFlushOptions struct {
	TopicPartitions int32 // default: 1
}

type KafkaPoolOptions struct {
	ClientID              string
	RequiredAcks          int // 0=none, 1=leader, -1=all
	ProducerLinger        time.Duration
	MaxBufferedRecords    int
	SASL                  *KafkaSASLConfig
	IgnoredTopics         []string
	IgnoredConsumerGroups []string
}

type KafkaConsumerGroupSettings struct {
	Name               string
	Topics             []string
	SessionTimeout     time.Duration
	RebalanceTimeout   time.Duration
	FetchMaxBytes      int32
	AutoCommitInterval time.Duration // 0 = manual commit only

	// Dead-letter queue settings. Set via KafkaConsumerGroupBuilder.WithDeadLetter()
	// on the user-facing consumer group; propagated automatically.
	DeadLetterEnabled         bool
	DeadLetterMaxAttempts     int
	DeadLetterTopicPartitions int32

	// DeadLetterParentGroup is non-empty only on the auto-registered sibling
	// consumer group that drains the DLQ topic. Its value is the name of the
	// parent consumer group whose failures it retries.
	DeadLetterParentGroup string
}

type KafkaSASLConfig struct {
	Mechanism string // "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	User      string
	Password  string
}

type KafkaRecord struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []KafkaRecordHeader
	Partition int32
	Offset    int64
	Timestamp time.Time
}

type KafkaRecordHeader struct {
	Key   string
	Value []byte
}

type KafkaFetches struct {
	fetches kgo.Fetches
	cg      *kafkaConsumerGroupImplementation
	ctx     Context
}

func (f KafkaFetches) Records() []*KafkaRecord {
	var records []*KafkaRecord
	f.fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, fromKgoRecord(r))
	})
	return records
}

func (f KafkaFetches) EachRecord(fn func(*KafkaRecord)) {
	f.fetches.EachRecord(func(r *kgo.Record) {
		fn(fromKgoRecord(r))
	})
}

func (f KafkaFetches) EachError(fn func(string, int32, error)) {
	f.fetches.EachError(fn)
}

func (f KafkaFetches) IsEmpty() bool {
	return len(f.fetches) == 0
}

// EachDebeziumEvent iterates over Debezium CDC events in the fetches.
// Each record is parsed into an entity ID and DebeziumEvent.
// Tombstone records (nil value) are skipped silently.
//
// Behavior depends on the consumer group's dead-letter configuration:
//
//   - If no DLQ is configured, a non-nil error from fn (or a parse error) stops
//     iteration, no offsets are committed, and the error is returned.
//
//   - If the consumer group was registered with WithDeadLetter(), errors from fn
//     (and parse errors) cause the raw record to be produced to "_dlq_<group>"
//     with retry headers; iteration continues. On clean finish, offsets are
//     committed automatically.
//
//   - If the consumer group is the auto-registered DLQ sibling (DeadLetterParentGroup
//     set), errors from fn cause the record to be requeued to the tail of the
//     same DLQ topic with dlq-attempts incremented. When dlq-attempts reaches
//     the parent's MaxAttempts, the record is parked (committed, not re-produced).
//     Offsets are committed automatically on clean finish.
func (f KafkaFetches) EachDebeziumEvent(fn func(entityID uint64, event *DebeziumEvent) error) error {
	if f.cg == nil {
		return f.eachDebeziumEventLegacy(fn)
	}
	settings := f.cg.settings
	switch {
	case settings.DeadLetterParentGroup != "":
		return f.eachDebeziumEventDLQConsumer(fn)
	case settings.DeadLetterEnabled:
		return f.eachDebeziumEventWithDLQ(fn)
	default:
		return f.eachDebeziumEventLegacy(fn)
	}
}

func (f KafkaFetches) eachDebeziumEventLegacy(fn func(entityID uint64, event *DebeziumEvent) error) error {
	var retErr error
	f.fetches.EachRecord(func(r *kgo.Record) {
		if retErr != nil {
			return
		}
		record := fromKgoRecord(r)
		if record.Value == nil {
			return
		}
		entityID, err := ParseDebeziumKey(record)
		if err != nil {
			retErr = fmt.Errorf("record topic '%s' offset %d: %w", record.Topic, record.Offset, err)
			return
		}
		event, err := ParseDebeziumEvent(record)
		if err != nil {
			retErr = fmt.Errorf("record topic '%s' offset %d: %w", record.Topic, record.Offset, err)
			return
		}
		retErr = fn(entityID, event)
	})
	return retErr
}

func (f KafkaFetches) eachDebeziumEventWithDLQ(fn func(entityID uint64, event *DebeziumEvent) error) error {
	dlqTopic := DeadLetterTopicName(f.cg.settings.Name)
	var infraErr error
	f.fetches.EachRecord(func(r *kgo.Record) {
		if infraErr != nil {
			return
		}
		record := fromKgoRecord(r)
		if record.Value == nil {
			return
		}
		entityID, err := ParseDebeziumKey(record)
		if err != nil {
			infraErr = f.sendToDLQ(dlqTopic, record, fmt.Errorf("parse debezium key: %w", err))
			return
		}
		event, err := ParseDebeziumEvent(record)
		if err != nil {
			infraErr = f.sendToDLQ(dlqTopic, record, fmt.Errorf("parse debezium event: %w", err))
			return
		}
		if err := fn(entityID, event); err != nil {
			infraErr = f.sendToDLQ(dlqTopic, record, err)
			return
		}
	})
	if infraErr != nil {
		return infraErr
	}
	return f.cg.CommitUncommittedOffsets(f.ctx)
}

func (f KafkaFetches) eachDebeziumEventDLQConsumer(fn func(entityID uint64, event *DebeziumEvent) error) error {
	settings := f.cg.settings
	dlqTopic := DeadLetterTopicName(settings.DeadLetterParentGroup)
	maxAttempts := settings.DeadLetterMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = DefaultDeadLetterMaxAttempts
	}
	var infraErr error
	f.fetches.EachRecord(func(r *kgo.Record) {
		if infraErr != nil {
			return
		}
		record := fromKgoRecord(r)
		if record.Value == nil {
			return
		}
		entityID, err := ParseDebeziumKey(record)
		if err != nil {
			f.parkDLQRecord(record, fmt.Errorf("parse debezium key: %w", err))
			return
		}
		event, err := ParseDebeziumEvent(record)
		if err != nil {
			f.parkDLQRecord(record, fmt.Errorf("parse debezium event: %w", err))
			return
		}
		if err := fn(entityID, event); err != nil {
			md := ReadDeadLetterMetadata(record)
			nextAttempts := md.Attempts + 1
			if nextAttempts >= maxAttempts {
				f.parkDLQRecord(record, err)
				return
			}
			infraErr = f.requeueDLQRecord(dlqTopic, record, err, nextAttempts)
			return
		}
	})
	if infraErr != nil {
		return infraErr
	}
	return f.cg.CommitUncommittedOffsets(f.ctx)
}

func (f KafkaFetches) sendToDLQ(dlqTopic string, record *KafkaRecord, cause error) error {
	dlqRecord := buildInitialDeadLetterRecord(dlqTopic, record, cause.Error(), time.Now())
	if err := f.cg.pool.ProduceSync(f.ctx, dlqRecord); err != nil {
		return fmt.Errorf("produce to dead-letter topic '%s': %w", dlqTopic, err)
	}
	if metrics, has := f.ctx.Engine().Registry().getMetricsRegistry(); has {
		metrics.kafkaDLQDeadLettered.WithLabelValues(f.cg.poolCode, f.cg.settings.Name).Inc()
	}
	return nil
}

func (f KafkaFetches) requeueDLQRecord(dlqTopic string, record *KafkaRecord, cause error, nextAttempts int) error {
	dlqRecord := buildRequeueDeadLetterRecord(dlqTopic, record, cause.Error(), nextAttempts, time.Now())
	if err := f.cg.pool.ProduceSync(f.ctx, dlqRecord); err != nil {
		return fmt.Errorf("requeue to dead-letter topic '%s': %w", dlqTopic, err)
	}
	if metrics, has := f.ctx.Engine().Registry().getMetricsRegistry(); has {
		metrics.kafkaDLQRequeued.WithLabelValues(f.cg.poolCode, f.cg.settings.Name).Inc()
	}
	return nil
}

func (f KafkaFetches) parkDLQRecord(record *KafkaRecord, cause error) {
	if metrics, has := f.ctx.Engine().Registry().getMetricsRegistry(); has {
		metrics.kafkaDLQParked.WithLabelValues(f.cg.poolCode, f.cg.settings.Name).Inc()
	}
	_, loggers := f.ctx.getKafkaLoggers()
	if len(loggers) > 0 {
		md := ReadDeadLetterMetadata(record)
		message := fmt.Sprintf(
			"DLQ parked topic=%s offset=%d attempts=%d source=%s:%d:%d first_failed=%s err=%s",
			record.Topic, record.Offset, md.Attempts,
			md.SourceTopic, md.SourcePartition, md.SourceOffset,
			md.FirstFailedAt.Format(time.RFC3339Nano), cause.Error(),
		)
		fillLogFields(f.ctx, loggers, f.cg.poolCode+"/"+f.cg.settings.Name, sourceKafka, "DLQ_PARK", message, nil, false, nil)
	}
}

// Pool-level interface
type Kafka interface {
	GetCode() string
	GetBrokers() []string
	GetPoolOptions() *KafkaPoolOptions
	ProduceSync(ctx Context, records ...*KafkaRecord) error
	Produce(ctx Context, record *KafkaRecord, callback func(*KafkaRecord, error))
	ConsumerGroup(name string) (KafkaConsumerGroup, error)
	MustConsumerGroup(name string) KafkaConsumerGroup
	ConsumerGroupNames() []string
	Close()
}

// Consumer group interface
type KafkaConsumerGroup interface {
	GetName() string
	GetSettings() *KafkaConsumerGroupSettings
	GetKgoClient() *kgo.Client
	PollFetches(ctx Context) KafkaFetches
	CommitUncommittedOffsets(ctx Context) error
	Close()
}

// Internal config structs

type kafkaPoolConfig struct {
	code           string
	brokers        []string
	options        *KafkaPoolOptions
	consumerGroups map[string]*KafkaConsumerGroupSettings
}

// Internal implementation structs

type kafkaPoolImplementation struct {
	config              *kafkaPoolConfig
	producerClient      *kgo.Client
	producerCancel      context.CancelFunc
	hasRegisteredTopics bool
}

type kafkaConsumerGroupImplementation struct {
	poolCode  string
	settings  *KafkaConsumerGroupSettings
	client    *kgo.Client
	ctxCancel context.CancelFunc
	pool      *kafkaPoolImplementation
}

// kafkaPoolImplementation implements Kafka

func (k *kafkaPoolImplementation) GetCode() string {
	return k.config.code
}

func (k *kafkaPoolImplementation) GetBrokers() []string {
	return k.config.brokers
}

func (k *kafkaPoolImplementation) GetPoolOptions() *KafkaPoolOptions {
	return k.config.options
}

func (k *kafkaPoolImplementation) ConsumerGroup(name string) (KafkaConsumerGroup, error) {
	settings, ok := k.config.consumerGroups[name]
	if !ok {
		return nil, fmt.Errorf("kafka pool '%s': consumer group '%s' not registered", k.config.code, name)
	}
	ctx, cancel := context.WithCancel(context.Background())
	opts := buildConsumerKgoOpts(k.config, settings, k.hasRegisteredTopics)
	opts = append(opts, kgo.WithContext(ctx))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("kafka pool '%s' consumer group '%s': %w", k.config.code, name, err)
	}
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		cancel()
		return nil, fmt.Errorf("kafka pool '%s' consumer group '%s': failed to connect: %w", k.config.code, name, err)
	}
	return &kafkaConsumerGroupImplementation{
		poolCode:  k.config.code,
		settings:  settings,
		client:    client,
		ctxCancel: cancel,
		pool:      k,
	}, nil
}

func (k *kafkaPoolImplementation) MustConsumerGroup(name string) KafkaConsumerGroup {
	cg, err := k.ConsumerGroup(name)
	if err != nil {
		panic(err)
	}
	return cg
}

func (k *kafkaPoolImplementation) ConsumerGroupNames() []string {
	names := make([]string, 0, len(k.config.consumerGroups))
	for name := range k.config.consumerGroups {
		names = append(names, name)
	}
	return names
}

func (k *kafkaPoolImplementation) ProduceSync(ctx Context, records ...*KafkaRecord) error {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	kgoRecords := make([]*kgo.Record, len(records))
	for i, r := range records {
		kgoRecords[i] = toKgoRecord(r)
	}
	results := k.producerClient.ProduceSync(ctx.Context(), kgoRecords...)
	var err error
	for _, r := range results {
		if r.Err != nil {
			err = r.Err
			break
		}
	}
	duration := time.Since(start)
	if hasLogger {
		message := fmt.Sprintf("topics: %d records", len(records))
		if len(records) == 1 {
			message = fmt.Sprintf("topic: %s", records[0].Topic)
		}
		k.fillLogFields(ctx, "PRODUCE", message, duration, err)
	}
	k.fillMetrics(ctx, duration, "produce", err)
	return err
}

func (k *kafkaPoolImplementation) Produce(ctx Context, record *KafkaRecord, callback func(*KafkaRecord, error)) {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	k.producerClient.Produce(ctx.Context(), toKgoRecord(record), func(r *kgo.Record, err error) {
		duration := time.Since(start)
		if hasLogger {
			message := fmt.Sprintf("topic: %s", record.Topic)
			k.fillLogFields(ctx, "PRODUCE_ASYNC", message, duration, err)
		}
		k.fillMetrics(ctx, duration, "produce_async", err)
		if callback != nil {
			callback(fromKgoRecord(r), err)
		}
	})
}

func (k *kafkaPoolImplementation) fillMetrics(ctx Context, duration time.Duration, operation string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesKafka.WithLabelValues(operation, k.config.code, ctx.getMetricsSourceTag(), "").Observe(duration.Seconds())
		if err != nil {
			metrics.queriesKafkaErrors.WithLabelValues(k.config.code, ctx.getMetricsSourceTag(), "").Inc()
		}
	}
}

func (k *kafkaPoolImplementation) fillLogFields(ctx Context, operation, message string, duration time.Duration, err error) {
	_, loggers := ctx.getKafkaLoggers()
	fillLogFields(ctx, loggers, k.config.code, sourceKafka, operation, message, &duration, false, err)
}

func (k *kafkaPoolImplementation) Close() {
	k.producerCancel()
	k.producerClient.Close()
}

// kafkaConsumerGroupImplementation implements KafkaConsumerGroup

func (k *kafkaConsumerGroupImplementation) GetName() string {
	return k.settings.Name
}

func (k *kafkaConsumerGroupImplementation) GetSettings() *KafkaConsumerGroupSettings {
	return k.settings
}

func (k *kafkaConsumerGroupImplementation) GetKgoClient() *kgo.Client {
	return k.client
}

func (k *kafkaConsumerGroupImplementation) PollFetches(ctx Context) KafkaFetches {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	fetches := k.client.PollFetches(ctx.Context())
	duration := time.Since(start)
	var firstErr error
	fetches.EachError(func(_ string, _ int32, err error) {
		if firstErr == nil {
			firstErr = err
		}
	})
	if hasLogger {
		var totalRecords int
		fetches.EachRecord(func(_ *kgo.Record) {
			totalRecords++
		})
		message := fmt.Sprintf("%d records fetched", totalRecords)
		k.fillLogFields(ctx, "POLL", message, duration, firstErr)
	}
	k.fillMetrics(ctx, duration, "poll", firstErr)
	return KafkaFetches{fetches: fetches, cg: k, ctx: ctx}
}

func (k *kafkaConsumerGroupImplementation) CommitUncommittedOffsets(ctx Context) error {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	err := k.client.CommitUncommittedOffsets(ctx.Context())
	duration := time.Since(start)
	if hasLogger {
		k.fillLogFields(ctx, "COMMIT", "commit uncommitted offsets", duration, err)
	}
	k.fillMetrics(ctx, duration, "commit", err)
	return err
}

func (k *kafkaConsumerGroupImplementation) Close() {
	k.ctxCancel()
	k.client.Close()
}

func (k *kafkaConsumerGroupImplementation) fillMetrics(ctx Context, duration time.Duration, operation string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesKafka.WithLabelValues(operation, k.poolCode, ctx.getMetricsSourceTag(), k.settings.Name).Observe(duration.Seconds())
		if err != nil {
			metrics.queriesKafkaErrors.WithLabelValues(k.poolCode, ctx.getMetricsSourceTag(), k.settings.Name).Inc()
		}
	}
}

func (k *kafkaConsumerGroupImplementation) fillLogFields(ctx Context, operation, message string, duration time.Duration, err error) {
	_, loggers := ctx.getKafkaLoggers()
	poolLabel := k.poolCode + "/" + k.settings.Name
	fillLogFields(ctx, loggers, poolLabel, sourceKafka, operation, message, &duration, false, err)
}

func toKgoRecord(r *KafkaRecord) *kgo.Record {
	rec := &kgo.Record{
		Topic: r.Topic,
		Key:   r.Key,
		Value: r.Value,
	}
	if len(r.Headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, len(r.Headers))
		for i, h := range r.Headers {
			rec.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}
	return rec
}

func fromKgoRecord(r *kgo.Record) *KafkaRecord {
	rec := &KafkaRecord{
		Topic:     r.Topic,
		Key:       r.Key,
		Value:     r.Value,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
	}
	if len(r.Headers) > 0 {
		rec.Headers = make([]KafkaRecordHeader, len(r.Headers))
		for i, h := range r.Headers {
			rec.Headers[i] = KafkaRecordHeader{Key: h.Key, Value: h.Value}
		}
	}
	return rec
}
