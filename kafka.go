package fluxaorm

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConfig interface {
	GetCode() string
	GetBrokers() []string
	GetOptions() *KafkaOptions
	getClient() *kgo.Client
}

type kafkaConfig struct {
	code    string
	brokers []string
	client  *kgo.Client
	options *KafkaOptions
}

func (p *kafkaConfig) GetCode() string {
	return p.code
}

func (p *kafkaConfig) GetBrokers() []string {
	return p.brokers
}

func (p *kafkaConfig) getClient() *kgo.Client {
	return p.client
}

func (p *kafkaConfig) GetOptions() *KafkaOptions {
	return p.options
}

type KafkaOptions struct {
	ClientID           string
	ConsumerGroup      string
	ConsumeTopics      []string
	RequiredAcks       int // 0=none, 1=leader, -1=all (default: -1)
	ProducerLinger     time.Duration
	MaxBufferedRecords int
	SessionTimeout     time.Duration
	RebalanceTimeout   time.Duration
	FetchMaxBytes      int32
	AutoCommitInterval time.Duration // 0 = manual commit only
	SASL               *KafkaSASLConfig
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

type Kafka interface {
	GetConfig() KafkaConfig
	GetKgoClient() *kgo.Client
	ProduceSync(ctx Context, records ...*KafkaRecord) error
	Produce(ctx Context, record *KafkaRecord, callback func(*KafkaRecord, error))
	PollFetches(ctx Context) KafkaFetches
	CommitUncommittedOffsets(ctx Context) error
	Close()
}

type kafkaImplementation struct {
	config KafkaConfig
}

func (k *kafkaImplementation) GetConfig() KafkaConfig {
	return k.config
}

func (k *kafkaImplementation) GetKgoClient() *kgo.Client {
	return k.config.getClient()
}

func (k *kafkaImplementation) ProduceSync(ctx Context, records ...*KafkaRecord) error {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	kgoRecords := make([]*kgo.Record, len(records))
	for i, r := range records {
		kgoRecords[i] = toKgoRecord(r)
	}
	results := k.config.getClient().ProduceSync(ctx.Context(), kgoRecords...)
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

func (k *kafkaImplementation) Produce(ctx Context, record *KafkaRecord, callback func(*KafkaRecord, error)) {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	k.config.getClient().Produce(ctx.Context(), toKgoRecord(record), func(r *kgo.Record, err error) {
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

func (k *kafkaImplementation) PollFetches(ctx Context) KafkaFetches {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	fetches := k.config.getClient().PollFetches(ctx.Context())
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
	return KafkaFetches{fetches: fetches}
}

func (k *kafkaImplementation) CommitUncommittedOffsets(ctx Context) error {
	hasLogger, _ := ctx.getKafkaLoggers()
	start := time.Now()
	err := k.config.getClient().CommitUncommittedOffsets(ctx.Context())
	duration := time.Since(start)
	if hasLogger {
		k.fillLogFields(ctx, "COMMIT", "commit uncommitted offsets", duration, err)
	}
	k.fillMetrics(ctx, duration, "commit", err)
	return err
}

func (k *kafkaImplementation) Close() {
	k.config.getClient().Close()
}

func (k *kafkaImplementation) fillMetrics(ctx Context, duration time.Duration, operation string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesKafka.WithLabelValues(operation, k.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Observe(duration.Seconds())
		if err != nil {
			metrics.queriesKafkaErrors.WithLabelValues(k.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
	}
}

func (k *kafkaImplementation) fillLogFields(ctx Context, operation, message string, duration time.Duration, err error) {
	_, loggers := ctx.getKafkaLoggers()
	fillLogFields(ctx, loggers, k.GetConfig().GetCode(), sourceKafka, operation, message, &duration, false, err)
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
