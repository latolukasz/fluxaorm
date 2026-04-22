package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeadLetterTopicName(t *testing.T) {
	assert.Equal(t, "_dlq_product-indexer", DeadLetterTopicName("product-indexer"))
	assert.Equal(t, "_dlq_payment-processor", DeadLetterConsumerGroupName("payment-processor"))
}

func TestWithDeadLetterDefaults(t *testing.T) {
	cg := NewKafkaConsumerGroup("my-group", "default").Topics("t1").WithDeadLetter()
	assert.True(t, cg.dlqEnabled)
	assert.Equal(t, DefaultDeadLetterMaxAttempts, cg.dlqMaxAttempts)
	assert.Equal(t, int32(1), cg.dlqTopicPartitions)

	settings := cg.toSettings()
	assert.True(t, settings.DeadLetterEnabled)
	assert.Equal(t, DefaultDeadLetterMaxAttempts, settings.DeadLetterMaxAttempts)
	assert.Equal(t, int32(1), settings.DeadLetterTopicPartitions)
	assert.Equal(t, "", settings.DeadLetterParentGroup)
}

func TestWithDeadLetterOptions(t *testing.T) {
	cg := NewKafkaConsumerGroup("my-group", "default").Topics("t1").WithDeadLetter(&DeadLetterOptions{
		MaxAttempts:     5,
		TopicPartitions: 3,
	})
	assert.Equal(t, 5, cg.dlqMaxAttempts)
	assert.Equal(t, int32(3), cg.dlqTopicPartitions)

	// Passing nil options keeps defaults.
	cg2 := NewKafkaConsumerGroup("g2", "default").Topics("t1").WithDeadLetter(nil)
	assert.True(t, cg2.dlqEnabled)
	assert.Equal(t, DefaultDeadLetterMaxAttempts, cg2.dlqMaxAttempts)
	assert.Equal(t, int32(1), cg2.dlqTopicPartitions)
}

func TestReadDeadLetterMetadata(t *testing.T) {
	firstFailed := time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC)
	lastFailed := firstFailed.Add(5 * time.Minute)

	record := &KafkaRecord{
		Headers: []KafkaRecordHeader{
			{Key: HeaderDLQError, Value: []byte("boom")},
			{Key: HeaderDLQAttempts, Value: []byte("3")},
			{Key: HeaderDLQSourceTopic, Value: []byte("fluxa_default.orders")},
			{Key: HeaderDLQSourcePartition, Value: []byte("7")},
			{Key: HeaderDLQSourceOffset, Value: []byte("42")},
			{Key: HeaderDLQFirstFailedAt, Value: []byte(firstFailed.Format(time.RFC3339Nano))},
			{Key: HeaderDLQLastFailedAt, Value: []byte(lastFailed.Format(time.RFC3339Nano))},
		},
	}
	md := ReadDeadLetterMetadata(record)
	assert.Equal(t, "boom", md.Error)
	assert.Equal(t, 3, md.Attempts)
	assert.Equal(t, "fluxa_default.orders", md.SourceTopic)
	assert.Equal(t, int32(7), md.SourcePartition)
	assert.Equal(t, int64(42), md.SourceOffset)
	assert.True(t, md.FirstFailedAt.Equal(firstFailed))
	assert.True(t, md.LastFailedAt.Equal(lastFailed))
}

func TestReadDeadLetterMetadataEmpty(t *testing.T) {
	record := &KafkaRecord{Headers: []KafkaRecordHeader{{Key: "unrelated", Value: []byte("x")}}}
	md := ReadDeadLetterMetadata(record)
	assert.Equal(t, "", md.Error)
	assert.Equal(t, 0, md.Attempts)
	assert.Equal(t, "", md.SourceTopic)
	assert.True(t, md.FirstFailedAt.IsZero())
}

func TestBuildInitialDeadLetterRecord(t *testing.T) {
	source := &KafkaRecord{
		Topic:     "fluxa_default.orders",
		Partition: 2,
		Offset:    99,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers: []KafkaRecordHeader{
			{Key: "origin", Value: []byte("debezium")},
		},
	}
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)

	dlq := buildInitialDeadLetterRecord("_dlq_indexer", source, "boom", now)
	assert.Equal(t, "_dlq_indexer", dlq.Topic)
	assert.Equal(t, []byte("k"), dlq.Key)
	assert.Equal(t, []byte("v"), dlq.Value)

	md := ReadDeadLetterMetadata(dlq)
	assert.Equal(t, "boom", md.Error)
	assert.Equal(t, 1, md.Attempts)
	assert.Equal(t, "fluxa_default.orders", md.SourceTopic)
	assert.Equal(t, int32(2), md.SourcePartition)
	assert.Equal(t, int64(99), md.SourceOffset)
	assert.True(t, md.FirstFailedAt.Equal(now))
	assert.True(t, md.LastFailedAt.Equal(now))

	// Non-DLQ headers preserved.
	var foundOrigin bool
	for _, h := range dlq.Headers {
		if h.Key == "origin" {
			foundOrigin = true
			assert.Equal(t, []byte("debezium"), h.Value)
		}
	}
	assert.True(t, foundOrigin)
}

func TestBuildRequeueDeadLetterRecordPreservesFirstFailure(t *testing.T) {
	firstFailed := time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC)
	source := &KafkaRecord{
		Topic: "_dlq_indexer",
		Key:   []byte("k"),
		Value: []byte("v"),
		Headers: []KafkaRecordHeader{
			{Key: HeaderDLQError, Value: []byte("first boom")},
			{Key: HeaderDLQAttempts, Value: []byte("2")},
			{Key: HeaderDLQSourceTopic, Value: []byte("fluxa_default.orders")},
			{Key: HeaderDLQSourcePartition, Value: []byte("2")},
			{Key: HeaderDLQSourceOffset, Value: []byte("99")},
			{Key: HeaderDLQFirstFailedAt, Value: []byte(firstFailed.Format(time.RFC3339Nano))},
			{Key: HeaderDLQLastFailedAt, Value: []byte(firstFailed.Format(time.RFC3339Nano))},
		},
	}
	now := firstFailed.Add(1 * time.Hour)

	requeued := buildRequeueDeadLetterRecord("_dlq_indexer", source, "still broken", 3, now)
	md := ReadDeadLetterMetadata(requeued)
	assert.Equal(t, "still broken", md.Error)
	assert.Equal(t, 3, md.Attempts)
	assert.Equal(t, "fluxa_default.orders", md.SourceTopic)
	assert.Equal(t, int32(2), md.SourcePartition)
	assert.Equal(t, int64(99), md.SourceOffset)
	assert.True(t, md.FirstFailedAt.Equal(firstFailed), "first-failed-at should be preserved across requeues")
	assert.True(t, md.LastFailedAt.Equal(now))
}

func TestAutoRegisterDeadLetterQueues(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterKafka([]string{"localhost:9092"}, "default", nil)
	r.RegisterKafkaConsumerGroup(
		NewKafkaConsumerGroup("product-indexer", "default").Topics("products").
			WithDeadLetter(&DeadLetterOptions{MaxAttempts: 5, TopicPartitions: 3}),
	)
	r.RegisterKafkaConsumerGroup(
		NewKafkaConsumerGroup("regular-group", "default").Topics("regular"),
	)
	// Pre-existing explicit DLQ topic — auto-reg should leave it alone.
	r.RegisterKafkaTopic(NewKafkaTopic(DeadLetterTopicName("product-indexer"), "default").Partitions(9))

	r.autoRegisterDeadLetterQueues()

	// Sibling CG registered.
	var sibling *KafkaConsumerGroupBuilder
	for _, cg := range r.kafkaConsumerGroups {
		if cg.name == DeadLetterConsumerGroupName("product-indexer") {
			sibling = cg
			break
		}
	}
	assert.NotNil(t, sibling, "sibling DLQ CG should be auto-registered")
	assert.Equal(t, "product-indexer", sibling.dlqParentGroup)
	assert.Equal(t, 5, sibling.dlqMaxAttempts)
	assert.Equal(t, []string{DeadLetterTopicName("product-indexer")}, sibling.topics)
	assert.False(t, sibling.dlqEnabled, "sibling CG should not have dlqEnabled")

	// Regular group gets NO DLQ sibling.
	for _, cg := range r.kafkaConsumerGroups {
		assert.NotEqual(t, DeadLetterConsumerGroupName("regular-group"), cg.name)
	}

	// Pre-existing DLQ topic kept at 9 partitions (not overwritten).
	var dlqTopic *KafkaTopicBuilder
	for _, topic := range r.kafkaTopics {
		if topic.topicName == DeadLetterTopicName("product-indexer") {
			dlqTopic = topic
			break
		}
	}
	assert.NotNil(t, dlqTopic)
	assert.Equal(t, int32(9), dlqTopic.numPartitions)

	// Idempotency: second call adds nothing.
	lenTopicsBefore := len(r.kafkaTopics)
	lenCGsBefore := len(r.kafkaConsumerGroups)
	r.autoRegisterDeadLetterQueues()
	assert.Equal(t, lenTopicsBefore, len(r.kafkaTopics))
	assert.Equal(t, lenCGsBefore, len(r.kafkaConsumerGroups))
}

func TestAutoRegisterDeadLetterQueuesCreatesDefaults(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterKafka([]string{"localhost:9092"}, "default", nil)
	r.RegisterKafkaConsumerGroup(
		NewKafkaConsumerGroup("payment-processor", "default").Topics("payments").WithDeadLetter(),
	)

	r.autoRegisterDeadLetterQueues()

	// Topic auto-created with default 1 partition.
	var dlqTopic *KafkaTopicBuilder
	for _, topic := range r.kafkaTopics {
		if topic.topicName == DeadLetterTopicName("payment-processor") {
			dlqTopic = topic
			break
		}
	}
	assert.NotNil(t, dlqTopic)
	assert.Equal(t, int32(1), dlqTopic.numPartitions)

	// Sibling CG carries default MaxAttempts.
	var sibling *KafkaConsumerGroupBuilder
	for _, cg := range r.kafkaConsumerGroups {
		if cg.name == DeadLetterConsumerGroupName("payment-processor") {
			sibling = cg
			break
		}
	}
	assert.NotNil(t, sibling)
	assert.Equal(t, DefaultDeadLetterMaxAttempts, sibling.dlqMaxAttempts)
	settings := sibling.toSettings()
	assert.Equal(t, "payment-processor", settings.DeadLetterParentGroup)
	assert.Equal(t, DefaultDeadLetterMaxAttempts, settings.DeadLetterMaxAttempts)
}

func TestCopyNonDLQHeaders(t *testing.T) {
	in := []KafkaRecordHeader{
		{Key: HeaderDLQError, Value: []byte("x")},
		{Key: HeaderDLQAttempts, Value: []byte("1")},
		{Key: "trace-id", Value: []byte("abc")},
		{Key: HeaderDLQSourceOffset, Value: []byte("1")},
		{Key: "origin", Value: []byte("debezium")},
	}
	out := copyNonDLQHeaders(in)
	assert.Len(t, out, 2)
	keys := []string{out[0].Key, out[1].Key}
	assert.Contains(t, keys, "trace-id")
	assert.Contains(t, keys, "origin")
}
