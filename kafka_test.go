package fluxaorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKafka(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	k := engine.Kafka(DefaultPoolCode)
	assert.NotNil(t, k)

	config := k.GetConfig()
	assert.Equal(t, DefaultPoolCode, config.GetCode())
	assert.Equal(t, []string{"localhost:9944"}, config.GetBrokers())

	pools := engine.Registry().KafkaPools()
	assert.Len(t, pools, 1)
	assert.NotNil(t, pools[DefaultPoolCode])
}

func TestKafkaProduceSync(t *testing.T) {
	topic := "test_" + t.Name()
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, &KafkaOptions{
		ConsumerGroup: "test_group_" + t.Name(),
		ConsumeTopics: []string{topic},
	})
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	k := engine.Kafka(DefaultPoolCode)

	err = k.ProduceSync(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Headers: []KafkaRecordHeader{
			{Key: "h1", Value: []byte("v1")},
		},
	})
	assert.NoError(t, err)

	fetches := k.PollFetches(ctx)
	assert.False(t, fetches.IsEmpty())
	records := fetches.Records()
	assert.GreaterOrEqual(t, len(records), 1)
	found := false
	for _, r := range records {
		if string(r.Key) == "key1" {
			assert.Equal(t, []byte("value1"), r.Value)
			assert.Equal(t, topic, r.Topic)
			assert.Len(t, r.Headers, 1)
			assert.Equal(t, "h1", r.Headers[0].Key)
			assert.Equal(t, []byte("v1"), r.Headers[0].Value)
			found = true
		}
	}
	assert.True(t, found)

	err = k.CommitUncommittedOffsets(ctx)
	assert.NoError(t, err)
}

func TestKafkaProduceAsync(t *testing.T) {
	topic := "test_" + t.Name()
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	k := engine.Kafka(DefaultPoolCode)

	done := make(chan struct{})
	k.Produce(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("async_key"),
		Value: []byte("async_value"),
	}, func(r *KafkaRecord, err error) {
		assert.NoError(t, err)
		assert.Equal(t, topic, r.Topic)
		close(done)
	})

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("async produce callback not called within timeout")
	}
}

func TestKafkaConsumerGroup(t *testing.T) {
	topic := "test_" + t.Name()
	group := "test_group_" + t.Name()

	// Producer client
	registryProducer := NewRegistry()
	registryProducer.RegisterKafka([]string{"localhost:9944"}, "producer", nil)
	registryProducer.RegisterRedis("localhost:6395", 15, "redis", nil)
	engineProducer, err := registryProducer.Validate()
	assert.NoError(t, err)
	defer engineProducer.Kafka("producer").Close()

	// Consumer client
	registryConsumer := NewRegistry()
	registryConsumer.RegisterKafka([]string{"localhost:9944"}, "consumer", &KafkaOptions{
		ConsumerGroup: group,
		ConsumeTopics: []string{topic},
	})
	registryConsumer.RegisterRedis("localhost:6395", 15, "redis", nil)
	engineConsumer, err := registryConsumer.Validate()
	assert.NoError(t, err)
	defer engineConsumer.Kafka("consumer").Close()

	ctx := engineProducer.NewContext(context.Background())
	err = engineProducer.Kafka("producer").ProduceSync(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("cg_key"),
		Value: []byte("cg_value"),
	})
	assert.NoError(t, err)

	ctxConsumer := engineConsumer.NewContext(context.Background())
	fetches := engineConsumer.Kafka("consumer").PollFetches(ctxConsumer)
	assert.False(t, fetches.IsEmpty())

	records := fetches.Records()
	found := false
	for _, r := range records {
		if string(r.Key) == "cg_key" {
			assert.Equal(t, []byte("cg_value"), r.Value)
			found = true
		}
	}
	assert.True(t, found)

	err = engineConsumer.Kafka("consumer").CommitUncommittedOffsets(ctxConsumer)
	assert.NoError(t, err)
}

func TestKafkaOptions(t *testing.T) {
	registry := NewRegistry()
	opts := &KafkaOptions{
		ClientID:           "test-client",
		ConsumerGroup:      "test-group",
		ConsumeTopics:      []string{"topic1", "topic2"},
		RequiredAcks:       -1,
		ProducerLinger:     10 * time.Millisecond,
		MaxBufferedRecords: 1000,
		SessionTimeout:     30 * time.Second,
		RebalanceTimeout:   60 * time.Second,
		FetchMaxBytes:      1048576,
		AutoCommitInterval: 5 * time.Second,
	}
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, opts)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	config := k.GetConfig()
	assert.Equal(t, "test-client", config.GetOptions().ClientID)
	assert.Equal(t, "test-group", config.GetOptions().ConsumerGroup)
	assert.Equal(t, []string{"topic1", "topic2"}, config.GetOptions().ConsumeTopics)
	assert.Equal(t, -1, config.GetOptions().RequiredAcks)
	assert.Equal(t, 10*time.Millisecond, config.GetOptions().ProducerLinger)
	assert.Equal(t, 1000, config.GetOptions().MaxBufferedRecords)
	assert.Equal(t, 30*time.Second, config.GetOptions().SessionTimeout)
	assert.Equal(t, 60*time.Second, config.GetOptions().RebalanceTimeout)
	assert.Equal(t, int32(1048576), config.GetOptions().FetchMaxBytes)
	assert.Equal(t, 5*time.Second, config.GetOptions().AutoCommitInterval)
}

func TestKafkaLogging(t *testing.T) {
	topic := "test_" + t.Name()
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	testLogger := &MockLogHandler{}
	ctx.RegisterQueryLogger(testLogger, QueryLoggerOptions{Kafka: true})

	err = engine.Kafka(DefaultPoolCode).ProduceSync(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("log_key"),
		Value: []byte("log_value"),
	})
	assert.NoError(t, err)
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "kafka", testLogger.Logs[0]["source"])
	assert.Equal(t, DefaultPoolCode, testLogger.Logs[0]["pool"])
	assert.Equal(t, "PRODUCE", testLogger.Logs[0]["operation"])
}

func TestKafkaClose(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	k := engine.Kafka(DefaultPoolCode)
	assert.NotNil(t, k.GetKgoClient())
	k.Close()
}
