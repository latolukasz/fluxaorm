package fluxaorm

import (
	"context"
	"fmt"
	"sort"
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
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	assert.NotNil(t, k)
	assert.Equal(t, DefaultPoolCode, k.GetCode())
	assert.Equal(t, []string{"localhost:9944"}, k.GetBrokers())

	pools := engine.Registry().KafkaPools()
	assert.Len(t, pools, 1)
	assert.NotNil(t, pools[DefaultPoolCode])
}

func TestKafkaProduceSync(t *testing.T) {
	topic := fmt.Sprintf("test_"+t.Name()+"_%d", time.Now().UnixNano())
	groupName := topic + "_group"
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil,
		KafkaConsumerGroupSettings{Name: groupName, Topics: []string{topic}},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	pool := engine.Kafka(DefaultPoolCode)

	// Produce first — ensures topic exists with partitions before consumer starts
	err = pool.ProduceSync(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("key1"),
		Value: []byte("value1"),
		Headers: []KafkaRecordHeader{
			{Key: "h1", Value: []byte("v1")},
		},
	})
	assert.NoError(t, err)

	// Create consumer after topic exists — metadata will find partitions immediately
	cg := pool.MustConsumerGroup(groupName)
	defer cg.Close()

	records := pollUntilRecords(t, cg, engine, 60*time.Second)
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

	err = cg.CommitUncommittedOffsets(ctx)
	assert.NoError(t, err)
}

func TestKafkaProduceAsync(t *testing.T) {
	topic := "test_" + t.Name()
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil,
		KafkaConsumerGroupSettings{Name: "test_group_" + t.Name(), Topics: []string{topic}},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	pool := engine.Kafka(DefaultPoolCode)

	done := make(chan struct{})
	pool.Produce(ctx, &KafkaRecord{
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
	topic := fmt.Sprintf("test_"+t.Name()+"_%d", time.Now().UnixNano())
	group := topic + "_group"

	// Producer pool
	registryProducer := NewRegistry()
	registryProducer.RegisterKafka([]string{"localhost:9944"}, "producer", nil)
	registryProducer.RegisterRedis("localhost:6395", 15, "redis", nil)
	engineProducer, err := registryProducer.Validate()
	assert.NoError(t, err)
	defer engineProducer.Kafka("producer").Close()

	// Produce first — ensures topic exists with partitions
	ctx := engineProducer.NewContext(context.Background())
	err = engineProducer.Kafka("producer").ProduceSync(ctx, &KafkaRecord{
		Topic: topic,
		Key:   []byte("cg_key"),
		Value: []byte("cg_value"),
	})
	assert.NoError(t, err)

	// Consumer pool — topic already exists
	registryConsumer := NewRegistry()
	registryConsumer.RegisterKafka([]string{"localhost:9944"}, "consumer", nil,
		KafkaConsumerGroupSettings{Name: group, Topics: []string{topic}},
	)
	registryConsumer.RegisterRedis("localhost:6395", 15, "redis", nil)
	engineConsumer, err := registryConsumer.Validate()
	assert.NoError(t, err)
	defer engineConsumer.Kafka("consumer").Close()

	consumerCG := engineConsumer.Kafka("consumer").MustConsumerGroup(group)
	defer consumerCG.Close()

	records := pollUntilRecords(t, consumerCG, engineConsumer, 60*time.Second)
	found := false
	for _, r := range records {
		if string(r.Key) == "cg_key" {
			assert.Equal(t, []byte("cg_value"), r.Value)
			found = true
		}
	}
	assert.True(t, found)

	ctxConsumer := engineConsumer.NewContext(context.Background())
	err = consumerCG.CommitUncommittedOffsets(ctxConsumer)
	assert.NoError(t, err)
}

func TestKafkaPoolOptions(t *testing.T) {
	registry := NewRegistry()
	opts := &KafkaPoolOptions{
		ClientID:           "test-client",
		RequiredAcks:       -1,
		ProducerLinger:     10 * time.Millisecond,
		MaxBufferedRecords: 1000,
	}
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, opts,
		KafkaConsumerGroupSettings{
			Name:               "test-group",
			Topics:             []string{"topic1", "topic2"},
			SessionTimeout:     30 * time.Second,
			RebalanceTimeout:   60 * time.Second,
			FetchMaxBytes:      1048576,
			AutoCommitInterval: 5 * time.Second,
		},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	assert.Equal(t, "test-client", k.GetPoolOptions().ClientID)
	assert.Equal(t, -1, k.GetPoolOptions().RequiredAcks)
	assert.Equal(t, 10*time.Millisecond, k.GetPoolOptions().ProducerLinger)
	assert.Equal(t, 1000, k.GetPoolOptions().MaxBufferedRecords)

	cg := k.MustConsumerGroup("test-group")
	defer cg.Close()
	assert.Equal(t, "test-group", cg.GetName())
	assert.Equal(t, []string{"topic1", "topic2"}, cg.GetSettings().Topics)
	assert.Equal(t, 30*time.Second, cg.GetSettings().SessionTimeout)
	assert.Equal(t, 60*time.Second, cg.GetSettings().RebalanceTimeout)
	assert.Equal(t, int32(1048576), cg.GetSettings().FetchMaxBytes)
	assert.Equal(t, 5*time.Second, cg.GetSettings().AutoCommitInterval)
}

func TestKafkaLogging(t *testing.T) {
	topic := "test_" + t.Name()
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil,
		KafkaConsumerGroupSettings{Name: "log_group", Topics: []string{topic}},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	ctx := engine.NewContext(context.Background())
	testLogger := &MockLogHandler{}
	ctx.RegisterQueryLogger(testLogger, QueryLoggerOptions{Kafka: true})

	pool := engine.Kafka(DefaultPoolCode)
	err = pool.ProduceSync(ctx, &KafkaRecord{
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
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil,
		KafkaConsumerGroupSettings{Name: "close_group", Topics: []string{"test_topic"}},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	k := engine.Kafka(DefaultPoolCode)
	cg := k.MustConsumerGroup("close_group")
	assert.NotNil(t, cg.GetKgoClient())
	cg.Close()
	k.Close()
}

func TestKafkaConsumerGroupNotFound(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	cg, err := k.ConsumerGroup("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, cg)
}

func TestKafkaMustConsumerGroupPanics(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	assert.Panics(t, func() {
		k.MustConsumerGroup("nonexistent")
	})
}

func TestKafkaConsumerGroupNames(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil,
		KafkaConsumerGroupSettings{Name: "group1", Topics: []string{"topic1"}},
		KafkaConsumerGroupSettings{Name: "group2", Topics: []string{"topic2"}},
	)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	names := k.ConsumerGroupNames()
	sort.Strings(names)
	assert.Equal(t, []string{"group1", "group2"}, names)
}

func TestKafkaPoolWithoutConsumerGroups(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	defer engine.Kafka(DefaultPoolCode).Close()

	k := engine.Kafka(DefaultPoolCode)
	assert.NotNil(t, k)
	names := k.ConsumerGroupNames()
	assert.Len(t, names, 0)
}

func pollUntilRecords(t *testing.T, cg KafkaConsumerGroup, engine Engine, deadline time.Duration) []*KafkaRecord {
	t.Helper()
	timeoutAt := time.After(deadline)
	for {
		pollCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		fetches := cg.PollFetches(engine.NewContext(pollCtx))
		cancel()
		records := fetches.Records()
		if len(records) > 0 {
			return records
		}
		select {
		case <-timeoutAt:
			t.Fatal("timeout waiting for records from consumer group")
			return nil
		default:
		}
	}
}
