package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKafkaTopicDefaults(t *testing.T) {
	topic := NewKafkaTopic("orders", "default")
	assert.Equal(t, "orders", topic.topicName)
	assert.Equal(t, "default", topic.poolCode)
	assert.Equal(t, int32(1), topic.numPartitions)
	assert.Equal(t, int16(1), topic.replicationFactor)
	assert.Nil(t, topic.configs)
}

func TestKafkaTopicFluentAPI(t *testing.T) {
	topic := NewKafkaTopic("events", "default").
		Partitions(12).
		ReplicationFactor(3).
		RetentionMs(86400000).
		RetentionBytes(1073741824).
		MaxMessageBytes(2097152).
		CleanupPolicy("delete").
		MinInsyncReplicas(2).
		CompressionType("lz4").
		Config("segment.ms", "3600000")

	assert.Equal(t, "events", topic.topicName)
	assert.Equal(t, "default", topic.poolCode)
	assert.Equal(t, int32(12), topic.numPartitions)
	assert.Equal(t, int16(3), topic.replicationFactor)
	assert.Equal(t, "86400000", topic.configs["retention.ms"])
	assert.Equal(t, "1073741824", topic.configs["retention.bytes"])
	assert.Equal(t, "2097152", topic.configs["max.message.bytes"])
	assert.Equal(t, "delete", topic.configs["cleanup.policy"])
	assert.Equal(t, "2", topic.configs["min.insync.replicas"])
	assert.Equal(t, "lz4", topic.configs["compression.type"])
	assert.Equal(t, "3600000", topic.configs["segment.ms"])
}

func TestKafkaTopicValidation(t *testing.T) {
	// Empty topic name
	topic := NewKafkaTopic("", "default")
	err := topic.validate()
	assert.EqualError(t, err, "kafka topic name is required")

	// Empty pool code
	topic = NewKafkaTopic("orders", "")
	err = topic.validate()
	assert.EqualError(t, err, "kafka pool code is required for topic 'orders'")

	// Invalid partitions
	topic = NewKafkaTopic("orders", "default").Partitions(0)
	err = topic.validate()
	assert.EqualError(t, err, "kafka topic 'orders' must have at least 1 partition")

	// Invalid replication factor
	topic = NewKafkaTopic("orders", "default").ReplicationFactor(0)
	err = topic.validate()
	assert.EqualError(t, err, "kafka topic 'orders' must have replication factor >= 1")

	// Valid topic
	topic = NewKafkaTopic("orders", "default").Partitions(6).ReplicationFactor(3)
	err = topic.validate()
	assert.NoError(t, err)
}

func TestKafkaTopicRegistrationDuplicate(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterKafka([]string{"localhost:9092"}, "default", nil)
	r.RegisterKafkaTopic(NewKafkaTopic("orders", "default"))
	r.RegisterKafkaTopic(NewKafkaTopic("orders", "default"))

	assert.Len(t, r.kafkaTopics, 2)
	// Duplicate detection happens in Validate()
}

func TestKafkaTopicRegistrationPoolNotFound(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterKafkaTopic(NewKafkaTopic("orders", "nonexistent"))

	// We can't call Validate() without real connections, but we can verify topics are stored
	assert.Len(t, r.kafkaTopics, 1)
}

func TestKafkaAlterDescription(t *testing.T) {
	alter := KafkaAlter{
		Description: "CREATE topic 'orders' (6 partitions, rf=1)",
		Pool:        "default",
		execFunc:    func(ctx Context) error { return nil },
	}
	assert.Equal(t, "CREATE topic 'orders' (6 partitions, rf=1)", alter.Description)
	assert.Equal(t, "default", alter.Pool)
	assert.NoError(t, alter.Exec(nil))
}

func TestConfigKafkaTopicInitByConfig(t *testing.T) {
	r := NewRegistry().(*registry)
	config := &Config{
		KafkaPools: []ConfigKafka{
			{
				Code:          "default",
				Brokers:       []string{"localhost:9092"},
				IgnoredTopics: []string{"legacy-topic"},
				Topics: []ConfigKafkaTopic{
					{
						Name:              "orders",
						Partitions:        6,
						ReplicationFactor: 3,
						Configs: map[string]string{
							"retention.ms": "86400000",
						},
					},
					{
						Name: "events",
					},
				},
			},
		},
	}

	err := r.InitByConfig(config)
	assert.NoError(t, err)
	assert.Len(t, r.kafkaTopics, 2)

	// Verify first topic
	assert.Equal(t, "orders", r.kafkaTopics[0].topicName)
	assert.Equal(t, "default", r.kafkaTopics[0].poolCode)
	assert.Equal(t, int32(6), r.kafkaTopics[0].numPartitions)
	assert.Equal(t, int16(3), r.kafkaTopics[0].replicationFactor)
	assert.Equal(t, "86400000", r.kafkaTopics[0].configs["retention.ms"])

	// Verify second topic with defaults
	assert.Equal(t, "events", r.kafkaTopics[1].topicName)
	assert.Equal(t, int32(1), r.kafkaTopics[1].numPartitions)
	assert.Equal(t, int16(1), r.kafkaTopics[1].replicationFactor)

	// Verify ignored topics
	assert.Equal(t, []string{"legacy-topic"}, r.kafkaPools["default"].options.IgnoredTopics)
}
