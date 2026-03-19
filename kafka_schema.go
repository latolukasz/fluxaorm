package fluxaorm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// KafkaTopicBuilder defines a Kafka topic schema using a fluent API.
type KafkaTopicBuilder struct {
	topicName         string
	poolCode          string
	numPartitions     int32
	replicationFactor int16
	configs           map[string]string
}

// NewKafkaTopic creates a new Kafka topic builder.
func NewKafkaTopic(topicName, poolCode string) *KafkaTopicBuilder {
	return &KafkaTopicBuilder{
		topicName:         topicName,
		poolCode:          poolCode,
		numPartitions:     1,
		replicationFactor: 1,
	}
}

// Partitions sets the number of partitions.
func (b *KafkaTopicBuilder) Partitions(n int32) *KafkaTopicBuilder {
	b.numPartitions = n
	return b
}

// ReplicationFactor sets the replication factor.
func (b *KafkaTopicBuilder) ReplicationFactor(n int16) *KafkaTopicBuilder {
	b.replicationFactor = n
	return b
}

// RetentionMs sets the retention.ms config.
func (b *KafkaTopicBuilder) RetentionMs(ms int64) *KafkaTopicBuilder {
	return b.Config("retention.ms", strconv.FormatInt(ms, 10))
}

// RetentionBytes sets the retention.bytes config.
func (b *KafkaTopicBuilder) RetentionBytes(bytes int64) *KafkaTopicBuilder {
	return b.Config("retention.bytes", strconv.FormatInt(bytes, 10))
}

// MaxMessageBytes sets the max.message.bytes config.
func (b *KafkaTopicBuilder) MaxMessageBytes(bytes int32) *KafkaTopicBuilder {
	return b.Config("max.message.bytes", strconv.FormatInt(int64(bytes), 10))
}

// CleanupPolicy sets the cleanup.policy config ("delete", "compact", "delete,compact").
func (b *KafkaTopicBuilder) CleanupPolicy(policy string) *KafkaTopicBuilder {
	return b.Config("cleanup.policy", policy)
}

// MinInsyncReplicas sets the min.insync.replicas config.
func (b *KafkaTopicBuilder) MinInsyncReplicas(n int) *KafkaTopicBuilder {
	return b.Config("min.insync.replicas", strconv.Itoa(n))
}

// CompressionType sets the compression.type config ("gzip", "snappy", "lz4", "zstd", "uncompressed").
func (b *KafkaTopicBuilder) CompressionType(ct string) *KafkaTopicBuilder {
	return b.Config("compression.type", ct)
}

// Config sets an arbitrary Kafka topic config key-value pair.
func (b *KafkaTopicBuilder) Config(key, value string) *KafkaTopicBuilder {
	if b.configs == nil {
		b.configs = make(map[string]string)
	}
	b.configs[key] = value
	return b
}

func (b *KafkaTopicBuilder) validate() error {
	if b.topicName == "" {
		return fmt.Errorf("kafka topic name is required")
	}
	if b.poolCode == "" {
		return fmt.Errorf("kafka pool code is required for topic '%s'", b.topicName)
	}
	if b.numPartitions < 1 {
		return fmt.Errorf("kafka topic '%s' must have at least 1 partition", b.topicName)
	}
	if b.replicationFactor < 1 {
		return fmt.Errorf("kafka topic '%s' must have replication factor >= 1", b.topicName)
	}
	return nil
}

// KafkaConsumerGroupBuilder defines a Kafka consumer group using a fluent API.
type KafkaConsumerGroupBuilder struct {
	name               string
	poolCode           string
	topics             []string
	sessionTimeout     time.Duration
	rebalanceTimeout   time.Duration
	fetchMaxBytes      int32
	autoCommitInterval time.Duration
}

// NewKafkaConsumerGroup creates a new Kafka consumer group builder.
func NewKafkaConsumerGroup(name, poolCode string) *KafkaConsumerGroupBuilder {
	return &KafkaConsumerGroupBuilder{
		name:     name,
		poolCode: poolCode,
	}
}

// Topics sets the topics for this consumer group.
func (b *KafkaConsumerGroupBuilder) Topics(topics ...string) *KafkaConsumerGroupBuilder {
	b.topics = topics
	return b
}

// SessionTimeout sets the session timeout.
func (b *KafkaConsumerGroupBuilder) SessionTimeout(d time.Duration) *KafkaConsumerGroupBuilder {
	b.sessionTimeout = d
	return b
}

// RebalanceTimeout sets the rebalance timeout.
func (b *KafkaConsumerGroupBuilder) RebalanceTimeout(d time.Duration) *KafkaConsumerGroupBuilder {
	b.rebalanceTimeout = d
	return b
}

// FetchMaxBytes sets the maximum fetch bytes.
func (b *KafkaConsumerGroupBuilder) FetchMaxBytes(n int32) *KafkaConsumerGroupBuilder {
	b.fetchMaxBytes = n
	return b
}

// AutoCommitInterval sets the auto-commit interval (0 = manual commit only).
func (b *KafkaConsumerGroupBuilder) AutoCommitInterval(d time.Duration) *KafkaConsumerGroupBuilder {
	b.autoCommitInterval = d
	return b
}

func (b *KafkaConsumerGroupBuilder) validate() error {
	if b.name == "" {
		return fmt.Errorf("kafka consumer group name is required")
	}
	if b.poolCode == "" {
		return fmt.Errorf("kafka pool code is required for consumer group '%s'", b.name)
	}
	if len(b.topics) == 0 {
		return fmt.Errorf("kafka consumer group '%s' must have at least one topic", b.name)
	}
	return nil
}

func (b *KafkaConsumerGroupBuilder) toSettings() *KafkaConsumerGroupSettings {
	return &KafkaConsumerGroupSettings{
		Name:               b.name,
		Topics:             b.topics,
		SessionTimeout:     b.sessionTimeout,
		RebalanceTimeout:   b.rebalanceTimeout,
		FetchMaxBytes:      b.fetchMaxBytes,
		AutoCommitInterval: b.autoCommitInterval,
	}
}

// KafkaAlter holds a pending Kafka operation.
type KafkaAlter struct {
	Description string
	Pool        string
	execFunc    func(ctx Context) error
}

// Exec executes the Kafka operation.
func (a KafkaAlter) Exec(ctx Context) error {
	return a.execFunc(ctx)
}

// GetKafkaAlters compares registered Kafka topic definitions with actual
// broker state and returns the operations needed to synchronize them.
func GetKafkaAlters(ctx Context) ([]KafkaAlter, error) {
	registry := ctx.Engine().Registry().(*engineRegistryImplementation)

	// Group registered topics by pool
	topicsByPool := make(map[string][]*KafkaTopicBuilder)
	for _, t := range registry.kafkaTopics {
		topicsByPool[t.poolCode] = append(topicsByPool[t.poolCode], t)
	}

	var alters []KafkaAlter

	for poolCode, topics := range topicsByPool {
		kafka := ctx.Engine().Kafka(poolCode)
		if kafka == nil {
			return nil, fmt.Errorf("kafka pool '%s' not found", poolCode)
		}
		pool := kafka.(*kafkaPoolImplementation)
		adminClient := kadm.NewClient(pool.producerClient)

		// List existing topics
		topicDetails, err := adminClient.ListTopics(ctx.Context())
		if err != nil {
			return nil, fmt.Errorf("kafka pool '%s': failed to list topics: %w", poolCode, err)
		}

		// Get configs for existing topics
		existingTopicNames := make([]string, 0)
		for name := range topicDetails {
			existingTopicNames = append(existingTopicNames, name)
		}

		var existingConfigs kadm.ResourceConfigs
		if len(existingTopicNames) > 0 {
			existingConfigs, err = adminClient.DescribeTopicConfigs(ctx.Context(), existingTopicNames...)
			if err != nil {
				return nil, fmt.Errorf("kafka pool '%s': failed to describe topic configs: %w", poolCode, err)
			}
		}

		registeredNames := make(map[string]bool)
		for _, topic := range topics {
			registeredNames[topic.topicName] = true

			detail, exists := topicDetails[topic.topicName]
			if !exists || detail.Err != nil {
				// CREATE topic
				createAlter := buildCreateTopicAlter(poolCode, topic, pool, adminClient)
				alters = append(alters, createAlter)
				continue
			}

			// Check partitions
			currentPartitions := int32(len(detail.Partitions))
			if topic.numPartitions > currentPartitions {
				alters = append(alters, buildIncreasePartitionsAlter(poolCode, topic, currentPartitions, pool, adminClient))
			} else if topic.numPartitions < currentPartitions {
				alters = append(alters, KafkaAlter{
					Description: fmt.Sprintf("-- WARNING: topic '%s' has %d partitions, expected %d. Cannot decrease partitions.", topic.topicName, currentPartitions, topic.numPartitions),
					Pool:        poolCode,
					execFunc:    func(ctx Context) error { return nil },
				})
			}

			// Check replication factor
			if len(detail.Partitions) > 0 {
				currentRF := int16(len(detail.Partitions[0].Replicas))
				if topic.replicationFactor != currentRF {
					alters = append(alters, KafkaAlter{
						Description: fmt.Sprintf("-- WARNING: topic '%s' has replication factor %d, expected %d. Cannot change via admin API.", topic.topicName, currentRF, topic.replicationFactor),
						Pool:        poolCode,
						execFunc:    func(ctx Context) error { return nil },
					})
				}
			}

			// Check configs
			if len(topic.configs) > 0 {
				configAlters := buildConfigAlters(poolCode, topic, existingConfigs, pool, adminClient)
				alters = append(alters, configAlters...)
			}
		}

		// DELETE unregistered topics
		ignoredTopics := registry.kafkaIgnoredTopics[poolCode]
		for topicName := range topicDetails {
			if registeredNames[topicName] {
				continue
			}
			if strings.HasPrefix(topicName, "__") {
				continue
			}
			if ignoredTopics != nil && ignoredTopics[topicName] {
				continue
			}
			alters = append(alters, buildDeleteTopicAlter(poolCode, topicName, pool, adminClient))
		}
	}

	// Delete topics in pools with no registered topics
	for poolCode, kafka := range registry.engine.kafkaServers {
		if _, hasRegistered := topicsByPool[poolCode]; hasRegistered {
			continue
		}
		pool := kafka.(*kafkaPoolImplementation)
		adminClient := kadm.NewClient(pool.producerClient)

		topicDetails, err := adminClient.ListTopics(ctx.Context())
		if err != nil {
			return nil, fmt.Errorf("kafka pool '%s': failed to list topics: %w", poolCode, err)
		}

		ignoredTopics := registry.kafkaIgnoredTopics[poolCode]
		for topicName := range topicDetails {
			if strings.HasPrefix(topicName, "__") {
				continue
			}
			if ignoredTopics != nil && ignoredTopics[topicName] {
				continue
			}
			alters = append(alters, buildDeleteTopicAlter(poolCode, topicName, pool, adminClient))
		}
	}

	// Delete orphaned consumer groups
	cgsByPool := make(map[string]map[string]bool)
	for _, cg := range registry.kafkaConsumerGroups {
		if cgsByPool[cg.poolCode] == nil {
			cgsByPool[cg.poolCode] = make(map[string]bool)
		}
		cgsByPool[cg.poolCode][cg.name] = true
	}

	for poolCode, kafka := range registry.engine.kafkaServers {
		pool := kafka.(*kafkaPoolImplementation)
		adminClient := kadm.NewClient(pool.producerClient)

		listedGroups, err := adminClient.ListGroups(ctx.Context())
		if err != nil {
			return nil, fmt.Errorf("kafka pool '%s': failed to list consumer groups: %w", poolCode, err)
		}

		registeredCGs := cgsByPool[poolCode]
		ignoredCGs := registry.kafkaIgnoredConsumerGroups[poolCode]
		for _, groupName := range listedGroups.Groups() {
			if registeredCGs != nil && registeredCGs[groupName] {
				continue
			}
			if strings.HasPrefix(groupName, "__") {
				continue
			}
			if ignoredCGs != nil && ignoredCGs[groupName] {
				continue
			}
			alters = append(alters, buildDeleteConsumerGroupAlter(poolCode, groupName, pool, adminClient))
		}
	}

	sort.Slice(alters, func(i, j int) bool {
		return alters[i].Description < alters[j].Description
	})

	return alters, nil
}

func buildCreateTopicAlter(poolCode string, topic *KafkaTopicBuilder, pool *kafkaPoolImplementation, adminClient *kadm.Client) KafkaAlter {
	desc := fmt.Sprintf("CREATE topic '%s' (%d partitions, rf=%d)", topic.topicName, topic.numPartitions, topic.replicationFactor)
	if len(topic.configs) > 0 {
		var parts []string
		for k, v := range topic.configs {
			parts = append(parts, k+"="+v)
		}
		sort.Strings(parts)
		desc += " [" + strings.Join(parts, ", ") + "]"
	}
	topicName := topic.topicName
	partitions := topic.numPartitions
	rf := topic.replicationFactor
	configs := toStringPtrMap(topic.configs)
	return KafkaAlter{
		Description: desc,
		Pool:        poolCode,
		execFunc: func(ctx Context) error {
			resp, err := adminClient.CreateTopic(ctx.Context(), partitions, rf, configs, topicName)
			if err != nil {
				return fmt.Errorf("failed to create topic '%s': %w", topicName, err)
			}
			if resp.Err != nil {
				return fmt.Errorf("failed to create topic '%s': %w", topicName, resp.Err)
			}
			return nil
		},
	}
}

func buildIncreasePartitionsAlter(poolCode string, topic *KafkaTopicBuilder, currentPartitions int32, pool *kafkaPoolImplementation, adminClient *kadm.Client) KafkaAlter {
	topicName := topic.topicName
	targetPartitions := topic.numPartitions
	return KafkaAlter{
		Description: fmt.Sprintf("ALTER topic '%s' partitions %d -> %d", topicName, currentPartitions, targetPartitions),
		Pool:        poolCode,
		execFunc: func(ctx Context) error {
			resp, err := adminClient.CreatePartitions(ctx.Context(), int(targetPartitions), topicName)
			if err != nil {
				return fmt.Errorf("failed to increase partitions for topic '%s': %w", topicName, err)
			}
			for _, r := range resp {
				if r.Err != nil {
					return fmt.Errorf("failed to increase partitions for topic '%s': %w", topicName, r.Err)
				}
			}
			return nil
		},
	}
}

func buildConfigAlters(poolCode string, topic *KafkaTopicBuilder, existingConfigs kadm.ResourceConfigs, pool *kafkaPoolImplementation, adminClient *kadm.Client) []KafkaAlter {
	var configRC kadm.ResourceConfig
	for _, rc := range existingConfigs {
		if rc.Name == topic.topicName {
			configRC = rc
			break
		}
	}

	configsToSet := make(map[string]string)
	for key, expectedValue := range topic.configs {
		currentValue := ""
		if configRC.Configs != nil {
			for _, c := range configRC.Configs {
				if c.Key == key {
					if c.Value != nil {
						currentValue = *c.Value
					}
					break
				}
			}
		}
		if currentValue != expectedValue {
			configsToSet[key] = expectedValue
		}
	}

	if len(configsToSet) == 0 {
		return nil
	}

	var parts []string
	for k, v := range configsToSet {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)

	topicName := topic.topicName
	alterConfigs := make([]kadm.AlterConfig, 0, len(configsToSet))
	for k, v := range configsToSet {
		alterConfigs = append(alterConfigs, kadm.AlterConfig{
			Name:  k,
			Value: kadm.StringPtr(v),
			Op:    kadm.SetConfig,
		})
	}

	return []KafkaAlter{
		{
			Description: fmt.Sprintf("ALTER topic '%s' configs: %s", topicName, strings.Join(parts, ", ")),
			Pool:        poolCode,
			execFunc: func(ctx Context) error {
				resp, err := adminClient.AlterTopicConfigs(ctx.Context(), alterConfigs, topicName)
				if err != nil {
					return fmt.Errorf("failed to alter configs for topic '%s': %w", topicName, err)
				}
				for _, r := range resp {
					if r.Err != nil {
						return fmt.Errorf("failed to alter configs for topic '%s': %w", topicName, r.Err)
					}
				}
				return nil
			},
		},
	}
}

func toStringPtrMap(m map[string]string) map[string]*string {
	if m == nil {
		return nil
	}
	result := make(map[string]*string, len(m))
	for k, v := range m {
		v := v
		result[k] = &v
	}
	return result
}

func buildDeleteTopicAlter(poolCode string, topicName string, pool *kafkaPoolImplementation, adminClient *kadm.Client) KafkaAlter {
	return KafkaAlter{
		Description: fmt.Sprintf("DELETE topic '%s'", topicName),
		Pool:        poolCode,
		execFunc: func(ctx Context) error {
			resp, err := adminClient.DeleteTopics(ctx.Context(), topicName)
			if err != nil {
				return fmt.Errorf("failed to delete topic '%s': %w", topicName, err)
			}
			for _, r := range resp {
				if r.Err != nil {
					return fmt.Errorf("failed to delete topic '%s': %w", topicName, r.Err)
				}
			}
			return nil
		},
	}
}

func buildDeleteConsumerGroupAlter(poolCode string, groupName string, pool *kafkaPoolImplementation, adminClient *kadm.Client) KafkaAlter {
	return KafkaAlter{
		Description: fmt.Sprintf("DELETE consumer group '%s'", groupName),
		Pool:        poolCode,
		execFunc: func(ctx Context) error {
			resp, err := adminClient.DeleteGroups(ctx.Context(), groupName)
			if err != nil {
				return fmt.Errorf("failed to delete consumer group '%s': %w", groupName, err)
			}
			for _, r := range resp {
				if r.Err != nil {
					return fmt.Errorf("failed to delete consumer group '%s': %w", groupName, r.Err)
				}
			}
			return nil
		},
	}
}
