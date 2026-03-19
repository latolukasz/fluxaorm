package fluxaorm

import (
	"time"
)

type ConfigMysql struct {
	Code               string   `yaml:"code" validate:"required"`
	URI                string   `yaml:"uri" validate:"required"`
	ConnMaxLifetime    int      `yaml:"connMaxLifetime"`
	MaxOpenConnections int      `yaml:"maxOpenConnections"`
	MaxIdleConnections int      `yaml:"maxIdleConnections"`
	DefaultEncoding    string   `yaml:"defaultEncoding"`
	DefaultCollate     string   `yaml:"defaultCollate"`
	IgnoredTables      []string `yaml:"ignoredTables"`
}

type ConfigRedis struct {
	Code     string `yaml:"code" validate:"required"`
	URI      string `yaml:"uri" validate:"required"`
	Database int    `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type ConfigRedisSentinel struct {
	Code       string   `yaml:"code" validate:"required"`
	MasterName string   `yaml:"masterName" validate:"required"`
	Database   int      `yaml:"database"`
	Sentinels  []string `yaml:"sentinels"`
	User       string   `yaml:"user"`
	Password   string   `yaml:"password"`
}

type ConfigLocalCache struct {
	Code  string `yaml:"code" validate:"required"`
	Limit int    `yaml:"limit" validate:"required"`
}

type ConfigClickhouse struct {
	Code               string   `yaml:"code" validate:"required"`
	URI                string   `yaml:"uri" validate:"required"`
	ConnMaxLifetime    int      `yaml:"connMaxLifetime"`
	MaxOpenConnections int      `yaml:"maxOpenConnections"`
	MaxIdleConnections int      `yaml:"maxIdleConnections"`
	IgnoredTables      []string `yaml:"ignoredTables"`
}

type ConfigKafkaConsumerGroup struct {
	Name                 string   `yaml:"name" validate:"required"`
	Topics               []string `yaml:"topics" validate:"required"`
	SessionTimeoutMs     int      `yaml:"sessionTimeoutMs"`
	RebalanceTimeoutMs   int      `yaml:"rebalanceTimeoutMs"`
	FetchMaxBytes        int      `yaml:"fetchMaxBytes"`
	AutoCommitIntervalMs int      `yaml:"autoCommitIntervalMs"`
}

type ConfigKafkaTopic struct {
	Name              string            `yaml:"name" validate:"required"`
	Partitions        int32             `yaml:"partitions"`
	ReplicationFactor int16             `yaml:"replicationFactor"`
	Configs           map[string]string `yaml:"configs"`
}

type ConfigKafka struct {
	Code                  string                     `yaml:"code" validate:"required"`
	Brokers               []string                   `yaml:"brokers" validate:"required"`
	ClientID              string                     `yaml:"clientID"`
	RequiredAcks          int                        `yaml:"requiredAcks"`
	ProducerLingerMs      int                        `yaml:"producerLingerMs"`
	MaxBufferedRecords    int                        `yaml:"maxBufferedRecords"`
	SASLMechanism         string                     `yaml:"saslMechanism"`
	SASLUser              string                     `yaml:"saslUser"`
	SASLPassword          string                     `yaml:"saslPassword"`
	ConsumerGroups        []ConfigKafkaConsumerGroup `yaml:"consumerGroups"`
	IgnoredTopics         []string                   `yaml:"ignoredTopics"`
	IgnoredConsumerGroups []string                   `yaml:"ignoredConsumerGroups"`
	Topics                []ConfigKafkaTopic         `yaml:"topics"`
}

type ConfigAsyncFlush struct {
	KafkaPool       string `yaml:"kafkaPool" validate:"required"`
	TopicPartitions int32  `yaml:"topicPartitions"`
}

type Config struct {
	MySQlPools         []ConfigMysql         `yaml:"mysqlPools"`
	RedisPools         []ConfigRedis         `yaml:"redisPools"`
	RedisSentinelPools []ConfigRedisSentinel `yaml:"redisSentinelPools"`
	LocalCachePools    []ConfigLocalCache    `yaml:"localCachePools"`
	ClickhousePools    []ConfigClickhouse    `yaml:"clickhousePools"`
	KafkaPools         []ConfigKafka         `yaml:"kafkaPools"`
	AsyncFlush         *ConfigAsyncFlush     `yaml:"asyncFlush"`
}

func (r *registry) InitByConfig(config *Config) error {
	for _, pool := range config.MySQlPools {
		options := &MySQLOptions{}
		options.ConnMaxLifetime = time.Duration(pool.ConnMaxLifetime) * time.Second
		options.MaxOpenConnections = pool.MaxOpenConnections
		options.MaxIdleConnections = pool.MaxIdleConnections
		options.DefaultEncoding = pool.DefaultEncoding
		options.DefaultCollate = pool.DefaultCollate
		options.IgnoredTables = pool.IgnoredTables
		r.RegisterMySQL(pool.URI, pool.Code, options)
	}
	for _, pool := range config.RedisPools {
		options := &RedisOptions{}
		if pool.User != "" {
			options.User = pool.User
		}
		if pool.Password != "" {
			options.Password = pool.Password
		}
		r.RegisterRedis(pool.URI, pool.Database, pool.Code, options)
	}
	for _, pool := range config.RedisSentinelPools {
		options := &RedisOptions{Master: pool.MasterName, Sentinels: pool.Sentinels}
		if pool.User != "" {
			options.User = pool.User
		}
		if pool.Password != "" {
			options.Password = pool.Password
		}
		r.RegisterRedis("", pool.Database, pool.Code, options)
	}
	for _, pool := range config.LocalCachePools {
		r.RegisterLocalCache(pool.Code, pool.Limit)
	}
	for _, pool := range config.ClickhousePools {
		options := &ClickhouseOptions{}
		options.ConnMaxLifetime = time.Duration(pool.ConnMaxLifetime) * time.Second
		options.MaxOpenConnections = pool.MaxOpenConnections
		options.MaxIdleConnections = pool.MaxIdleConnections
		options.IgnoredTables = pool.IgnoredTables
		r.RegisterClickhouse(pool.URI, pool.Code, options)
	}
	for _, pool := range config.KafkaPools {
		options := &KafkaPoolOptions{}
		options.ClientID = pool.ClientID
		options.RequiredAcks = pool.RequiredAcks
		if pool.ProducerLingerMs > 0 {
			options.ProducerLinger = time.Duration(pool.ProducerLingerMs) * time.Millisecond
		}
		options.MaxBufferedRecords = pool.MaxBufferedRecords
		options.IgnoredTopics = pool.IgnoredTopics
		options.IgnoredConsumerGroups = pool.IgnoredConsumerGroups
		if pool.SASLMechanism != "" {
			options.SASL = &KafkaSASLConfig{
				Mechanism: pool.SASLMechanism,
				User:      pool.SASLUser,
				Password:  pool.SASLPassword,
			}
		}
		r.RegisterKafka(pool.Brokers, pool.Code, options)
		for _, cg := range pool.ConsumerGroups {
			builder := NewKafkaConsumerGroup(cg.Name, pool.Code).Topics(cg.Topics...)
			if cg.SessionTimeoutMs > 0 {
				builder.SessionTimeout(time.Duration(cg.SessionTimeoutMs) * time.Millisecond)
			}
			if cg.RebalanceTimeoutMs > 0 {
				builder.RebalanceTimeout(time.Duration(cg.RebalanceTimeoutMs) * time.Millisecond)
			}
			if cg.FetchMaxBytes > 0 {
				builder.FetchMaxBytes(int32(cg.FetchMaxBytes))
			}
			if cg.AutoCommitIntervalMs > 0 {
				builder.AutoCommitInterval(time.Duration(cg.AutoCommitIntervalMs) * time.Millisecond)
			}
			r.RegisterKafkaConsumerGroup(builder)
		}
		for _, topic := range pool.Topics {
			builder := NewKafkaTopic(topic.Name, pool.Code)
			if topic.Partitions > 0 {
				builder.Partitions(topic.Partitions)
			}
			if topic.ReplicationFactor > 0 {
				builder.ReplicationFactor(topic.ReplicationFactor)
			}
			for k, v := range topic.Configs {
				builder.Config(k, v)
			}
			r.RegisterKafkaTopic(builder)
		}
	}
	if config.AsyncFlush != nil {
		r.RegisterAsyncFlush(config.AsyncFlush.KafkaPool, &AsyncFlushOptions{
			TopicPartitions: config.AsyncFlush.TopicPartitions,
		})
	}
	return nil
}
