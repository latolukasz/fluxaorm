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
	Code     string   `yaml:"code" validate:"required"`
	URI      string   `yaml:"uri" validate:"required"`
	Database int      `yaml:"database"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Streams  []string `yaml:"streams"`
}

type ConfigRedisSentinel struct {
	Code       string   `yaml:"code" validate:"required"`
	MasterName string   `yaml:"masterName" validate:"required"`
	Database   int      `yaml:"database"`
	Sentinels  []string `yaml:"sentinels"`
	User       string   `yaml:"user"`
	Password   string   `yaml:"password"`
	Streams    []string `yaml:"streams"`
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

type ConfigKafka struct {
	Code               string                     `yaml:"code" validate:"required"`
	Brokers            []string                   `yaml:"brokers" validate:"required"`
	ClientID           string                     `yaml:"clientID"`
	RequiredAcks       int                        `yaml:"requiredAcks"`
	ProducerLingerMs   int                        `yaml:"producerLingerMs"`
	MaxBufferedRecords int                        `yaml:"maxBufferedRecords"`
	SASLMechanism      string                     `yaml:"saslMechanism"`
	SASLUser           string                     `yaml:"saslUser"`
	SASLPassword       string                     `yaml:"saslPassword"`
	ConsumerGroups     []ConfigKafkaConsumerGroup `yaml:"consumerGroups"`
}

type Config struct {
	MySQlPools         []ConfigMysql         `yaml:"mysqlPools"`
	RedisPools         []ConfigRedis         `yaml:"redisPools"`
	RedisSentinelPools []ConfigRedisSentinel `yaml:"redisSentinelPools"`
	LocalCachePools    []ConfigLocalCache    `yaml:"localCachePools"`
	ClickhousePools    []ConfigClickhouse    `yaml:"clickhousePools"`
	KafkaPools         []ConfigKafka         `yaml:"kafkaPools"`
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
		for _, stream := range pool.Streams {
			r.RegisterRedisStream(stream, pool.Code)
		}
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
		for _, stream := range pool.Streams {
			r.RegisterRedisStream(stream, pool.Code)
		}
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
		if pool.SASLMechanism != "" {
			options.SASL = &KafkaSASLConfig{
				Mechanism: pool.SASLMechanism,
				User:      pool.SASLUser,
				Password:  pool.SASLPassword,
			}
		}
		var consumerGroups []KafkaConsumerGroupSettings
		for _, cg := range pool.ConsumerGroups {
			settings := KafkaConsumerGroupSettings{
				Name:   cg.Name,
				Topics: cg.Topics,
			}
			if cg.SessionTimeoutMs > 0 {
				settings.SessionTimeout = time.Duration(cg.SessionTimeoutMs) * time.Millisecond
			}
			if cg.RebalanceTimeoutMs > 0 {
				settings.RebalanceTimeout = time.Duration(cg.RebalanceTimeoutMs) * time.Millisecond
			}
			if cg.FetchMaxBytes > 0 {
				settings.FetchMaxBytes = int32(cg.FetchMaxBytes)
			}
			if cg.AutoCommitIntervalMs > 0 {
				settings.AutoCommitInterval = time.Duration(cg.AutoCommitIntervalMs) * time.Millisecond
			}
			consumerGroups = append(consumerGroups, settings)
		}
		r.RegisterKafka(pool.Brokers, pool.Code, options, consumerGroups...)
	}
	return nil
}
