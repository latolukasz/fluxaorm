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

type ConfigNatsConsumer struct {
	Name           string   `yaml:"name" validate:"required"`
	FilterSubjects []string `yaml:"filterSubjects"`
	AckWaitMs      int      `yaml:"ackWaitMs"`
	MaxAckPending  int      `yaml:"maxAckPending"`
	MaxDeliver     int      `yaml:"maxDeliver"`
}

type ConfigNatsStream struct {
	Name            string   `yaml:"name" validate:"required"`
	Subjects        []string `yaml:"subjects" validate:"required"`
	MaxAgeMs        int      `yaml:"maxAgeMs"`
	MaxBytes        int64    `yaml:"maxBytes"`
	MaxMsgSize      int32    `yaml:"maxMsgSize"`
	Replicas        int      `yaml:"replicas"`
	DuplicateWindow int      `yaml:"duplicateWindowMs"`
	Storage         string   `yaml:"storage"`   // "file" or "memory"
	Retention       string   `yaml:"retention"` // "limits", "interest", "workqueue"
}

type ConfigNats struct {
	Code             string               `yaml:"code" validate:"required"`
	URLs             []string             `yaml:"urls" validate:"required"`
	ClientID         string               `yaml:"clientID"`
	MaxReconnects    int                  `yaml:"maxReconnects"`
	ReconnectWaitMs  int                  `yaml:"reconnectWaitMs"`
	ReconnectBufSize int                  `yaml:"reconnectBufSize"`
	AuthToken        string               `yaml:"authToken"`
	AuthUser         string               `yaml:"authUser"`
	AuthPassword     string               `yaml:"authPassword"`
	AuthCredsFile    string               `yaml:"authCredsFile"`
	AuthNKeySeed     string               `yaml:"authNKeySeed"`
	IgnoredSubjects  []string             `yaml:"ignoredSubjects"`
	IgnoredConsumers []string             `yaml:"ignoredConsumers"`
	Consumers        []ConfigNatsConsumer `yaml:"consumers"`
	Streams          []ConfigNatsStream   `yaml:"streams"`
}

type ConfigAsyncFlush struct {
	NatsPool          string `yaml:"natsPool" validate:"required"`
	StreamReplicas    int    `yaml:"streamReplicas"`
	DuplicateWindowMs int    `yaml:"duplicateWindowMs"`
	MaxAckPending     int    `yaml:"maxAckPending"`
	AckWaitMs         int    `yaml:"ackWaitMs"`
	MaxDeliver        int    `yaml:"maxDeliver"`
}

type Config struct {
	MySQlPools         []ConfigMysql         `yaml:"mysqlPools"`
	RedisPools         []ConfigRedis         `yaml:"redisPools"`
	RedisSentinelPools []ConfigRedisSentinel `yaml:"redisSentinelPools"`
	LocalCachePools    []ConfigLocalCache    `yaml:"localCachePools"`
	ClickhousePools    []ConfigClickhouse    `yaml:"clickhousePools"`
	NatsPools          []ConfigNats          `yaml:"natsPools"`
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
	for _, pool := range config.NatsPools {
		options := &NatsPoolOptions{
			ClientID:         pool.ClientID,
			MaxReconnects:    pool.MaxReconnects,
			ReconnectBufSize: pool.ReconnectBufSize,
			IgnoredSubjects:  pool.IgnoredSubjects,
			IgnoredConsumers: pool.IgnoredConsumers,
		}
		if pool.ReconnectWaitMs > 0 {
			options.ReconnectWait = time.Duration(pool.ReconnectWaitMs) * time.Millisecond
		}
		if pool.AuthToken != "" || pool.AuthUser != "" || pool.AuthCredsFile != "" || pool.AuthNKeySeed != "" {
			options.Auth = &NatsAuthConfig{
				Token:     pool.AuthToken,
				User:      pool.AuthUser,
				Password:  pool.AuthPassword,
				CredsFile: pool.AuthCredsFile,
				NKeySeed:  pool.AuthNKeySeed,
			}
		}
		r.RegisterNats(pool.URLs, pool.Code, options)
		for _, cons := range pool.Consumers {
			builder := NewNatsConsumer(cons.Name, pool.Code).FilterSubjects(cons.FilterSubjects...)
			if cons.AckWaitMs > 0 {
				builder.AckWait(time.Duration(cons.AckWaitMs) * time.Millisecond)
			}
			if cons.MaxAckPending > 0 {
				builder.MaxAckPending(cons.MaxAckPending)
			}
			if cons.MaxDeliver != 0 {
				builder.MaxDeliver(cons.MaxDeliver)
			}
			r.RegisterNatsConsumer(builder)
		}
		for _, stream := range pool.Streams {
			builder := NewNatsStream(stream.Name, pool.Code).Subjects(stream.Subjects...)
			if stream.MaxAgeMs > 0 {
				builder.MaxAge(time.Duration(stream.MaxAgeMs) * time.Millisecond)
			}
			if stream.MaxBytes > 0 {
				builder.MaxBytes(stream.MaxBytes)
			}
			if stream.MaxMsgSize > 0 {
				builder.MaxMsgSize(stream.MaxMsgSize)
			}
			if stream.Replicas > 0 {
				builder.Replicas(stream.Replicas)
			}
			if stream.DuplicateWindow > 0 {
				builder.Duplicates(time.Duration(stream.DuplicateWindow) * time.Millisecond)
			}
			r.RegisterNatsStream(builder)
		}
	}
	if config.AsyncFlush != nil {
		opts := &AsyncFlushOptions{
			StreamReplicas: config.AsyncFlush.StreamReplicas,
			MaxAckPending:  config.AsyncFlush.MaxAckPending,
			MaxDeliver:     config.AsyncFlush.MaxDeliver,
		}
		if config.AsyncFlush.DuplicateWindowMs > 0 {
			opts.DuplicateWindow = time.Duration(config.AsyncFlush.DuplicateWindowMs) * time.Millisecond
		}
		if config.AsyncFlush.AckWaitMs > 0 {
			opts.AckWait = time.Duration(config.AsyncFlush.AckWaitMs) * time.Millisecond
		}
		r.RegisterAsyncFlush(config.AsyncFlush.NatsPool, opts)
	}
	return nil
}
