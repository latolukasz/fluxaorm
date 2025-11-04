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

type Config struct {
	MySQlPools         []ConfigMysql         `yaml:"mysqlPools"`
	RedisPools         []ConfigRedis         `yaml:"redisPools"`
	RedisSentinelPools []ConfigRedisSentinel `yaml:"redisSentinelPools"`
	LocalCachePools    []ConfigLocalCache    `yaml:"localCachePools"`
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
	return nil
}
