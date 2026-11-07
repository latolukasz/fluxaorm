package fluxaorm

// EntityProvider is the base interface implemented by all generated entity providers.
type EntityProvider interface {
	TableName() string
	DBCode() string
}

// RedisCacheEntityProvider is implemented by providers whose entity has a Redis cache (redisCache tag).
type RedisCacheEntityProvider interface {
	RedisCode() string
	RedisCachePrefix() string
	ClearRedisCache(ctx Context) (int, error)
}
