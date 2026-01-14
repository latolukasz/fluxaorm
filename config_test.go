package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	registry := NewRegistry()
	config := &Config{
		MySQlPools: []ConfigMysql{
			{URI: "root:root@tcp(localhost:3397)/test", Code: "default"},
		},
		RedisPools: []ConfigRedis{
			{URI: "localhost:6395", Code: "default", Database: 0, Streams: []string{
				"test-stream", "test-group",
			}},
			{URI: "localhost:6395", Code: "test", Database: 1},
		},
		LocalCachePools: []ConfigLocalCache{
			{Code: "test", Limit: 10000},
			{Code: "default", Limit: 200},
		},
	}

	err := registry.InitByConfig(config)
	assert.NoError(t, err)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	assert.Len(t, engine.Registry().LocalCachePools(), 2)
	assert.Equal(t, 200, engine.LocalCache("default").GetConfig().GetLimit())
	assert.Equal(t, 10000, engine.LocalCache("test").GetConfig().GetLimit())

	assert.Len(t, engine.Registry().RedisPools(), 2)
	assert.Equal(t, 0, engine.Redis("default").GetConfig().GetDatabaseNumber())
	assert.Equal(t, 1, engine.Redis("test").GetConfig().GetDatabaseNumber())

}
