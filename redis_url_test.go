package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterRedisURL(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterRedis("rediss://app:secret@cache.example:6379/2", 0, "cache", nil)
	r.RegisterRedis("redis://state.example:6379", 0, "state", &RedisOptions{User: "app", Password: "other"})
	r.RegisterRedis("localhost:6395", 1, "plain", &RedisOptions{User: "u", Password: "p"})

	cache := r.redisPools["cache"]
	options := cache.getClient().Options()
	assert.Equal(t, "cache.example:6379", options.Addr)
	assert.Equal(t, "app", options.Username)
	assert.Equal(t, "secret", options.Password)
	assert.Equal(t, 2, options.DB)
	assert.NotNil(t, options.TLSConfig)
	assert.Equal(t, "cache.example", options.TLSConfig.ServerName)
	assert.True(t, options.UnstableResp3)
	assert.Equal(t, "cache.example:6379", cache.GetAddress())
	assert.Equal(t, 2, cache.GetDatabaseNumber())

	state := r.redisPools["state"].getClient().Options()
	assert.Nil(t, state.TLSConfig)
	assert.Equal(t, "app", state.Username)
	assert.Equal(t, "other", state.Password)

	plain := r.redisPools["plain"]
	assert.Equal(t, "localhost:6395", plain.getClient().Options().Addr)
	assert.Nil(t, plain.getClient().Options().TLSConfig)
	assert.Equal(t, "u", plain.getClient().Options().Username)
	assert.Equal(t, 1, plain.GetDatabaseNumber())
}

func TestRegisterRedisURLDatabaseOverride(t *testing.T) {
	r := NewRegistry().(*registry)
	r.RegisterRedis("rediss://cache.example:6379", 3, "cache", nil)
	assert.Equal(t, 3, r.redisPools["cache"].getClient().Options().DB)
}

func TestInitByConfigRejectsBadRedisURL(t *testing.T) {
	err := NewRegistry().InitByConfig(&Config{RedisPools: []ConfigRedis{{Code: "cache", URI: "rediss://cache.example:6379/notanumber"}}})
	assert.ErrorContains(t, err, "redis pool 'cache'")
}

func TestInitByYamlRedisURL(t *testing.T) {
	r := NewRegistry().(*registry)
	err := r.InitByYaml(map[string]any{"cache": map[string]any{"redis": "rediss://app:secret@cache.example:6379/0"}})
	assert.NoError(t, err)
	assert.NotNil(t, r.redisPools["cache"].getClient().Options().TLSConfig)
}
