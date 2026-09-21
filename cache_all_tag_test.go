package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cacheAllEntityValid struct {
	ID   uint64 `orm:"redisCache;cached"`
	Name string
}

type cacheAllEntityNoRedis struct {
	ID   uint64 `orm:"cached"`
	Name string
}

type cacheAllEntityUntagged struct {
	ID   uint64 `orm:"redisCache"`
	Name string
}

func validateCacheAll(t *testing.T, entities ...any) (Engine, error) {
	t.Helper()

	r := NewRegistry()
	r.RegisterMySQL("user:pass@tcp(localhost:3306)/test", DefaultPoolCode, &MySQLOptions{})
	r.RegisterRedis("localhost:6379", 0, DefaultPoolCode, &RedisOptions{})
	r.RegisterEntity(entities...)

	return r.ValidateForCodeGen()
}

func cacheAllSchema(t *testing.T, engine Engine, table string) *entitySchema {
	t.Helper()

	reg := engine.Registry().(*engineRegistryImplementation)
	for _, schema := range reg.entitySchemas {
		if schema.tableName == table {
			return schema
		}
	}

	t.Fatalf("schema not found for table %s", table)

	return nil
}

// The tag is only meaningful on top of the row cache: GetAll hydrates through GetByIDs, so without
// redisCache it would generate a method that reads every row from MySQL while looking cached.
// Rejected at registration rather than silently downgraded, in the style of the retired `dirty` tag.
func TestCachedTagRequiresRedisCache(t *testing.T) {
	_, err := validateCacheAll(t, cacheAllEntityNoRedis{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cacheAllEntityNoRedis")
	assert.Contains(t, err.Error(), "redisCache")
}

func TestCachedTagIsOptIn(t *testing.T) {
	engine, err := validateCacheAll(t, cacheAllEntityValid{}, cacheAllEntityUntagged{})
	require.NoError(t, err)

	assert.True(t, cacheAllSchema(t, engine, "cacheAllEntityValid").cacheAll)
	assert.False(t, cacheAllSchema(t, engine, "cacheAllEntityUntagged").cacheAll)
}
