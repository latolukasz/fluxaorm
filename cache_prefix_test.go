package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type prefixV1 struct {
	ID   uint64 `orm:"table=prefix_probe;redisCache"`
	Name string
}

type prefixV2 struct {
	ID    uint64 `orm:"table=prefix_probe;redisCache"`
	Name  string
	Added string
}

type prefixOtherTable struct {
	ID   uint64 `orm:"table=prefix_probe_other;redisCache"`
	Name string
}

func schemaFor(t *testing.T, entity any) *entitySchema {
	t.Helper()
	r := NewRegistry()
	r.RegisterMySQL("user:pass@tcp(localhost:3306)/test", DefaultPoolCode, &MySQLOptions{})
	r.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	r.RegisterEntity(entity)
	engine, err := r.ValidateForCodeGen()
	require.NoError(t, err)
	for _, schema := range engine.Registry().(*engineRegistryImplementation).entitySchemas {
		return schema
	}
	t.Fatal("no schema registered")

	return nil
}

// The bug this closes: the prefix used to include the column list, so a schema change moved the
// key space. Mid-rollout the new version cached under one prefix while the old version invalidated
// under another, and neither could see the other's writes — stale reads for the full cache TTL.
func TestCachePrefixSurvivesAColumnChange(t *testing.T) {
	before := schemaFor(t, prefixV1{})
	after := schemaFor(t, prefixV2{})

	assert.Equal(t, before.cacheKey, after.cacheKey,
		"two versions of one table must share a key space, or writes by one are invisible to the other")
	assert.NotEqual(t, before.structureHash, after.structureHash,
		"the stamp is the only thing left separating the two layouts")
}

func TestCachePrefixDiffersPerTable(t *testing.T) {
	assert.NotEqual(t, schemaFor(t, prefixV1{}).cacheKey, schemaFor(t, prefixOtherTable{}).cacheKey)
}

// With the prefix carrying no shape information, a stamp collision is read positionally into
// originRedisValues, which is an out-of-range panic or a silently wrong field rather than a miss.
func TestStampCarriesTheShapeEntropy(t *testing.T) {
	schema := schemaFor(t, prefixV1{})

	assert.Len(t, schema.cacheKey, 8)
	assert.Len(t, schema.structureHash, 16)
}

// A collision has to name the two tables rather than silently unlink one entity's rows when the
// other's cache is cleared.
func TestCollidingPrefixesAreRejected(t *testing.T) {
	schemas := map[string]*entitySchema{
		"a": {tableName: "alpha", cacheKey: "deadbeef", hasRedisCache: true},
		"b": {tableName: "beta", cacheKey: "deadbeef", hasRedisCache: true},
	}

	err := validateRedisKeyNamespaces(schemas)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "alpha")
	assert.Contains(t, err.Error(), "beta")
}

// Row keys and search document keys live in one Redis keyspace, and ClearRedisCache deletes by
// SCANning the row prefix. Today's two derivations cannot produce the same string; the guard is
// what keeps that true if either one changes.
func TestSearchPrefixSharesTheNamespace(t *testing.T) {
	schemas := map[string]*entitySchema{
		"a": {tableName: "alpha", cacheKey: "abc12345", hasRedisCache: true},
		"b": {tableName: "beta", redisSearchPrefix: "abc12345:", hasRedisSearch: true},
	}

	assert.Error(t, validateRedisKeyNamespaces(schemas))
}
