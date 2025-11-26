package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type clearEntity struct {
	ID  uint64 `orm:"localCache;redisCache;cacheAll;redisSearch=default"`
	Age uint8  `orm:"index;Age;cached;searchable"`
}

func TestClearEntity(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), clearEntity{})
	assert.NotNil(t, orm)

	entity, err := NewEntity[clearEntity](orm)
	assert.NoError(t, err)
	entity.Age = 1
	entity, err = NewEntity[clearEntity](orm)
	assert.NoError(t, err)
	entity.Age = 2
	assert.NoError(t, orm.Flush())

	schema, err := GetEntitySchema[clearEntity](orm)
	assert.NoError(t, err)
	cleared, err := schema.ClearCache(orm)
	assert.NoError(t, err)
	assert.Equal(t, 5, cleared)
}
