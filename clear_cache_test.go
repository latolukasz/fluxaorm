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

	entity := NewEntity[clearEntity](orm)
	entity.Age = 1
	entity = NewEntity[clearEntity](orm)
	entity.Age = 2
	assert.NoError(t, orm.FlushWithCheck())

	schema := GetEntitySchema[clearEntity](orm)
	cleared, err := schema.ClearCache(orm)
	assert.NoError(t, err)
	assert.Equal(t, 5, cleared)
}
