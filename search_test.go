package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type searchEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestSearch(t *testing.T) {
	var entity *searchEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema := GetEntitySchema[searchEntity](orm)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[searchEntity](orm)
		entity.Name = "name %d"
		ids = append(ids, entity.ID)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	rows, total, err := SearchWithCount[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, total)
	assert.Equal(t, 8, rows.Len())

	foundIDs, total, err := SearchIDsWithCount[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, total)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])

	foundIDs, err = SearchIDs[searchEntity](orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])

	entity, found, err := SearchOne[searchEntity](orm, NewWhere("ID = ?", ids[2]))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, ids[2], entity.ID)

	rowsAnonymous, total, err := schema.SearchWithCount(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, total)
	assert.Equal(t, 8, rowsAnonymous.Len())

	iterations := 0
	for rowsAnonymous.Next() {
		row, err := rowsAnonymous.Entity()
		assert.NoError(t, err)
		assert.Equal(t, ids[iterations+2], row.(*searchEntity).ID)
		iterations++
	}
	assert.Equal(t, 8, iterations)
	rowsAnonymous, err = schema.Search(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, rowsAnonymous.Len())
	foundIDs, total, err = schema.SearchIDsWithCount(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, total)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])
	foundIDs, err = schema.SearchIDs(orm, NewWhere("ID > ?", ids[1]), nil)
	assert.NoError(t, err)
	assert.Len(t, foundIDs, 8)
	assert.Equal(t, ids[2], foundIDs[0])
}
