package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testFakeDeleteEntityIndexes = struct {
	Name       IndexDefinition
	Code       UniqueIndexDefinition
	CodeCached UniqueIndexDefinition
	AgeCached  IndexDefinition
}{
	Name:       IndexDefinition{"Name", false},
	Code:       UniqueIndexDefinition{"Code", false},
	CodeCached: UniqueIndexDefinition{"CodeCached", true},
	AgeCached:  IndexDefinition{"AgeCached", true},
}

type testFakeDeleteEntity struct {
	ID         uint64 `orm:"localCache;redisCache;redisSearch"`
	Name       string `orm:"searchable"`
	Code       string
	CodeCached string
	AgeCached  int
	Ref        Reference[testFakeDeleteEntityReference]
	FakeDelete bool
}

func (e *testFakeDeleteEntity) Indexes() any {
	return testFakeDeleteEntityIndexes
}

type testFakeDeleteEntityReference struct {
	ID   uint32
	Name string
}

func TestFakeDeleteNoCache(t *testing.T) {
	testFakeDelete(t, false, false)
}

func TestFakeDeleteRedis(t *testing.T) {
	testFakeDelete(t, false, true)
}

func TestFakeDeleteLocal(t *testing.T) {
	testFakeDelete(t, true, false)
}

func TestFakeDeleteLocalRedis(t *testing.T) {
	testFakeDelete(t, true, true)
}

func testFakeDelete(t *testing.T, local, redis bool) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, &testFakeDeleteEntity{}, &testFakeDeleteEntityReference{})

	schema, err := GetEntitySchema[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)
	schema, err = GetEntitySchema[testFakeDeleteEntityReference](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)

	ref, err := NewEntity[testFakeDeleteEntityReference](orm)
	assert.NoError(t, err)
	entity, err := NewEntity[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	entity.Name = "a"
	entity.Code = "a"
	entity.AgeCached = 1
	entity2, err := NewEntity[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	entity2.Name = "b"
	entity2.Code = "b"
	entity2.CodeCached = "b"
	entity2.AgeCached = 2
	entity3, err := NewEntity[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	entity3.Name = "c"
	entity3.Code = "c"
	entity3.AgeCached = 3
	ref.Name = "ref"
	entity3.Ref = Reference[testFakeDeleteEntityReference](ref.ID)
	err = orm.Flush()
	assert.NoError(t, err)

	_, found, err := GetByID[testFakeDeleteEntity](orm, entity2.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	err = DeleteEntity(orm, entity2)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())

	deleted, found, err := GetByID[testFakeDeleteEntity](orm, entity2.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted.FakeDelete)

	rows, err := Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, rows.Len())
	allI, err := GetAll[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	all, err := allI.All()
	assert.NoError(t, err)

	assert.Equal(t, entity.ID, all[0].ID)
	assert.Equal(t, entity3.ID, all[1].ID)

	rowsFromRS, totalInRS, err := RedisSearch[testFakeDeleteEntity](orm, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, totalInRS)
	assert.Equal(t, 2, rowsFromRS.Len())

	rows, err = Search[testFakeDeleteEntity](orm, NewWhere("1").WithFakeDeletes(), nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())

	_, err = GetByIndex[testFakeDeleteEntity](orm, nil, testFakeDeleteEntityIndexes.AgeCached, 3)
	assert.NoError(t, err)

	_, _, err = GetByUniqueIndex[testFakeDeleteEntity](orm, testFakeDeleteEntityIndexes.CodeCached, "b")

	err = DeleteEntity(orm, entity3)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())

	rows, err = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, entity.ID, all[0].ID)

	rows, err = GetByIDs[testFakeDeleteEntity](orm, entity.ID, entity2.ID, entity3.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())

	ids, err := SearchIDs[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	ids, err = SearchIDs[testFakeDeleteEntity](orm, NewWhere("1").WithFakeDeletes(), nil)
	assert.NoError(t, err)
	assert.Len(t, ids, 3)

	row, found, err := SearchOne[testFakeDeleteEntity](orm, NewWhere("`Code` = ?", "b"))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, row)

	row, found, err = SearchOne[testFakeDeleteEntity](orm, NewWhere("`Code` = ?", "b").WithFakeDeletes())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, row)
	assert.True(t, row.FakeDelete)

	rows, err = GetByIndex[testFakeDeleteEntity](orm, nil, testFakeDeleteEntityIndexes.Name, "b")
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())

	row, found, err = GetByUniqueIndex[testFakeDeleteEntity](orm, testFakeDeleteEntityIndexes.Code, "b")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, row)

	rows, err = GetAll[testFakeDeleteEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())

	rows, err = GetByIndex[testFakeDeleteEntity](orm, nil, testFakeDeleteEntityIndexes.AgeCached, 3)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())

	_, found, err = GetByUniqueIndex[testFakeDeleteEntity](orm, testFakeDeleteEntityIndexes.CodeCached, "b")
	assert.NoError(t, err)
	assert.False(t, found)

	row, _, err = GetByID[testFakeDeleteEntity](orm, entity3.ID)
	assert.NoError(t, err)
	row, err = EditEntity(orm, row)
	assert.NoError(t, err)
	row.FakeDelete = false
	assert.NoError(t, orm.Flush())
	rowsFromRS, totalInRS, err = RedisSearch[testFakeDeleteEntity](orm, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, totalInRS)
	assert.Equal(t, 2, rowsFromRS.Len())
	all, err = rowsFromRS.All()
	assert.NoError(t, err)
	assert.Equal(t, entity.ID, all[0].ID)
	assert.Equal(t, entity3.ID, all[1].ID)
	rows, err = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, rows.Len())
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, entity.ID, all[0].ID)
	assert.Equal(t, entity3.ID, all[1].ID)

	row, err = EditEntity(orm, row)
	assert.NoError(t, err)
	row.FakeDelete = true
	assert.NoError(t, orm.Flush())
	rowsFromRS, totalInRS, err = RedisSearch[testFakeDeleteEntity](orm, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, totalInRS)
	assert.Equal(t, 1, rowsFromRS.Len())
	all, err = rowsFromRS.All()
	assert.NoError(t, err)
	assert.Equal(t, entity.ID, all[0].ID)
	rows, err = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, entity.ID, all[0].ID)
}
