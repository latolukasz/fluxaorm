package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testFakeDeleteEntity struct {
	ID         uint64 `orm:"redisSearch"`
	Name       string `orm:"index=Name;searchable"`
	Code       string `orm:"unique=Code"`
	AgeCached  int    `orm:"index=AgeCached;cached"`
	Ref        Reference[testFakeDeleteEntityReference]
	FakeDelete bool
}

type testFakeDeleteEntityReference struct {
	ID   uint32
	Name string
}

func TestFakeDelete(t *testing.T) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, &testFakeDeleteEntity{}, &testFakeDeleteEntityReference{})

	entity := NewEntity[testFakeDeleteEntity](orm)
	entity.Name = "a"
	entity.Code = "a"
	entity.AgeCached = 1
	entity2 := NewEntity[testFakeDeleteEntity](orm)
	entity2.Name = "b"
	entity2.Code = "b"
	entity2.AgeCached = 2
	entity3 := NewEntity[testFakeDeleteEntity](orm)
	entity3.Name = "c"
	entity3.Code = "c"
	entity3.AgeCached = 3
	ref := NewEntity[testFakeDeleteEntityReference](orm)
	ref.Name = "ref"
	entity3.Ref = Reference[testFakeDeleteEntityReference](ref.ID)
	err := orm.FlushWithCheck()
	assert.NoError(t, err)

	orm.DeleteEntity(entity2)
	assert.NoError(t, orm.FlushWithCheck())

	rows := Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, entity.ID, rows.All()[0].ID)
	assert.Equal(t, entity3.ID, rows.All()[1].ID)

	rowsFromRS, totalInRS := RedisSearch[testFakeDeleteEntity](orm, "*", nil)
	assert.Equal(t, 2, totalInRS)
	assert.Equal(t, 2, rowsFromRS.Len())

	rows = Search[testFakeDeleteEntity](orm, NewWhere("1").WithFakeDeletes(), nil)
	assert.Equal(t, 3, rows.Len())

	DeleteEntity(orm, entity3)
	assert.NoError(t, orm.FlushWithCheck())

	rows = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, entity.ID, rows.All()[0].ID)

	rows = GetByIDs[testFakeDeleteEntity](orm, entity.ID, entity2.ID, entity3.ID)
	assert.Equal(t, 3, rows.Len())

	ids := SearchIDs[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.Len(t, ids, 1)
	ids = SearchIDs[testFakeDeleteEntity](orm, NewWhere("1").WithFakeDeletes(), nil)
	assert.Len(t, ids, 3)

	row, found := SearchOne[testFakeDeleteEntity](orm, NewWhere("`Code` = ?", "b"))
	assert.False(t, found)
	assert.Nil(t, row)

	row, found = SearchOne[testFakeDeleteEntity](orm, NewWhere("`Code` = ?", "b").WithFakeDeletes())
	assert.True(t, found)
	assert.NotNil(t, row)
	assert.True(t, row.FakeDelete)

	rows = GetByIndex[testFakeDeleteEntity](orm, nil, "Name", "b")
	assert.Equal(t, 0, rows.Len())

	row, found = GetByUniqueIndex[testFakeDeleteEntity](orm, "Code", "b")
	assert.False(t, found)
	assert.Nil(t, row)

	rows = GetByReference[testFakeDeleteEntity](orm, nil, "Ref", ref.ID)
	assert.Equal(t, 0, rows.Len())

	rows = GetAll[testFakeDeleteEntity](orm)
	assert.Equal(t, 1, rows.Len())

	rows = GetByIndex[testFakeDeleteEntity](orm, nil, "AgeCached", 3)
	assert.Equal(t, 0, rows.Len())

	row = MustByID[testFakeDeleteEntity](orm, entity3.ID)
	row = EditEntity(orm, row)
	row.FakeDelete = false
	assert.NoError(t, orm.FlushWithCheck())
	rowsFromRS, totalInRS = RedisSearch[testFakeDeleteEntity](orm, "*", nil)
	assert.Equal(t, 2, totalInRS)
	assert.Equal(t, 2, rowsFromRS.Len())
	assert.Equal(t, entity.ID, rowsFromRS.All()[0].ID)
	assert.Equal(t, entity3.ID, rowsFromRS.All()[1].ID)
	rows = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, entity.ID, rows.All()[0].ID)
	assert.Equal(t, entity3.ID, rows.All()[1].ID)

	row = EditEntity(orm, row)
	row.FakeDelete = true
	assert.NoError(t, orm.FlushWithCheck())
	rowsFromRS, totalInRS = RedisSearch[testFakeDeleteEntity](orm, "*", nil)
	assert.Equal(t, 1, totalInRS)
	assert.Equal(t, 1, rowsFromRS.Len())
	assert.Equal(t, entity.ID, rowsFromRS.All()[0].ID)
	rows = Search[testFakeDeleteEntity](orm, NewWhere("1"), nil)
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, entity.ID, rows.All()[0].ID)
}
