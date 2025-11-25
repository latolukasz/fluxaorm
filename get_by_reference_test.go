package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByReferenceEntity struct {
	ID               uint64 `orm:"localCache;redisCache"`
	Name             string
	Ref              Reference[getByReferenceReference]        `orm:"index=Ref"`
	RefCached        Reference[getByReferenceReference]        `orm:"index=RefCached;cached"`
	RefCachedNoCache Reference[getByReferenceReferenceNoCache] `orm:"index=RefCachedNoCache;cached"`
}

type getByReferenceReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

type getByReferenceReferenceNoCache struct {
	ID   uint64
	Name string
}

func TestGetByReferenceNoCache(t *testing.T) {
	testGetByReference(t, false, false)
}

func TestGetByReferenceLocalCache(t *testing.T) {
	testGetByReference(t, true, false)
}

func TestGetByReferenceRedisCache(t *testing.T) {
	testGetByReference(t, false, true)
}

func TestGetByReferenceLocalRedisCache(t *testing.T) {
	testGetByReference(t, true, true)
}

func testGetByReference(t *testing.T, local, redis bool) {
	var entity *getByReferenceEntity
	orm := PrepareTables(t, NewRegistry(), entity, getByReferenceReference{}, getByReferenceReferenceNoCache{})
	schema, found := GetEntitySchema[getByReferenceEntity](orm)
	assert.True(t, found)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	// getting missing rows
	rows, err := GetByReference[getByReferenceEntity](orm, nil, "RefCached", 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)
	loggerDB.Clear()

	var entities []*getByReferenceEntity
	ref, err := NewEntity[getByReferenceReference](orm)
	assert.NoError(t, err)
	ref.Name = "Ref 1"
	ref2, err := NewEntity[getByReferenceReference](orm)
	assert.NoError(t, err)
	ref2.Name = "Ref 2"
	refNoCache, err := NewEntity[getByReferenceReferenceNoCache](orm)
	assert.NoError(t, err)
	refNoCache.Name = "Ref 1"
	refNoCache2, err := NewEntity[getByReferenceReferenceNoCache](orm)
	assert.NoError(t, err)
	refNoCache2.Name = "Ref 2"
	for i := 0; i < 10; i++ {
		entity, err = NewEntity[getByReferenceEntity](orm)
		assert.NoError(t, err)
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Ref = Reference[getByReferenceReference](ref.ID)
		entity.RefCached = Reference[getByReferenceReference](ref.ID)
		entity.RefCachedNoCache = Reference[getByReferenceReferenceNoCache](refNoCache.ID)
		entities = append(entities, entity)
	}
	err = orm.Flush()
	assert.NoError(t, err)

	loggerDB.Clear()
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "Ref", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e, err := rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)

	rows, err = GetByReference[getByReferenceEntity](orm, nil, "Ref", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())

	rows, total, err := GetByReferenceWithCount[getByReferenceEntity](orm, nil, "Ref", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	assert.Equal(t, 10, total)

	rows, total, err = GetByReferenceWithCount[getByReferenceEntity](orm, NewPager(0, 0), "Ref", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	assert.Equal(t, 10, total)

	loggerDB.Clear()
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	rows, err = GetByReference[getByReferenceEntity](orm, NewPager(1, 100), "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	rows, err = GetByReference[getByReferenceEntity](orm, NewPager(1, 3), "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	rows, err = GetByReference[getByReferenceEntity](orm, NewPager(100, 2), "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())

	loggerDB.Clear()

	rows2, err := GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e, err = rows2.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	rows2, err = GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache.ID)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e, err = rows2.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	// Update set to nil
	entity, err = EditEntity(orm, e)
	assert.NoError(t, err)
	entity.Ref = 0
	entity.RefCached = 0
	entity.RefCachedNoCache = 0
	err = orm.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()

	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 9, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[1].ID, e.ID)
	assert.Equal(t, entities[1].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows2, err = GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache.ID)
	assert.NoError(t, err)
	assert.Equal(t, 9, rows2.Len())
	assert.NoError(t, err)
	rows2.Next()
	e, err = rows2.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[1].ID, e.ID)
	assert.Equal(t, entities[1].Name, e.Name)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// update change id
	entity, err = EditEntity(orm, entities[3])
	assert.NoError(t, err)
	entity.Ref = Reference[getByReferenceReference](ref2.ID)
	entity.RefCached = Reference[getByReferenceReference](ref2.ID)
	entity.RefCachedNoCache = Reference[getByReferenceReferenceNoCache](refNoCache2.ID)
	err = orm.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()

	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 8, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref2.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, "Name 3", e.Name)

	rows2, err = GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache.ID)
	assert.NoError(t, err)
	assert.Equal(t, 8, rows2.Len())

	rows2, err = GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache2.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows2.Len())
	rows2.Next()
	e, err = rows2.Entity()
	assert.NoError(t, err)
	assert.Equal(t, "Name 3", e.Name)

	err = DeleteEntity(orm, entities[7])
	assert.NoError(t, err)
	err = orm.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 7, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows2, err = GetByReference[getByReferenceEntity](orm, nil, "RefCachedNoCache", refNoCache.ID)
	assert.NoError(t, err)
	assert.Equal(t, 7, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	err = EditEntityField(orm, entities[0], "RefCached", ref2)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref2.ID)
	assert.NoError(t, err)
	assert.Equal(t, 2, rows.Len())
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.Equal(t, 7, rows.Len())
	err = EditEntityField(orm, entities[0], "RefCached", ref)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref2.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	rows, err = GetByReference[getByReferenceEntity](orm, nil, "RefCached", ref.ID)
	assert.NoError(t, err)
	assert.Equal(t, 8, rows.Len())
}
