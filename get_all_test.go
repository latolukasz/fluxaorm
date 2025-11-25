package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByAllCachedEntity struct {
	ID   uint64 `orm:"localCache;redisCache;cacheAll"`
	Name string
}

type getByAllNotCachedEntity struct {
	ID   uint64 `orm:"localCache;redisCache;"`
	Name string
}

func TestGetAllNoCache(t *testing.T) {
	testGetAll(t, false, false)
}

func TestGetAllLocalCache(t *testing.T) {
	testGetAll(t, true, false)
}

func TestGetAllRedisCache(t *testing.T) {
	testGetAll(t, false, true)
}

func TestGetAllLocalRedisCache(t *testing.T) {
	testGetAll(t, true, true)
}

func testGetAll(t *testing.T, local, redis bool) {
	var entity *getByAllCachedEntity
	var entityNotCached *getByAllNotCachedEntity
	orm := PrepareTables(t, NewRegistry(), entity, entityNotCached)
	schema, found := GetEntitySchema[getByAllCachedEntity](orm)
	assert.True(t, found)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	// getting missing rows
	rows, err := GetAll[getByAllCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows, err = GetAll[getByAllCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)
	loggerDB.Clear()
	rows2, err := GetAll[getByAllNotCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows2.Len())
	loggerDB.Clear()

	var entities []*getByAllCachedEntity
	var entitiesNoCache []*getByAllNotCachedEntity
	for i := 0; i < 10; i++ {
		entity, err = NewEntity[getByAllCachedEntity](orm)
		assert.NoError(t, err)
		entity.Name = fmt.Sprintf("Name %d", i)
		entities = append(entities, entity)

		entityNotCached, err = NewEntity[getByAllNotCachedEntity](orm)
		assert.NoError(t, err)
		entityNotCached.Name = fmt.Sprintf("Name %d", i)
		entitiesNoCache = append(entitiesNoCache, entityNotCached)
	}
	err = orm.Flush()
	assert.NoError(t, err)

	loggerDB.Clear()
	rows, err = GetAll[getByAllCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	rows.Next()
	e, err := rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	assert.Equal(t, entities[0].Name, e.Name)

	loggerDB.Clear()
	rows2, err = GetAll[getByAllNotCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows2.Len())
	rows2.Next()
	e2, err := rows2.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entitiesNoCache[0].ID, e2.ID)
	assert.Equal(t, entitiesNoCache[0].Name, e2.Name)
	assert.Len(t, loggerDB.Logs, 1)

	loggerDB.Clear()
	rows, err = GetAll[getByAllCachedEntity](orm)
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

	err = DeleteEntity(orm, entities[7])
	assert.NoError(t, err)
	err = DeleteEntity(orm, entitiesNoCache[7])
	assert.NoError(t, err)
	err = orm.Flush()
	assert.NoError(t, err)
	loggerDB.Clear()
	rows, err = GetAll[getByAllCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 9, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	rows2, err = GetAll[getByAllNotCachedEntity](orm)
	assert.NoError(t, err)
	assert.Equal(t, 9, rows2.Len())
}
