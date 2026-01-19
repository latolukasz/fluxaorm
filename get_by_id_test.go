package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIDEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
}

func TestGetByIdNoCache(t *testing.T) {
	testGetByID(t, false, false)
}

func TestGetByIdLocalCache(t *testing.T) {
	testGetByID(t, true, false)
}

func TestGetByIdRedisCache(t *testing.T) {
	testGetByID(t, false, true)
}

func TestGetByIdLocalRedisCache(t *testing.T) {
	testGetByID(t, true, true)
}

func testGetByID(t *testing.T, local, redis bool) {
	var entity *getByIDEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema := GetEntitySchema[getByIDEntity](orm)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity = NewEntity[getByIDEntity](orm)
		entity.Name = fmt.Sprintf("Name %d", i)
		ids = append(ids, entity.ID)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)
	loggerRedis := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerRedis, false, true, false)
	loggerLocal := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerLocal, false, false, false)
	entity, found, err := GetByID[getByIDEntity](orm, ids[0])
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, "Name 0", entity.Name)
	if !local && !redis {
		assert.Len(t, loggerDB.Logs, 1)
	}
	loggerDB.Clear()
	if local {
		lc, _ := schema.GetLocalCache()
		lc.Clear(orm)
	}
	if redis {
		rc, _ := schema.GetRedisCache()
		err = rc.FlushDB(orm)
		assert.NoError(t, err)
	}
	e2, found, err := schema.GetByID(orm, ids[0])
	assert.NoError(t, err)
	entity = e2.(*getByIDEntity)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, "Name 0", entity.Name)

	loggerDB.Clear()
	if local || redis {
		entity, found, err = GetByID[getByIDEntity](orm, ids[0])
		assert.NoError(t, err)
		assert.True(t, found)
		assert.NotNil(t, entity)
		assert.Equal(t, "Name 0", entity.Name)
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// invalid id
	entity, found, err = GetByID[getByIDEntity](orm, 1)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, entity)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()
	if local || redis {
		entity, found, err = GetByID[getByIDEntity](orm, 1)
		assert.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, entity)
		assert.Len(t, loggerDB.Logs, 0)
	}
}
