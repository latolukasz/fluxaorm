package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getByIdsEntity struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"max=100"`
}

func TestLoadByIdsNoCache(t *testing.T) {
	testLoadByIds(t, false, false)
}

func TestLoadByIdsLocalCache(t *testing.T) {
	testLoadByIds(t, true, false)
}

func TestLoadByIdsRedisCache(t *testing.T) {
	testLoadByIds(t, false, true)
}

func TestLoadByIdsLocalRedisCache(t *testing.T) {
	testLoadByIds(t, true, true)
}

func testLoadByIds(t *testing.T, local, redis bool) {
	var entity *getByIdsEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema, found := GetEntitySchema[getByIdsEntity](orm)
	assert.True(t, found)
	schema.DisableCache(!local, !redis)

	var ids []uint64
	for i := 0; i < 10; i++ {
		entity, err := NewEntity[getByIdsEntity](orm)
		assert.NoError(t, err)
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
	rows, err := GetByIDs[getByIdsEntity](orm, ids...)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())
	i := 0
	for rows.Next() {
		e, err := rows.Entity()
		assert.NoError(t, err)
		assert.NotNil(t, e)
		assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
		i++
	}
	assert.Equal(t, 10, i)
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

	rows, err = GetByIDs[getByIdsEntity](orm, ids...)
	assert.NoError(t, err)
	assert.Equal(t, 10, rows.Len())

	i = 0
	for rows.Next() {
		e, err := rows.Entity()
		assert.NoError(t, err)
		assert.NotNil(t, e)
		assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
		i++
	}

	loggerDB.Clear()
	if local || redis {
		rows, err = GetByIDs[getByIdsEntity](orm, ids...)
		assert.NoError(t, err)
		assert.Equal(t, 10, rows.Len())
		i = 0
		for rows.Next() {
			e, err := rows.Entity()
			assert.NoError(t, err)
			assert.NotNil(t, e)
			assert.Equal(t, fmt.Sprintf("Name %d", i), e.Name)
			i++
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	// invalid ids
	rows, err = GetByIDs[getByIdsEntity](orm, 7777, 8888, 9999)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	i = 0
	for rows.Next() {
		e, err := rows.Entity()
		assert.NoError(t, err)
		assert.Nil(t, e)
		i++
	}
	assert.Equal(t, 3, i)
	if !local {
		assert.Len(t, loggerDB.Logs, 1)
	}
	loggerDB.Clear()
	if local || redis {
		rows, err = GetByIDs[getByIdsEntity](orm, 7777, 8888, 9999)
		assert.NoError(t, err)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			e, err := rows.Entity()
			assert.NoError(t, err)
			assert.Nil(t, e)
		}
		assert.Len(t, loggerDB.Logs, 0)
	}
	if local && redis {
		lc, _ := schema.GetLocalCache()
		lc.Clear(orm)
		loggerDB.Clear()
		rows, err = GetByIDs[getByIdsEntity](orm, 7777, 8888, 9999)
		assert.NoError(t, err)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			e, err := rows.Entity()
			assert.NoError(t, err)
			assert.Nil(t, e)
		}
		assert.Len(t, loggerDB.Logs, 0)
		loggerLocal.Clear()
		loggerRedis.Clear()
		rows, err = GetByIDs[getByIdsEntity](orm, 7777, 8888, 9999)
		assert.NoError(t, err)
		assert.Equal(t, 3, rows.Len())
		for rows.Next() {
			e, err := rows.Entity()
			assert.NoError(t, err)
			assert.Nil(t, e)
		}
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerRedis.Logs, 0)
	}

	// missing one
	rows, err = GetByIDs[getByIdsEntity](orm, ids[0], 7777, ids[1])
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	e, err := rows.Entity()
	assert.NoError(t, err)
	assert.NotNil(t, e)
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Nil(t, e)
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.NotNil(t, e)

	// duplicated
	rows, err = GetByIDs[getByIdsEntity](orm, ids[0], ids[0], ids[0])
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	for rows.Next() {
		e, err := rows.Entity()
		assert.NoError(t, err)
		assert.NotNil(t, e)
		assert.Equal(t, ids[0], e.ID)
		assert.Equal(t, "Name 0", e.Name)
	}
}
