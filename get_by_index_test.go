package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type getByIndexEntity struct {
	ID    uint64     `orm:"localCache;redisCache"`
	Name  string     `orm:"index=Name"`
	Age   uint32     `orm:"index=Age;unique=Fake;cached"`
	Born  *time.Time `orm:"index=Age:2;unique=Fake:2;cached"`
	Other int        `orm:"unique=Fake:3"`
}

func TestGetByIndexNoCache(t *testing.T) {
	testGetByIndex(t, false, false)
}

func TestGetByIndexLocalCache(t *testing.T) {
	testGetByIndex(t, true, false)
}

func TestGetByIndexRedisCache(t *testing.T) {
	testGetByIndex(t, false, true)
}

func TestGetByIndexLocalRedisCache(t *testing.T) {
	testGetByIndex(t, true, true)
}

func testGetByIndex(t *testing.T, local, redis bool) {
	var entity *getByIndexEntity
	orm := PrepareTables(t, NewRegistry(), entity)
	schema, err := GetEntitySchema[getByIndexEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	now := time.Now().UTC()
	nextWeek := now.Add(time.Hour * 24 * 7)
	// getting missing rows
	rows, err := GetByIndex[getByIndexEntity](orm, NewPager(1, 100), "Age", 23, now)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 23, now)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	assert.Len(t, loggerDB.Logs, 0)

	var entities []*getByIndexEntity
	for i := 0; i < 10; i++ {
		entity, err = NewEntity[getByIndexEntity](orm)
		assert.NoError(t, err)
		entity.Age = 10
		entity.Other = i
		if i >= 5 {
			entity.Name = "Test Name"
			entity.Age = 18
			entity.Born = &now
		}
		if i >= 8 {
			entity.Name = "Test Name 2"
			entity.Age = 40
			entity.Born = &nextWeek
		}
		entities = append(entities, entity)
	}
	assert.NoError(t, orm.Flush())

	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(1, 5), "Name", nil)
	assert.NoError(t, err)
	assert.Equal(t, 5, rows.Len())
	rows.Next()
	e, err := rows.Entity()
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)

	rows, total, err := GetByIndexWithCount[getByIndexEntity](orm, nil, "Name", nil)
	assert.NoError(t, err)
	assert.Equal(t, 5, rows.Len())
	assert.Equal(t, 5, total)

	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Name", "Test name")
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[5].ID, e.ID)

	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(1, 100), "Age", 10, nil)
	assert.NoError(t, err)
	assert.Equal(t, 5, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, entities[0].ID, e.ID)
	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(1, 100), "Age", 10, nil)
	assert.NoError(t, err)
	assert.Equal(t, 5, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 18, now)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	rows.Next()
	e, err = rows.Entity()
	assert.NoError(t, err)
	loggerDB.Clear()
	assert.Equal(t, entities[5].ID, e.ID)
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 18, now)
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	loggerDB.Clear()

	entity, err = NewEntity[getByIndexEntity](orm)
	assert.NoError(t, err)
	entity.Age = 10
	assert.NoError(t, orm.Flush())
	entities = append(entities, entity)

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 10, nil)
	assert.NoError(t, err)
	all, err := rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 6, rows.Len())
	assert.Equal(t, all[5].ID, entity.ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	err = DeleteEntity(orm, entities[0])
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 10, nil)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 5, rows.Len())
	assert.Equal(t, all[0].ID, entities[1].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	entity, err = EditEntity(orm, entities[6])
	assert.NoError(t, err)
	entity.Name = ""
	entity.Age = 40
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Name", nil)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 6, rows.Len())
	assert.Equal(t, all[4].ID, entities[6].ID)

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(1, 100), "Name", "Test name")
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, all[0].ID, entities[5].ID)

	// Pager
	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(1, 1), "Name", "Test name")
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[5].ID)

	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(2, 1), "Name", "Test name")
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[7].ID)

	rows, err = GetByIndex[getByIndexEntity](orm, NewPager(3, 1), "Name", "Test name")
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())

	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 40, now)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[6].ID)
	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 40, now)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.Equal(t, 1, rows.Len())
	assert.Equal(t, all[0].ID, entities[6].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 18, now)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 2, rows.Len())
	assert.Equal(t, all[0].ID, entities[5].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	assert.NoError(t, EditEntityField(orm, entity, "Age", 18))
	assert.NoError(t, orm.Flush())

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 18, now)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 3, rows.Len())
	assert.Equal(t, all[1].ID, entities[6].ID)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}

	loggerDB.Clear()
	rows, err = GetByIndex[getByIndexEntity](orm, nil, "Age", 40, now)
	assert.NoError(t, err)
	all, err = rows.All()
	assert.NoError(t, err)
	assert.Equal(t, 0, rows.Len())
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
}
