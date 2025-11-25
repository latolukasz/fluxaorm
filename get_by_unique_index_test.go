package fluxaorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type getByUniqueIndexEntity struct {
	ID        uint64                               `orm:"localCache;redisCache"`
	Name      string                               `orm:"unique=Name;cached"`
	Age       uint8                                `orm:"unique=Multi;cached"`
	Active    bool                                 `orm:"unique=Multi:2;cached"`
	Ref       Reference[getByUniqueIndexReference] `orm:"unique=Ref"`
	BirthDate time.Time                            `orm:"time;unique=Time"`
	Died      bool                                 `orm:"time;unique=Died"`
	DeathDate time.Time                            `orm:"unique=Died:2"`
	Price     float32                              `orm:"unique=Price"`
}

type getByUniqueIndexReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string
}

func TestGetByUniqueIndexNoCache(t *testing.T) {
	testGetByUniqueIndex(t, false, false)
}

func TestGetByUniqueIndexLocalCache(t *testing.T) {
	testGetByUniqueIndex(t, true, false)
}

func TestGetByUniqueIndexRedisCache(t *testing.T) {
	testGetByUniqueIndex(t, false, true)
}

func TestGetByUniqueIndexLocalRedisCache(t *testing.T) {
	testGetByUniqueIndex(t, true, true)
}

func testGetByUniqueIndex(t *testing.T, local, redis bool) {
	var entity *getByUniqueIndexEntity
	orm := PrepareTables(t, NewRegistry(), entity, getByUniqueIndexReference{})
	schema, found := GetEntitySchema[getByUniqueIndexEntity](orm)
	assert.True(t, found)
	schema.DisableCache(!local, !redis)

	var entities []*getByUniqueIndexEntity
	var refs []*getByUniqueIndexReference
	date := time.Now().UTC()
	died := time.Now().UTC()
	for i := 0; i < 10; i++ {
		ref, err := NewEntity[getByUniqueIndexReference](orm)
		assert.NoError(t, err)
		ref.Name = fmt.Sprintf("Ref %d", i)
		entity, err = NewEntity[getByUniqueIndexEntity](orm)
		assert.NoError(t, err)
		entity.Name = fmt.Sprintf("Name %d", i)
		entity.Age = uint8(i)
		entity.Ref = Reference[getByUniqueIndexReference](ref.ID)
		date = date.Add(time.Hour)
		died = died.Add(time.Hour * 24)
		entity.BirthDate = date
		entity.DeathDate = died
		if i > 5 {
			entity.Died = true
		}
		entity.Price = float32(i)
		entities = append(entities, entity)
		refs = append(refs, ref)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "Name 3")
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entity.Name)

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "Missing")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, entity)

	entity, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", time.Now())
	assert.EqualError(t, err, "[Name] invalid value")

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", 4, false)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", 4, 0)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entity.Name)

	numbers := []any{uint8(4), uint16(4), uint32(4), uint(4), "4", int8(4), int16(4), int32(4), int64(4)}
	for _, number := range numbers {
		entity, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", number, 0)
		assert.NoError(t, err)
		assert.Equal(t, "Name 4", entity.Name)
	}

	negativeNumbers := []any{int8(-4), int16(-4), int32(-4), -4, int8(-4), int16(-4), int32(-4), int64(-4)}
	for _, number := range negativeNumbers {
		entity, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", number, 0)
		assert.EqualError(t, err, "[Age] negative number -4 not allowed")
	}

	entity, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", "invalid", 0)
	assert.EqualError(t, err, "[Age] invalid number invalid")

	entity, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Multi", time.Now(), 0)
	assert.EqualError(t, err, "[Age] invalid value")

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Ref", refs[4].ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[4].ID, entity.ID)
	assert.Equal(t, "Name 4", entities[4].Name)

	date = date.Add(time.Hour * -3)
	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Time", date)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", true, died)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", "true", died)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", 1, died)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[6].ID, entity.ID)
	assert.Equal(t, "Name 6", entities[6].Name)

	died = died.Add(time.Hour * -72)
	entity, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", "false", died)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, entities[3].ID, entity.ID)
	assert.Equal(t, "Name 3", entities[3].Name)

	_, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "a", "b")
	assert.EqualError(t, err, "invalid number of index `Name` attributes, got 2, 1 expected")

	_, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Invalid")
	assert.EqualError(t, err, "unknown index name `Invalid`")

	_, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", nil)
	assert.EqualError(t, err, "nil attribute for index name `Name` is not allowed")

	_, _, err = GetByUniqueIndex[time.Time](orm, "Name", nil)
	assert.EqualError(t, err, "entity 'time.Time' is not registered")

	_, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Time", 23)
	assert.EqualError(t, err, "[BirthDate] invalid value")

	_, _, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Died", time.Now(), died)
	assert.EqualError(t, err, "[Died] invalid value")

	r, hasRedis := schema.GetRedisCache()
	if !hasRedis {
		r = orm.Engine().Redis(DefaultPoolCode)
	}
	err = r.FlushAll(orm)
	assert.NoError(t, err)
	_, found, err = GetByUniqueIndex[getByUniqueIndexEntity](orm, "Name", "Name 9")
	assert.NoError(t, err)
	assert.True(t, found)
}
