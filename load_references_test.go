package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadReferenceSub struct {
	Sub1 Reference[loadSubReferenceEntity1]
	Sub2 Reference[loadSubReferenceEntity2]
}

type loadReferenceEntity struct {
	ID        uint64 `orm:"localCache;redisCache"`
	Name      string `orm:"required"`
	Sub       loadReferenceSub
	Ref1a     Reference[loadSubReferenceEntity1]
	Ref1b     Reference[loadSubReferenceEntity1]
	Ref2      Reference[loadSubReferenceEntity2]
	Ref1Array [2]Reference[loadSubReferenceEntity1]
}

type loadSubReferenceEntity1 struct {
	ID      uint64 `orm:"localCache;redisCache"`
	Name    string `orm:"required"`
	SubRef2 Reference[loadSubReferenceEntity2]
}

type loadSubReferenceEntity2 struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestLoadReferencesLocal(t *testing.T) {
	testLoadReferences(t, true, false)
}

func TestLoadReferencesRedis(t *testing.T) {
	testLoadReferences(t, false, true)
}

func testLoadReferences(t *testing.T, local, redis bool) {
	var entity *loadReferenceEntity
	var ref1 *loadSubReferenceEntity1
	var ref2 *loadSubReferenceEntity2
	orm := PrepareTables(t, NewRegistry(), entity, ref1, ref2)
	schema := GetEntitySchema[loadReferenceEntity](orm)
	schema.DisableCache(!local, !redis)
	schema2 := GetEntitySchema[loadSubReferenceEntity1](orm)
	schema3 := GetEntitySchema[loadSubReferenceEntity2](orm)
	schema2.DisableCache(!local, !redis)
	schema3.DisableCache(!local, !redis)

	for i := 1; i <= 10; i++ {
		entity = NewEntity[loadReferenceEntity](orm)
		entity.Name = fmt.Sprintf("Entity %d", i)
		ref1 = NewEntity[loadSubReferenceEntity1](orm)
		ref1.Name = fmt.Sprintf("Ref1 %d", i)
		entity.Ref1a = Reference[loadSubReferenceEntity1](ref1.ID)
		entity.Ref1Array[0] = Reference[loadSubReferenceEntity1](ref1.ID)
		sub1 := NewEntity[loadSubReferenceEntity1](orm)
		sub1.Name = fmt.Sprintf("Sub1 %d", i)
		entity.Sub.Sub1 = Reference[loadSubReferenceEntity1](sub1.ID)
		ref2 = NewEntity[loadSubReferenceEntity2](orm)
		ref2.Name = fmt.Sprintf("Ref2 %d", i)
		entity.Ref2 = Reference[loadSubReferenceEntity2](ref2.ID)
		if i > 5 {
			ref1.SubRef2 = Reference[loadSubReferenceEntity2](ref2.ID)
			ref1 = NewEntity[loadSubReferenceEntity1](orm)
			ref1.Name = fmt.Sprintf("Ref1b %d", i)
			entity.Ref1b = Reference[loadSubReferenceEntity1](ref1.ID)
			entity.Ref1Array[1] = Reference[loadSubReferenceEntity1](ref1.ID)
			sub2 := NewEntity[loadSubReferenceEntity2](orm)
			sub2.Name = fmt.Sprintf("Sub2 %d", i)
			entity.Sub.Sub2 = Reference[loadSubReferenceEntity2](sub2.ID)
		} else {
			sub2 := NewEntity[loadSubReferenceEntity2](orm)
			sub2.Name = fmt.Sprintf("SubSub %d", i)
			ref1.SubRef2 = Reference[loadSubReferenceEntity2](sub2.ID)
		}
	}
	err := orm.Flush()
	assert.NoError(t, err)

	iterator, err := Search[loadReferenceEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, iterator.Len())
	if local {
		schema.(*entitySchema).localCache.Clear(orm)
		schema2 = GetEntitySchema[loadSubReferenceEntity1](orm)
		schema2.(*entitySchema).localCache.Clear(orm)
		schema3 = GetEntitySchema[loadSubReferenceEntity2](orm)
		schema3.(*entitySchema).localCache.Clear(orm)
	}
	if redis {
		err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
		assert.NoError(t, err)
	}
	for iterator.Next() {
		_, err = iterator.Entity()
		assert.NoError(t, err)
	}
	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerLocal, false, false, true)
	loggerRedis := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerRedis, false, true, false)
	err = iterator.LoadReference("Ref1a")
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 0)
	i := 0
	for iterator.Next() {
		entity, err = iterator.Entity()
		assert.NoError(t, err)
		ref1, err = entity.Ref1a.GetEntity(orm)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("Ref1 %d", i+1), ref1.Name)
		i++
	}
	assert.Equal(t, 10, i)
	loggerDB.Clear()
	loggerRedis.Clear()
	loggerLocal.Clear()
	iterator, err = Search[loadReferenceEntity](orm, NewWhere("1"), nil)
	assert.NoError(t, err)
	i = 0
	for iterator.Next() {
		entity, err = iterator.Entity()
		assert.NoError(t, err)
		ref1, err = entity.Ref1a.GetEntity(orm)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("Ref1 %d", i+1), ref1.Name)
		i++
	}
	assert.Len(t, loggerDB.Logs, 1)
	if !local {
		assert.Len(t, loggerRedis.Logs, 10)
		assert.Len(t, loggerLocal.Logs, 0)
	} else {
		assert.Len(t, loggerRedis.Logs, 0)
		assert.Len(t, loggerLocal.Logs, 20)
	}
	assert.Equal(t, 10, i)

}
