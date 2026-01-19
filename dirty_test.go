package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dirtySub struct {
	Color string `orm:"dirty=Sub"`
}

func TestDirtyManualPush(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), dirtyEntity{})
	assert.NotNil(t, orm)

	err := orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)

	entity1 := &dirtyEntity{ID: 1}
	entity2 := &dirtyEntity{ID: 2}

	err = orm.PushDirty(entity1, entity2)
	assert.NoError(t, err)

	stats, err := orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), stats.Len)
}

type dirtyEntity struct {
	ID         uint64 `orm:"dirty=All,Added:add,Deleted:delete"`
	Name       string
	Age        uint8 `orm:"dirty=AgeAll,AgeBalance"`
	Balance    int   `orm:"dirty=AgeBalance"`
	AgeUpdated int   `orm:"dirty=AgeUpdated"`
	Sub        dirtySub
	FakeDelete bool
}

type dirtyEntity2 struct {
	ID   uint64 `orm:"dirty=All"`
	Name string
	Age  uint8 `orm:"dirty=AgeAll"`
}

func TestDirty(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), dirtyEntity{}, dirtyEntity2{})
	assert.NotNil(t, orm)

	streams := orm.Engine().GetRedisStreams()["default"]
	assert.NotNil(t, streams)
	assert.Equal(t, consumerGroupName, streams["dirty_All"])
	assert.Equal(t, consumerGroupName, streams["dirty_AgeAll"])
	assert.Equal(t, consumerGroupName, streams["dirty_AgeBalance"])
	assert.Equal(t, consumerGroupName, streams["dirty_Added"])
	assert.Equal(t, consumerGroupName, streams["dirty_Added"])
	assert.Equal(t, consumerGroupName, streams["dirty_Deleted"])
	assert.Equal(t, consumerGroupName, streams["dirty_Sub"])

	entity := NewEntity[dirtyEntity](orm)
	assert.NotNil(t, entity)
	entity.Name = "Test"
	entity.Age = 18
	entity.Balance = 100
	entity.Sub.Color = "red"

	entity2 := NewEntity[dirtyEntity2](orm)
	assert.NotNil(t, entity2)
	entity2.Name = "Test2"
	entity2.Age = 19

	assert.NoError(t, orm.Flush())

	stats, err := orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Added")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Sub")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Deleted")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)

	processed := 0
	consumerAll, err := NewDirtyStreamConsumerSingle(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", events[0].EntityName)
		assert.Equal(t, entity.ID, events[0].ID)
		assert.Equal(t, Insert, events[0].Operation)
		assert.Len(t, events[0].Bind, 7)

		assert.Equal(t, "fluxaorm.dirtyEntity2", events[1].EntityName)
		assert.Equal(t, entity2.ID, events[1].ID)
		assert.Equal(t, Insert, events[1].Operation)
		assert.Len(t, events[1].Bind, 3)
		processed = len(events)
	})
	assert.NoError(t, err)
	err = consumerAll.Consume(100, time.Millisecond)
	assert.NoError(t, err)
	err = consumerAll.Cleanup()
	assert.NoError(t, err)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	assert.Equal(t, 2, processed)

	err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)

	entity = EditEntity(orm, entity)
	entity.Age = 20
	entity.AgeUpdated = 20
	assert.NoError(t, orm.Flush())

	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Added")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Sub")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Deleted")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)

	processed = 0
	consumerAll, err = NewDirtyStreamConsumerSingle(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", events[0].EntityName)
		assert.Equal(t, entity.ID, events[0].ID)
		assert.Equal(t, Update, events[0].Operation)
		assert.Len(t, events[0].Bind, 2)
		processed = len(events)
	})
	assert.NoError(t, err)
	err = consumerAll.Consume(100, time.Millisecond)
	assert.NoError(t, err)
	err = consumerAll.Cleanup()
	assert.NoError(t, err)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	assert.Equal(t, 1, processed)

	err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)

	DeleteEntity(orm, entity2)
	assert.NoError(t, orm.Flush())

	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Added")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Sub")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Deleted")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)

	processed = 0
	consumerAll, err = NewDirtyStreamConsumerMany(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity2", events[0].EntityName)
		assert.Equal(t, entity2.ID, events[0].ID)
		assert.Equal(t, Delete, events[0].Operation)
		assert.Len(t, events[0].Bind, 3)
		processed = len(events)
	})
	assert.NoError(t, err)
	err = consumerAll.Consume(100, time.Millisecond)
	assert.NoError(t, err)
	err = consumerAll.Cleanup()
	assert.NoError(t, err)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	assert.Equal(t, 1, processed)

	err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)

	DeleteEntity(orm, entity)
	assert.NoError(t, orm.Flush())

	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Added")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Sub")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Sub")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_Deleted")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stats.Len)

	processed = 0
	consumerAll, err = NewDirtyStreamConsumerMany(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", events[0].EntityName)
		assert.Equal(t, entity.ID, events[0].ID)
		assert.Equal(t, Delete, events[0].Operation)
		assert.Len(t, events[0].Bind, 1)
		processed = len(events)
	})
	assert.NoError(t, err)
	err = consumerAll.AutoClaim(100, time.Millisecond)
	assert.NoError(t, err)
	err = consumerAll.Consume(100, time.Millisecond)
	assert.NoError(t, err)
	err = consumerAll.Cleanup()
	assert.NoError(t, err)
	stats, err = orm.GetEventBroker().GetStreamStatistics("dirty_All")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Len)
	assert.Equal(t, 1, processed)
}
