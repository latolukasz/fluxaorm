package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dirtySub struct {
	Color string `orm:"dirty=Sub"`
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
	entity.Name = "Test"
	entity.Age = 18
	entity.Balance = 100
	entity.Sub.Color = "red"

	entity2 := NewEntity[dirtyEntity2](orm)
	entity2.Name = "Test2"
	entity2.Age = 19

	assert.NoError(t, orm.FlushWithCheck())

	assert.Equal(t, uint64(2), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance").Len)
	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_Added").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Sub").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Deleted").Len)

	processed := 0
	consumerAll := NewDirtyStreamConsumerSingle(orm, "All", func(events []*DirtyStreamEvent) {
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
	consumerAll.Consume(100, time.Millisecond)
	consumerAll.Cleanup()
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, 2, processed)

	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)

	entity = EditEntity(orm, entity)
	entity.Age = 20
	entity.AgeUpdated = 20
	assert.NoError(t, orm.FlushWithCheck())

	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll").Len)
	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Added").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Sub").Len)
	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Deleted").Len)

	processed = 0
	consumerAll = NewDirtyStreamConsumerSingle(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", events[0].EntityName)
		assert.Equal(t, entity.ID, events[0].ID)
		assert.Equal(t, Update, events[0].Operation)
		assert.Len(t, events[0].Bind, 2)
		processed = len(events)
	})
	consumerAll.Consume(100, time.Millisecond)
	consumerAll.Cleanup()
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, 1, processed)

	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)

	DeleteEntity(orm, entity2)
	assert.NoError(t, orm.FlushWithCheck())

	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Added").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Sub").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Deleted").Len)

	processed = 0
	consumerAll = NewDirtyStreamConsumerMany(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity2", events[0].EntityName)
		assert.Equal(t, entity2.ID, events[0].ID)
		assert.Equal(t, Delete, events[0].Operation)
		assert.Len(t, events[0].Bind, 3)
		processed = len(events)
	})
	consumerAll.Consume(100, time.Millisecond)
	consumerAll.Cleanup()
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, 1, processed)

	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)

	DeleteEntity(orm, entity)
	assert.NoError(t, orm.FlushWithCheck())

	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeAll").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeBalance").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Added").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_Sub").Len)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_AgeUpdated").Len)
	assert.Equal(t, uint64(1), orm.GetEventBroker().GetStreamStatistics("dirty_Deleted").Len)

	processed = 0
	consumerAll = NewDirtyStreamConsumerMany(orm, "All", func(events []*DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", events[0].EntityName)
		assert.Equal(t, entity.ID, events[0].ID)
		assert.Equal(t, Delete, events[0].Operation)
		assert.Len(t, events[0].Bind, 1)
		processed = len(events)
	})
	consumerAll.AutoClaim(100, time.Millisecond)
	consumerAll.Consume(100, time.Millisecond)
	consumerAll.Cleanup()
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, 1, processed)
}
