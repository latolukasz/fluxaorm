package fluxaorm

import (
	"testing"

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
	consumerAll := NewDirtyStreamConsumer(orm, "All", func(event *DirtyStreamEvent) {
		if processed == 0 {
			assert.Equal(t, "fluxaorm.dirtyEntity", event.EntityName)
			assert.Equal(t, entity.ID, event.ID)
			assert.Equal(t, Insert, event.Operation)
			assert.Len(t, event.Bind, 7)
		} else if processed == 1 {
			assert.Equal(t, "fluxaorm.dirtyEntity2", event.EntityName)
			assert.Equal(t, entity2.ID, event.ID)
			assert.Equal(t, Insert, event.Operation)
			assert.Len(t, event.Bind, 3)
		}
		processed++
	})
	consumerAll.DisableBlockMode()
	consumerAll.Digest(100)
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
	consumerAll = NewDirtyStreamConsumer(orm, "All", func(event *DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", event.EntityName)
		assert.Equal(t, entity.ID, event.ID)
		assert.Equal(t, Update, event.Operation)
		assert.Len(t, event.Bind, 2)
		processed++
	})
	consumerAll.DisableBlockMode()
	consumerAll.Digest(100)
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
	consumerAll = NewDirtyStreamConsumer(orm, "All", func(event *DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity2", event.EntityName)
		assert.Equal(t, entity2.ID, event.ID)
		assert.Equal(t, Delete, event.Operation)
		assert.Len(t, event.Bind, 3)
		processed++
	})
	consumerAll.DisableBlockMode()
	consumerAll.Digest(100)
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
	consumerAll = NewDirtyStreamConsumer(orm, "All", func(event *DirtyStreamEvent) {
		assert.Equal(t, "fluxaorm.dirtyEntity", event.EntityName)
		assert.Equal(t, entity.ID, event.ID)
		assert.Equal(t, Delete, event.Operation)
		assert.Len(t, event.Bind, 1)
		processed++
	})
	consumerAll.DisableBlockMode()
	consumerAll.Digest(100)
	assert.Equal(t, uint64(0), orm.GetEventBroker().GetStreamStatistics("dirty_All").Len)
	assert.Equal(t, 1, processed)
}
