package fluxaorm

import (
	"github.com/puzpuzpuz/xsync/v2"
)

func (orm *ormImplementation) Flush() error {
	return orm.flush()
}

func (orm *ormImplementation) flush() (err error) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Entity]) bool {
		value.Range(func(_ uint64, e Entity) bool {
			err = e.PrivateFlush()
			if err != nil {
				return false
			}
			return true
		})
		return true
	})
	if err != nil {
		return err
	}
	for _, dbPipeline := range orm.dbPipeLines {
		err = dbPipeline.Exec(orm)
		if err != nil {
			return err
		}
	}
	for _, redisPipeline := range orm.redisPipeLines {
		_, err = redisPipeline.Exec(orm)
		if err != nil {
			return err
		}
	}
	if len(orm.engine.registry.dirtyPublishers) > 0 {
		orm.trackedEntities.Range(func(cacheIndex uint64, value *xsync.MapOf[uint64, Entity]) bool {
			publisher, hasPub := orm.engine.registry.dirtyPublishers[cacheIndex]
			if !hasPub {
				return true
			}
			value.Range(func(_ uint64, e Entity) bool {
				eventType, changes := e.PrivateFlushEvent()
				if eventType == 0 {
					return true
				}
				err = publishDirtyEvent(orm, publisher, e, dirtyOpFromFlushType(eventType), changes)
				return err == nil
			})
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	if orm.engine.afterInsertHandlers != nil || orm.engine.afterUpdateHandlers != nil || orm.engine.afterDeleteHandlers != nil {
		orm.trackedEntities.Range(func(cacheIndex uint64, value *xsync.MapOf[uint64, Entity]) bool {
			value.Range(func(_ uint64, e Entity) bool {
				eventType, changes := e.PrivateFlushEvent()
				switch eventType {
				case 1:
					if handler, ok := orm.engine.afterInsertHandlers[cacheIndex]; ok {
						err = handler(orm, e)
					}
				case 2:
					if handler, ok := orm.engine.afterUpdateHandlers[cacheIndex]; ok {
						err = handler(orm, e, changes)
					}
				case 3:
					if handler, ok := orm.engine.afterDeleteHandlers[cacheIndex]; ok {
						err = handler(orm, e)
					}
				}
				return err == nil
			})
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Entity]) bool {
		value.Range(func(_ uint64, e Entity) bool {
			e.PrivateFlushed()
			return true
		})
		return true
	})
	orm.trackedEntities.Clear()
	return nil
}

func (orm *ormImplementation) ClearFlush() {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities != nil {
		orm.trackedEntities.Clear()
	}
	orm.redisPipeLines = nil
	orm.dbPipeLines = nil
}
