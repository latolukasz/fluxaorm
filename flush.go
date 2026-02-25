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
	orm.trackedEntities.Clear()
	orm.redisPipeLines = nil
	orm.dbPipeLines = nil
}
