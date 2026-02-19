package fluxaorm

import (
	"github.com/puzpuzpuz/xsync/v2"
)

func (orm *ormImplementation) Flush() error {
	return orm.flushGenerated(false)
}

func (orm *ormImplementation) FlushAsync() error {
	return nil
}

func (orm *ormImplementation) flushGenerated(async bool) (err error) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedGeneratedEntities == nil || orm.trackedGeneratedEntities.Size() == 0 {
		return nil
	}
	orm.trackedGeneratedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Flushable]) bool {
		value.Range(func(_ uint64, f Flushable) bool {
			err = f.PrivateFlush()
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
	orm.trackedGeneratedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Flushable]) bool {
		value.Range(func(_ uint64, f Flushable) bool {
			f.PrivateFlushed()
			return true
		})
		return true
	})
	orm.trackedGeneratedEntities.Clear()
	return nil
}

func (orm *ormImplementation) ClearFlush() {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	orm.trackedGeneratedEntities.Clear()
	orm.redisPipeLines = nil
	orm.dbPipeLines = nil
}
