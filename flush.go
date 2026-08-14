package fluxaorm

// Flush writes every entity tracked on this context. It is the original unit of
// work API, kept working by routing through the same path as Save, so the two
// cannot drift apart.
func (orm *ormImplementation) Flush() error {
	tracked := orm.trackedSnapshot()
	if len(tracked) == 0 {
		return nil
	}
	if orm.tx != nil || len(tracked) == 1 {
		return orm.writeEntities(tracked)
	}
	return orm.Transaction(func(Context) error {
		return orm.writeEntities(tracked)
	})
}

func (orm *ormImplementation) trackedSnapshot() []*pendingWrite {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}
	var out []*pendingWrite
	orm.trackedEntities.Range(func(cacheIndex string, entities *xsyncEntityMap) bool {
		entities.Range(func(_ uint64, e Entity) bool {
			out = append(out, &pendingWrite{entity: e, cacheIndex: cacheIndex})
			return true
		})
		return true
	})
	return out
}

func (orm *ormImplementation) ClearFlush() {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities != nil {
		orm.trackedEntities.Clear()
	}
	orm.redisPipeLines = nil
	orm.dbPipeLines = nil
	orm.discardPendingInvalidations()
}
