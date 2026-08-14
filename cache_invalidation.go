package fluxaorm

import "time"

// EntityCacheTTL bounds how long a cached entity row may disagree with MySQL if
// an invalidation is ever lost. Correctness comes from the invalidation; this is
// only the backstop.
const EntityCacheTTL = time.Hour

// InvalidateCacheKey records a redis key that the pending write makes stale.
// Generated write code registers the keys; Save owns the timing - it deletes
// them once before the statement runs and once after the transaction commits.
// The second delete closes the window where a concurrent read repopulated the
// key from a pre-commit snapshot.
func (orm *ormImplementation) InvalidateCacheKey(pool, key string) {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.pendingInvalidations == nil {
		orm.pendingInvalidations = make(map[string]map[string]bool)
	}
	keys := orm.pendingInvalidations[pool]
	if keys == nil {
		keys = make(map[string]bool)
		orm.pendingInvalidations[pool] = keys
	}
	keys[key] = true
}

// takePendingInvalidations returns the registered keys per pool and clears the
// registry, so a later Save does not re-delete another write's keys.
func (orm *ormImplementation) takePendingInvalidations() map[string][]string {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if len(orm.pendingInvalidations) == 0 {
		return nil
	}
	out := make(map[string][]string, len(orm.pendingInvalidations))
	for pool, keys := range orm.pendingInvalidations {
		list := make([]string, 0, len(keys))
		for key := range keys {
			list = append(list, key)
		}
		out[pool] = list
	}
	orm.pendingInvalidations = nil
	return out
}

func (orm *ormImplementation) discardPendingInvalidations() {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	orm.pendingInvalidations = nil
}

func (orm *ormImplementation) deleteCacheKeys(keys map[string][]string) error {
	for pool, list := range keys {
		if len(list) == 0 {
			continue
		}
		if err := orm.engine.Redis(pool).Del(orm, list...); err != nil {
			return err
		}
	}
	return nil
}
