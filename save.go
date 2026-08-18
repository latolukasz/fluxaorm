package fluxaorm

import (
	"fmt"
	"reflect"
)

// Optional capabilities the generator emits. Entity itself is exported, so
// nothing may be added to it without breaking every consumer's generated code.
type entityCacheIndexed interface {
	PrivateCacheIndex() string
}

type entityNewState interface {
	PrivateIsNew() bool
}

type entityBoundContext interface {
	PrivateContext() Context
}

type entityDeletable interface {
	PrivateDelete()
}

type entityForceDeletable interface {
	PrivateForceDelete()
}

// pendingWrite is the entity plus the flush metadata captured before
// PrivateFlushed() folds the change set away.
type pendingWrite struct {
	entity     Entity
	cacheIndex string
}

// Save writes exactly the given entities and nothing else on this context.
// Passing more than one opens a transaction, so entities that belong together
// commit together without the caller arranging it. A clean entity is a no-op.
func (orm *ormImplementation) Save(entities ...Entity) error {
	list, err := orm.prepareWrites(entities)
	if err != nil || len(list) == 0 {
		return err
	}
	if orm.tx != nil || len(list) == 1 {
		return orm.writeEntities(list)
	}
	return orm.Transaction(func(Context) error {
		return orm.writeEntities(list)
	})
}

// Delete removes the entities immediately, honouring FakeDelete when the entity
// has it.
func (orm *ormImplementation) Delete(entities ...Entity) error {
	return orm.deleteEntities(entities, false)
}

// ForceDelete removes the rows even when the entity supports FakeDelete.
func (orm *ormImplementation) ForceDelete(entities ...Entity) error {
	return orm.deleteEntities(entities, true)
}

func (orm *ormImplementation) deleteEntities(entities []Entity, force bool) error {
	for _, e := range entities {
		if e == nil {
			continue
		}
		if isNew, ok := e.(entityNewState); ok && isNew.PrivateIsNew() {
			return fmt.Errorf("%w: %T %d", ErrEntityNotPersisted, e, e.GetID())
		}
		if force {
			if fd, ok := e.(entityForceDeletable); ok {
				fd.PrivateForceDelete()
				continue
			}
		}
		d, ok := e.(entityDeletable)
		if !ok {
			return fmt.Errorf("%T does not support Delete", e)
		}
		d.PrivateDelete()
	}
	return orm.Save(entities...)
}

// prepareWrites dedupes by pointer identity, rejects entities bound to another
// context, and drops the ones that would produce no statement.
func (orm *ormImplementation) prepareWrites(entities []Entity) ([]*pendingWrite, error) {
	if len(entities) == 0 {
		return nil, nil
	}
	seen := make(map[Entity]bool, len(entities))
	list := make([]*pendingWrite, 0, len(entities))
	for _, e := range entities {
		if e == nil || seen[e] {
			continue
		}
		seen[e] = true
		if bound, ok := e.(entityBoundContext); ok && bound.PrivateContext() != Context(orm) {
			return nil, fmt.Errorf("entity %T %d belongs to a different context; save it on the context that created or loaded it", e, e.GetID())
		}
		if orm.tx != nil && orm.tx.staged[e] && !orm.hasUnstagedChanges(e) {
			continue
		}
		cacheIndex := ""
		if indexed, ok := e.(entityCacheIndexed); ok {
			cacheIndex = indexed.PrivateCacheIndex()
		}
		list = append(list, &pendingWrite{entity: e, cacheIndex: cacheIndex})
	}
	return list, nil
}

// hasUnstagedChanges reports whether re-staging an already-staged entity would
// produce a meaningful statement. An insert or a delete would only be repeated;
// an update carries the cumulative bind, so re-issuing it is correct.
func (orm *ormImplementation) hasUnstagedChanges(e Entity) bool {
	if isNew, ok := e.(entityNewState); ok && isNew.PrivateIsNew() {
		return false
	}
	return len(e.PrivateGetDatabaseBind()) > 0
}

func (orm *ormImplementation) writeEntities(list []*pendingWrite) error {
	for _, w := range list {
		if err := w.entity.PrivateFlush(); err != nil {
			return err
		}
		if orm.tx != nil {
			if orm.tx.staged == nil {
				orm.tx.staged = make(map[Entity]bool)
			}
			orm.tx.staged[w.entity] = true
		}
	}
	keys := orm.takePendingInvalidations()
	if err := orm.deleteCacheKeys(keys); err != nil {
		return err
	}
	for _, dbPipeline := range orm.dbPipeLines {
		if err := dbPipeline.Exec(orm); err != nil {
			return err
		}
	}
	post := func() error { return orm.runPostCommit(list, keys) }
	if orm.tx != nil {
		orm.queueAfterCommit(post)
		return nil
	}
	if err := post(); err != nil {
		return &PostCommitError{Err: err}
	}
	return nil
}

// runPostCommit performs everything that must only become visible once the rows
// are durable. The second cache delete closes the window where a concurrent read
// refilled a key from a pre-commit snapshot.
func (orm *ormImplementation) runPostCommit(list []*pendingWrite, keys map[string][]string) error {
	if err := orm.deleteCacheKeys(keys); err != nil {
		return err
	}
	pipelines := orm.takeRedisPipelines()
	for _, redisPipeline := range pipelines {
		if _, err := redisPipeline.Exec(orm); err != nil {
			return err
		}
	}
	if err := orm.publishDirtyFor(list); err != nil {
		return err
	}
	if err := orm.runAfterHandlers(list); err != nil {
		return err
	}
	for _, w := range list {
		if eventType, _ := w.entity.PrivateFlushEvent(); eventType == 3 && w.cacheIndex != "" {
			orm.removeFromContextCache(w.cacheIndex, w.entity.GetID())
		}
		w.entity.PrivateFlushed()
		orm.untrack(w.entity, w.cacheIndex)
	}
	return nil
}

// publishDirtyFor must run before PrivateFlushed: the CDC snapshot reads the
// pre-change values, and folding first would make every update event report
// Before == After.
func (orm *ormImplementation) publishDirtyFor(list []*pendingWrite) error {
	if len(orm.engine.registry.dirtyPublishers) == 0 {
		return nil
	}
	pending := make(pendingDirtyMessages)
	for _, w := range list {
		eventType, changes := w.entity.PrivateFlushEvent()
		if eventType == 0 {
			continue
		}
		et := reflect.TypeOf(w.entity)
		if et.Kind() == reflect.Ptr {
			et = et.Elem()
		}
		publisher, hasPub := orm.engine.registry.dirtyPublishers[et]
		if !hasPub {
			continue
		}
		if err := buildDirtyEvent(orm, pending, publisher, w.entity, dirtyOpFromFlushType(eventType), changes); err != nil {
			return err
		}
	}
	return flushDirtyMessages(orm, pending)
}

func (orm *ormImplementation) runAfterHandlers(list []*pendingWrite) error {
	if orm.engine.afterInsertHandlers == nil && orm.engine.afterUpdateHandlers == nil && orm.engine.afterDeleteHandlers == nil {
		return nil
	}
	for _, w := range list {
		if w.cacheIndex == "" {
			continue
		}
		eventType, changes := w.entity.PrivateFlushEvent()
		var err error
		switch eventType {
		case 1:
			if handler, ok := orm.engine.afterInsertHandlers[w.cacheIndex]; ok {
				err = handler(orm, w.entity)
			}
		case 2:
			if handler, ok := orm.engine.afterUpdateHandlers[w.cacheIndex]; ok {
				err = handler(orm, w.entity, changes)
			}
		case 3:
			if handler, ok := orm.engine.afterDeleteHandlers[w.cacheIndex]; ok {
				err = handler(orm, w.entity)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// untrack drops a written entity from the dirty set, so the context cache may
// evict it again once it holds no unsaved changes.
func (orm *ormImplementation) untrack(e Entity, cacheIndex string) {
	if orm.trackedEntities == nil {
		return
	}
	if cacheIndex != "" {
		if entities, ok := orm.trackedEntities.Load(cacheIndex); ok {
			entities.Delete(e.GetID())
			return
		}
	}
	orm.trackedEntities.Range(func(_ string, entities *xsyncEntityMap) bool {
		if tracked, ok := entities.Load(e.GetID()); ok && tracked == e {
			entities.Delete(e.GetID())
			return false
		}
		return true
	})
}

// takeRedisPipelines drains the per-context pipeline registry. Every
// RedisPipeLine call appends, so without the drain a long-lived context grows a
// slice it never reuses.
func (orm *ormImplementation) takeRedisPipelines() []*RedisPipeLine {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	pipelines := orm.redisPipeLines
	orm.redisPipeLines = nil
	return pipelines
}
