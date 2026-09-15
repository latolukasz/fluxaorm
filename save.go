package fluxaorm

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrEntityNeedsRegeneration rejects generated code without transactional state
// snapshots. Continuing with that code would silently lose repeated writes.
var ErrEntityNeedsRegeneration = errors.New("generated entity needs regeneration with fluxaorm.Generate to support transactional write snapshots")

// ErrEntityReadOnly protects the frozen entity views passed to after callbacks.
// Obtain the live entity from its provider to make another write.
var ErrEntityReadOnly = errors.New("entity is a read-only write snapshot")

type entityWriteState interface {
	PrivateSnapshot() Entity
	PrivateRollback(Entity)
	PrivateAdvance(Entity)
	PrivateIsSnapshot() bool
	PrivateIsDeleted() bool
	PrivateDatabasePool() string
}

// Optional capabilities the generator emits. Adding a method to Entity itself
// breaks every consumer until it regenerates, so only put one there when the ORM
// cannot work without it (PrivateReload, PrivateIsNew).
type entityCacheIndexed interface {
	PrivateCacheIndex() string
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

// pendingWrite keeps a frozen SQL write alongside its independently mutable
// source. Post-commit events read the snapshot, never the live source.
type pendingWrite struct {
	entity     Entity
	source     Entity
	cacheIndex string
	// dirtyPayload is the CDC event serialised once during outbox staging and
	// reused when publishing. The payload is not deterministic - buildEvent
	// stamps TsMs - so re-serialising it would give the stored row different
	// bytes, and therefore a different Nats-Msg-Id, from the event consumers
	// actually saw. A relay replay would then dodge JetStream's dedup and
	// deliver twice. Nil for entities that do not route to the outbox.
	dirtyPayload []byte
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
		if state, ok := e.(entityWriteState); ok && state.PrivateIsSnapshot() {
			return ErrEntityReadOnly
		}
		if e.PrivateIsNew() {
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
// context, and checks that generated code supports transactional snapshots.
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
		state, ok := e.(entityWriteState)
		if !ok {
			return nil, fmt.Errorf("%w: %T", ErrEntityNeedsRegeneration, e)
		}
		if state.PrivateIsSnapshot() {
			return nil, ErrEntityReadOnly
		}
		cacheIndex := ""
		if indexed, ok := e.(entityCacheIndexed); ok {
			cacheIndex = indexed.PrivateCacheIndex()
		}
		list = append(list, &pendingWrite{source: e, cacheIndex: cacheIndex})
	}
	return list, nil
}

func (orm *ormImplementation) writeEntities(list []*pendingWrite) (err error) {
	written := false
	defer func() {
		if err != nil && !written {
			if orm.tx != nil {
				orm.tx.rollbackOnly = true
			}
			orm.discardWriteQueues()
		}
	}()
	for _, w := range list {
		if err := w.source.PrivateFlush(); err != nil {
			return err
		}
		// Capture after before-callbacks and automatic timestamps, but before
		// advancing the live origin. Later setters cannot alter this write.
		w.entity = w.source.(entityWriteState).PrivateSnapshot()
	}
	outboxIDs, err := orm.stageDirtyOutbox(list)
	if err != nil {
		return err
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
	written = true
	for _, w := range list {
		if eventType, _ := w.entity.PrivateFlushEvent(); eventType == 0 {
			continue
		}
		if orm.tx != nil {
			if orm.tx.originals == nil {
				orm.tx.originals = make(map[Entity]Entity)
			}
			if _, exists := orm.tx.originals[w.source]; !exists {
				orm.tx.originals[w.source] = w.entity
			}
		}
		w.source.(entityWriteState).PrivateAdvance(w.entity)
	}
	post := func() error { return orm.runPostCommit(list, keys, outboxIDs) }
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
func (orm *ormImplementation) runPostCommit(list []*pendingWrite, keys map[string][]string, outboxIDs []uint64) error {
	for _, w := range list {
		if eventType, _ := w.entity.PrivateFlushEvent(); eventType == 3 && w.cacheIndex != "" {
			// A later write may have restored a soft-deleted row or replaced a
			// hard-deleted handle. A historical event must not evict that handle.
			if w.source.(entityWriteState).PrivateIsDeleted() && orm.GetFromContextCache(w.cacheIndex, w.entity.GetID()) == w.source {
				orm.removeFromContextCache(w.cacheIndex, w.entity.GetID())
			}
		}
	}
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
	if err := orm.markOutboxDispatched(outboxIDs); err != nil {
		return err
	}
	return orm.runAfterHandlers(list)
}

func (orm *ormImplementation) discardWriteQueues() {
	orm.discardPendingInvalidations()
	for _, pipeline := range orm.dbPipeLines {
		pipeline.discard()
	}
	for _, pipeline := range orm.takeRedisPipelines() {
		pipeline.discard()
	}
}

// publishDirtyFor reads each frozen write's pre-change origin and change set.
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
		if err := buildDirtyEvent(orm, pending, publisher, w, dirtyOpFromFlushType(eventType), changes); err != nil {
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
