package fluxaorm

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Embedding keeps these probes limited to the methods under test. Any unexpected
// SQL/read path fails instead of quietly behaving like a successful database.
type commitFailureTransaction struct {
	DBTransaction
	err       error
	commits   int
	rollbacks int
}

func (tx *commitFailureTransaction) Commit(Context) error {
	tx.commits++
	return tx.err
}

func (tx *commitFailureTransaction) Rollback(Context) error {
	tx.rollbacks++
	return nil
}

type rollbackStateProbe struct {
	stubEntity
	pool      string
	restored  Entity
	rollbacks int
}

func (e *rollbackStateProbe) PrivateSnapshot() Entity     { return e }
func (e *rollbackStateProbe) PrivateAdvance(Entity)       {}
func (e *rollbackStateProbe) PrivateIsSnapshot() bool     { return false }
func (e *rollbackStateProbe) PrivateIsDeleted() bool      { return false }
func (e *rollbackStateProbe) PrivateDatabasePool() string { return e.pool }
func (e *rollbackStateProbe) PrivateRollback(original Entity) {
	e.restored = original
	e.rollbacks++
}

func TestCommitFailureRestoresOnlyUncommittedPools(t *testing.T) {
	for _, firstFails := range []bool{true, false} {
		name := "partial-commit"
		if firstFails {
			name = "first-commit"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("injected commit failure")
			firstTx := &commitFailureTransaction{}
			secondTx := &commitFailureTransaction{err: failure}
			if firstFails {
				firstTx.err = failure
			}
			first := &rollbackStateProbe{pool: "first"}
			second := &rollbackStateProbe{pool: "second"}
			firstOriginal, secondOriginal := &stubEntity{tag: "first original"}, &stubEntity{tag: "second original"}
			orm := &ormImplementation{}
			postCommitRan := false
			err := orm.Transaction(func(Context) error {
				orm.tx.txs = map[string]DBTransaction{"first": firstTx, "second": secondTx}
				orm.tx.order = []string{"first", "second"}
				orm.tx.originals = map[Entity]Entity{first: firstOriginal, second: secondOriginal}
				orm.queueAfterCommit(func() error { postCommitRan = true; return nil })
				return nil
			})
			require.ErrorIs(t, err, failure)
			require.False(t, orm.InTransaction())
			require.False(t, postCommitRan)
			require.Equal(t, 1, firstTx.commits)
			require.Equal(t, 1, second.rollbacks)
			require.Same(t, secondOriginal, second.restored)
			if firstFails {
				require.Equal(t, 1, first.rollbacks)
				require.Same(t, firstOriginal, first.restored)
				require.Zero(t, secondTx.commits)
			} else {
				require.ErrorContains(t, err, "after pools [first] already committed")
				require.Zero(t, first.rollbacks, "a durable entity must never become new/dirty after another pool fails")
				require.Zero(t, firstTx.rollbacks)
			}
		})
	}
}

type obsoleteGeneratedEntity struct {
	stubEntity
	flushes int
}

func (e *obsoleteGeneratedEntity) PrivateFlush() error {
	e.flushes++
	return nil
}

func TestSaveRejectsEntitiesNeedingRegenerationBeforeStagingSQL(t *testing.T) {
	orm := &ormImplementation{}
	entity := &obsoleteGeneratedEntity{}
	err := orm.Save(entity)
	require.ErrorIs(t, err, ErrEntityNeedsRegeneration)
	require.ErrorContains(t, err, "fluxaorm.Generate")
	require.Zero(t, entity.flushes)
	require.Empty(t, orm.dbPipeLines)
}

// deleteProbe records the delete intent Delete/ForceDelete apply, without ever
// reaching a database.
type deleteProbe struct {
	stubEntity
	snapshot     bool
	flushErr     error
	flushes      int
	deletes      int
	forceDeletes int
}

func (e *deleteProbe) PrivateDelete()              { e.deletes++ }
func (e *deleteProbe) PrivateForceDelete()         { e.forceDeletes++ }
func (e *deleteProbe) PrivateSnapshot() Entity     { return e }
func (e *deleteProbe) PrivateRollback(Entity)      {}
func (e *deleteProbe) PrivateAdvance(Entity)       {}
func (e *deleteProbe) PrivateIsSnapshot() bool     { return e.snapshot }
func (e *deleteProbe) PrivateIsDeleted() bool      { return false }
func (e *deleteProbe) PrivateDatabasePool() string { return "" }
func (e *deleteProbe) PrivateFlush() error {
	e.flushes++
	return e.flushErr
}

// undeletableProbe carries transactional write state but no delete support, so
// Delete rejects it on the entityDeletable check alone.
type undeletableProbe struct {
	stubEntity
}

func (e *undeletableProbe) PrivateSnapshot() Entity     { return e }
func (e *undeletableProbe) PrivateRollback(Entity)      {}
func (e *undeletableProbe) PrivateAdvance(Entity)       {}
func (e *undeletableProbe) PrivateIsSnapshot() bool     { return false }
func (e *undeletableProbe) PrivateIsDeleted() bool      { return false }
func (e *undeletableProbe) PrivateDatabasePool() string { return "" }
func (e *undeletableProbe) PrivateFlush() error         { return nil }

// foreignProbe belongs to another context, which only Save used to notice.
type foreignProbe struct {
	deleteProbe
	owner Context
}

func (e *foreignProbe) PrivateContext() Context { return e.owner }

// TestDeleteLeavesBatchUntouchedWhenAnyEntityIsRejected: the delete intent is
// permanent once set, so a rejected batch must not mark the entities it already
// walked past - an unrelated later Save would turn that intent into a DELETE.
func TestDeleteLeavesBatchUntouchedWhenAnyEntityIsRejected(t *testing.T) {
	for _, c := range []struct {
		name        string
		rejected    Entity
		errIs       error
		errContains string
	}{
		{
			name:     "never persisted",
			rejected: &deleteProbe{stubEntity: stubEntity{id: 2, isNew: true}},
			errIs:    ErrEntityNotPersisted,
		},
		{
			name:     "read-only snapshot",
			rejected: &deleteProbe{stubEntity: stubEntity{id: 2}, snapshot: true},
			errIs:    ErrEntityReadOnly,
		},
		{
			name:        "delete not supported",
			rejected:    &undeletableProbe{stubEntity: stubEntity{id: 2}},
			errContains: "does not support Delete",
		},
		{
			name:        "bound to another context",
			rejected:    &foreignProbe{deleteProbe: deleteProbe{stubEntity: stubEntity{id: 2}}, owner: &ormImplementation{}},
			errContains: "belongs to a different context",
		},
		{
			name:     "generated before write snapshots",
			rejected: &stubEntity{id: 2},
			errIs:    ErrEntityNeedsRegeneration,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, force := range []bool{false, true} {
				accepted := &deleteProbe{stubEntity: stubEntity{id: 1}}
				orm := &ormImplementation{}
				var err error
				if force {
					err = orm.ForceDelete(accepted, c.rejected)
				} else {
					err = orm.Delete(accepted, c.rejected)
				}
				if c.errIs != nil {
					require.ErrorIs(t, err, c.errIs)
				} else {
					require.ErrorContains(t, err, c.errContains)
				}
				require.Zero(t, accepted.deletes, "entity marked deleted although the batch was rejected")
				require.Zero(t, accepted.forceDeletes, "entity marked force-deleted although the batch was rejected")
				require.Zero(t, accepted.flushes, "rejected batch reached Save")
			}
		})
	}
}

// TestDeleteMarksEveryEntityOnceTheBatchValidates: the second pass still applies
// the intent to the whole batch, Delete and ForceDelete alike. The injected flush
// failure stops Save before it needs a database.
func TestDeleteMarksEveryEntityOnceTheBatchValidates(t *testing.T) {
	stop := errors.New("stop before the write")
	for _, force := range []bool{false, true} {
		first := &deleteProbe{stubEntity: stubEntity{id: 1}, flushErr: stop}
		second := &deleteProbe{stubEntity: stubEntity{id: 2}, flushErr: stop}
		orm := &ormImplementation{}
		var err error
		if force {
			err = orm.ForceDelete(first, second)
		} else {
			err = orm.Delete(first, second)
		}
		require.ErrorIs(t, err, stop)
		for _, e := range []*deleteProbe{first, second} {
			if force {
				require.Equal(t, 1, e.forceDeletes)
				require.Zero(t, e.deletes)
			} else {
				require.Equal(t, 1, e.deletes)
				require.Zero(t, e.forceDeletes)
			}
		}
	}
}

// TestDeleteSkipsNilEntities: a nil slot is ignored by both passes.
func TestDeleteSkipsNilEntities(t *testing.T) {
	stop := errors.New("stop before the write")
	entity := &deleteProbe{stubEntity: stubEntity{id: 1}, flushErr: stop}
	orm := &ormImplementation{}
	require.ErrorIs(t, orm.Delete(nil, entity, nil), stop)
	require.Equal(t, 1, entity.deletes)
}
