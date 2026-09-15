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
