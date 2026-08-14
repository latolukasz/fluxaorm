package fluxaorm

import (
	"errors"
	"fmt"
)

// ErrTxRollbackOnly is returned by the outermost Transaction when a nested
// Transaction failed and the caller swallowed its error.
var ErrTxRollbackOnly = errors.New("transaction is rollback-only")

// ErrEntityNotPersisted is returned when Delete is called on an entity that was
// never written to the database.
var ErrEntityNotPersisted = errors.New("entity was never persisted")

// PostCommitError wraps a failure that happened after the SQL transaction was
// already committed - cache invalidation, search indexing, CDC publishing or an
// after-* handler. The rows are durable; retrying the whole operation would
// apply them twice.
type PostCommitError struct {
	Err error
}

func (e *PostCommitError) Error() string {
	return "post-commit failure (database changes are committed): " + e.Err.Error()
}

func (e *PostCommitError) Unwrap() error {
	return e.Err
}

type txState struct {
	txs          map[string]DBTransaction
	order        []string
	rollbackOnly bool
	afterCommit  []func() error
	staged       map[Entity]bool
}

func (orm *ormImplementation) InTransaction() bool {
	return orm.tx != nil
}

// DB returns the executor reads must go through: the open transaction when this
// context holds one for the pool, otherwise the pool itself. Engine().DB stays
// pool-only, because DDL, the locker and GetDBClient all need a non-transactional
// handle.
func (orm *ormImplementation) DB(pool string) DBBase {
	if orm.tx != nil {
		if tx, has := orm.tx.txs[pool]; has {
			return tx
		}
	}
	return orm.engine.DB(pool)
}

// txFor opens the pool's transaction on first write and reuses it afterwards.
func (orm *ormImplementation) txFor(pool string) (DBTransaction, error) {
	tx, has := orm.tx.txs[pool]
	if has {
		return tx, nil
	}
	tx, err := orm.engine.DB(pool).Begin(orm)
	if err != nil {
		return nil, err
	}
	orm.tx.txs[pool] = tx
	orm.tx.order = append(orm.tx.order, pool)
	return tx, nil
}

// Transaction runs fn with every write on this context enrolled in one database
// transaction. BEGIN is lazy - a fn that reads only never opens one. Nested
// calls join the outer transaction; if a nested call fails the outer one can no
// longer commit, even if the caller swallows the error.
func (orm *ormImplementation) Transaction(fn func(tx Context) error) error {
	if orm.tx != nil {
		err := fn(orm)
		if err != nil {
			orm.tx.rollbackOnly = true
			return err
		}
		return nil
	}

	orm.tx = &txState{txs: make(map[string]DBTransaction)}
	committed := false
	defer func() {
		if r := recover(); r != nil {
			orm.rollbackTx()
			orm.tx = nil
			orm.discardPendingInvalidations()
			panic(r)
		}
		if !committed {
			orm.rollbackTx()
			orm.tx = nil
			orm.discardPendingInvalidations()
		}
	}()

	if err := fn(orm); err != nil {
		return err
	}
	if orm.tx.rollbackOnly {
		return ErrTxRollbackOnly
	}

	afterCommit := orm.tx.afterCommit
	if err := orm.commitTx(); err != nil {
		return err
	}
	committed = true
	orm.tx = nil

	for _, cb := range afterCommit {
		if err := cb(); err != nil {
			return &PostCommitError{Err: err}
		}
	}
	return nil
}

// afterCommit queues work that must only become visible once the rows are
// durable. Outside a transaction the caller runs it inline.
func (orm *ormImplementation) queueAfterCommit(fn func() error) {
	orm.tx.afterCommit = append(orm.tx.afterCommit, fn)
}

func (orm *ormImplementation) commitTx() error {
	var committedPools []string
	for _, pool := range orm.tx.order {
		if err := orm.tx.txs[pool].Commit(orm); err != nil {
			if len(committedPools) > 0 {
				return fmt.Errorf("commit failed on pool %q after pools %v already committed: %w", pool, committedPools, err)
			}
			orm.rollbackTx()
			return err
		}
		committedPools = append(committedPools, pool)
	}
	return nil
}

func (orm *ormImplementation) rollbackTx() {
	if orm.tx == nil {
		return
	}
	for _, pool := range orm.tx.order {
		_ = orm.tx.txs[pool].Rollback(orm)
	}
	orm.tx.txs = make(map[string]DBTransaction)
	orm.tx.order = nil
	for _, dbPipeline := range orm.dbPipeLines {
		dbPipeline.discard()
	}
	for _, redisPipeline := range orm.takeRedisPipelines() {
		redisPipeline.discard()
	}
}
