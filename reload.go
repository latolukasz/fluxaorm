package fluxaorm

import (
	"errors"
	"fmt"
)

// ErrEntityUnsavedChanges is returned by Reload for an entity holding unsaved
// changes. Reload discards them, and doing that to a handle an outer caller is
// still mutating is a silent lost update - save or drop the handle first.
var ErrEntityUnsavedChanges = errors.New("entity has unsaved changes")

// ErrEntityVanished is returned by Reload when the row is gone from the
// database, so there is nothing left to refresh the handle from.
var ErrEntityVanished = errors.New("entity row no longer exists")

// Reload re-reads each entity from MySQL, in place. The pointer is unchanged, so
// every holder of that entity - including the identity map - sees the fresh row;
// this is the only way to get current data without ending up with two divergent
// handles on one row.
//
// It bypasses the Redis row cache on purpose: a cached row is exactly what a
// caller reloading under a lock is trying not to trust. Inside a transaction the
// read goes through the transaction, so it sees that transaction's own writes.
func (orm *ormImplementation) Reload(entities ...Entity) error {
	for _, entity := range entities {
		if entity.PrivateIsNew() {
			return fmt.Errorf("reload entity %d: %w", entity.GetID(), ErrEntityNotPersisted)
		}
		if len(entity.PrivateGetDatabaseBind()) > 0 {
			return fmt.Errorf("reload entity %d: %w", entity.GetID(), ErrEntityUnsavedChanges)
		}
		found, err := entity.PrivateReload()
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("reload entity %d: %w", entity.GetID(), ErrEntityVanished)
		}
	}
	return nil
}
