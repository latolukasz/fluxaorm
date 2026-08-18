package fluxaorm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Reload discards whatever the handle holds, so it must refuse an entity with
// unsaved changes rather than silently dropping an outer caller's mutation.
func TestReloadRefusesAnEntityWithUnsavedChanges(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{})
	ctx := engine.NewContext(context.Background())

	dirty := &stubEntity{id: 7, bind: map[string]any{"Name": "pending"}}

	err := ctx.Reload(dirty)
	assert.ErrorIs(t, err, ErrEntityUnsavedChanges)
	assert.Equal(t, 0, dirty.reloads, "the row must not be read at all")
}

func TestReloadRefusesAnUnsavedEntity(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{})
	ctx := engine.NewContext(context.Background())

	fresh := &stubEntity{id: 8, isNew: true}

	err := ctx.Reload(fresh)
	assert.ErrorIs(t, err, ErrEntityNotPersisted)
	assert.Equal(t, 0, fresh.reloads)
}

// A vanished row cannot refresh the handle, and carrying on with values that no
// longer exist anywhere is worse than stopping.
func TestReloadReportsAVanishedRow(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{})
	ctx := engine.NewContext(context.Background())

	gone := &stubEntity{id: 9, notFound: true}

	err := ctx.Reload(gone)
	assert.ErrorIs(t, err, ErrEntityVanished)
}

func TestReloadIsVariadicAndStopsOnTheFirstFailure(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{})
	ctx := engine.NewContext(context.Background())

	first := &stubEntity{id: 1}
	bad := &stubEntity{id: 2, bind: map[string]any{"Name": "pending"}}
	last := &stubEntity{id: 3}

	err := ctx.Reload(first, bad, last)
	assert.ErrorIs(t, err, ErrEntityUnsavedChanges)
	assert.Equal(t, 1, first.reloads, "entities before the failure are already reloaded")
	assert.Equal(t, 0, last.reloads, "entities after it are not")

	assert.NoError(t, ctx.Reload(first, last))
	assert.Equal(t, 2, first.reloads)
	assert.Equal(t, 1, last.reloads)
}

// The error names the entity, otherwise a variadic Reload failure gives no clue
// which of the entities was the problem.
func TestReloadErrorNamesTheEntity(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{})
	ctx := engine.NewContext(context.Background())

	err := ctx.Reload(&stubEntity{id: 4321, bind: map[string]any{"Name": "x"}})
	assert.ErrorContains(t, err, "4321")
	assert.True(t, errors.Is(err, ErrEntityUnsavedChanges))
}
