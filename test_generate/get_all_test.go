package test_generate

import (
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cachedAllNames(t *testing.T, rows []*entities.GenerateEntityCachedAll) []string {
	t.Helper()

	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.GetName())
	}

	return names
}

// The point of `orm:"cached"` is that the second call touches no MySQL at all - neither for the id
// set nor for the rows. A GetAll that still ran its SELECT would be an ordinary query with extra
// Redis traffic bolted on.
func TestGetAllSecondCallHitsNoDatabase(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedAll{})

	for _, name := range []string{"alpha", "beta", "gamma"} {
		e := entities.GenerateEntityCachedAllProvider.New(ctx)
		e.SetName(name)
		require.NoError(t, ctx.Save(e))
	}

	warm := ctx.Clone()
	warm.DisableContextCache()
	first, err := entities.GenerateEntityCachedAllProvider.GetAll(warm)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, cachedAllNames(t, first))

	// The identity map would answer on its own and prove nothing about Redis.
	reader := ctx.Clone()
	reader.DisableContextCache()

	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})

	second, err := entities.GenerateEntityCachedAllProvider.GetAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, cachedAllNames(t, second))
	assert.Equal(t, 0, countSelects(sql), "GetAll fell through to MySQL on a warm cache")
}

// A row added after the set was cached has to show up. The set is invalidated by the write rather
// than patched, so this also proves the invalidation reaches the all-ids key and not just the rows.
func TestGetAllSeesRowsInsertedAfterItWasCached(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedAll{})

	first := entities.GenerateEntityCachedAllProvider.New(ctx)
	first.SetName("alpha")
	require.NoError(t, ctx.Save(first))

	warm, err := entities.GenerateEntityCachedAllProvider.GetAll(ctx.Clone())
	require.NoError(t, err)
	require.Len(t, warm, 1)

	second := entities.GenerateEntityCachedAllProvider.New(ctx)
	second.SetName("beta")
	require.NoError(t, ctx.Save(second))

	reader := ctx.Clone()
	reader.DisableContextCache()
	after, err := entities.GenerateEntityCachedAllProvider.GetAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, cachedAllNames(t, after))
}

// Same for a removal: a deleted row must leave the set, or GetAll keeps handing out an id whose
// row cache entry is already gone.
func TestGetAllDropsDeletedRows(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedAll{})

	keep := entities.GenerateEntityCachedAllProvider.New(ctx)
	keep.SetName("keep")
	require.NoError(t, ctx.Save(keep))

	drop := entities.GenerateEntityCachedAllProvider.New(ctx)
	drop.SetName("drop")
	require.NoError(t, ctx.Save(drop))

	warm, err := entities.GenerateEntityCachedAllProvider.GetAll(ctx.Clone())
	require.NoError(t, err)
	require.Len(t, warm, 2)

	require.NoError(t, ctx.Delete(drop))

	reader := ctx.Clone()
	reader.DisableContextCache()
	after, err := entities.GenerateEntityCachedAllProvider.GetAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []string{"keep"}, cachedAllNames(t, after))
}

// An empty table caches as a stamp-only list. Without that, a cold key and a genuinely empty table
// look identical and every GetAll re-runs the SELECT forever.
func TestGetAllCachesAnEmptyTable(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedAll{})

	empty, err := entities.GenerateEntityCachedAllProvider.GetAll(ctx.Clone())
	require.NoError(t, err)
	assert.Empty(t, empty)

	reader := ctx.Clone()
	reader.DisableContextCache()

	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})

	again, err := entities.GenerateEntityCachedAllProvider.GetAll(reader)
	require.NoError(t, err)
	assert.Empty(t, again)
	assert.Equal(t, 0, countSelects(sql), "an empty table was not cached")
}

// A soft delete changes membership without deleting a row, so the UPDATE has to invalidate the set
// too - otherwise GetAll keeps returning a row the rest of the ORM treats as gone.
func TestGetAllRespectsFakeDelete(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedAllFakeDelete{})

	keep := entities.GenerateEntityCachedAllFakeDeleteProvider.New(ctx)
	keep.SetName("keep")
	require.NoError(t, ctx.Save(keep))

	gone := entities.GenerateEntityCachedAllFakeDeleteProvider.New(ctx)
	gone.SetName("gone")
	require.NoError(t, ctx.Save(gone))

	warm, err := entities.GenerateEntityCachedAllFakeDeleteProvider.GetAll(ctx.Clone())
	require.NoError(t, err)
	require.Len(t, warm, 2)

	require.NoError(t, ctx.Delete(gone))

	reader := ctx.Clone()
	reader.DisableContextCache()
	after, err := entities.GenerateEntityCachedAllFakeDeleteProvider.GetAll(reader)
	require.NoError(t, err)
	require.Len(t, after, 1)
	assert.Equal(t, "keep", after[0].GetName())
}
