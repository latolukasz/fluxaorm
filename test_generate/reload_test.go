package test_generate

import (
	"strconv"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/assert"
)

// Reload is the supported way to get current data without ending up with two
// handles on one row: the pointer survives, only the values move.
func TestReloadRefreshesTheSamePointer(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})

	e := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	stale, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, "V1", stale.GetName())

	writer := ctx.Clone()
	toEdit, err := entities.GenerateEntityWithTimestampsProvider.GetByID(writer, e.GetID())
	assert.NoError(t, err)
	toEdit.SetName("V2")
	assert.NoError(t, writer.Save(toEdit))

	assert.Equal(t, "V1", stale.GetName(), "the handle is stale until it is reloaded")

	assert.NoError(t, reader.Reload(stale))
	assert.Equal(t, "V2", stale.GetName())

	after, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.Same(t, stale, after, "reload must not replace the identity-map entry with a second handle")
}

// Every holder of the row sees a reload, because they all hold one pointer. This
// is what makes reload-under-a-lock safe: the caller that fetched the entity
// earlier cannot keep acting on pre-lock values.
func TestReloadIsVisibleThroughEveryHolder(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})

	e := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	outer, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	inner, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.Same(t, outer, inner)

	writer := ctx.Clone()
	toEdit, err := entities.GenerateEntityWithTimestampsProvider.GetByID(writer, e.GetID())
	assert.NoError(t, err)
	toEdit.SetName("V2")
	assert.NoError(t, writer.Save(toEdit))

	assert.NoError(t, reader.Reload(inner))
	assert.Equal(t, "V2", outer.GetName(), "the handle the outer caller kept is current too")
}

// Reload must read MySQL, not the Redis row cache - a cached row is exactly what
// a caller reloading under a lock refuses to trust. Updating the row behind the
// ORM leaves the cached copy in place, so a reload that consulted Redis would
// return the old name.
func TestReloadBypassesTheRedisRowCache(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	// The save invalidated the key, so this read misses and refills it.
	warm := ctx.Clone()
	_, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(warm, e.GetID())
	assert.NoError(t, err)

	// This one hits Redis, so the handle carries redis-loaded values - the state
	// that has to be dropped on reload, since every getter prefers it.
	reader := ctx.Clone()
	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})
	cached, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, 0, countSelects(sql), "precondition: this handle was built from Redis, not MySQL")
	assert.Equal(t, "V1", cached.GetName())

	// Behind the ORM on purpose: the Redis list keeps saying "V1".
	_, err = ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx,
		"UPDATE `generateEntityWithTimestampsRedis` SET `Name` = ? WHERE `ID` = ?", "V2", e.GetID())
	assert.NoError(t, err)

	key := entities.GenerateEntityWithTimestampsRedisProvider.RedisCachePrefix() + strconv.FormatUint(e.GetID(), 10)
	row, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, key, 0, -1)
	assert.NoError(t, err)
	assert.Contains(t, row, "V1", "precondition: the stale row is still cached")

	assert.NoError(t, reader.Reload(cached))
	assert.Equal(t, "V2", cached.GetName(), "reload must read MySQL and drop the redis-loaded values")
}

// Inside a transaction the reload goes through that transaction, so it sees the
// transaction's own uncommitted writes rather than the pre-transaction row.
func TestReloadInsideTransactionSeesTheTransactionsWrite(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})

	e := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	handle, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)

	assert.NoError(t, reader.Transaction(func(tx fluxaorm.Context) error {
		_, err := tx.DB(fluxaorm.DefaultPoolCode).Exec(tx,
			"UPDATE `generateEntityWithTimestamps` SET `Name` = ? WHERE `ID` = ?", "V2", e.GetID())
		if err != nil {
			return err
		}

		if err := tx.Reload(handle); err != nil {
			return err
		}
		assert.Equal(t, "V2", handle.GetName())

		return nil
	}))
}

// A reloaded entity is clean, so it can be mutated and saved straight away and
// the UPDATE must be built against the values just read - not the ones the handle
// held before.
func TestReloadedEntityCanBeMutatedAndSaved(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})

	e := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	handle, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)

	writer := ctx.Clone()
	toEdit, err := entities.GenerateEntityWithTimestampsProvider.GetByID(writer, e.GetID())
	assert.NoError(t, err)
	toEdit.SetName("V2")
	assert.NoError(t, writer.Save(toEdit))

	assert.NoError(t, reader.Reload(handle))

	handle.SetName("V3")
	assert.NoError(t, reader.Save(handle))

	persisted, err := entities.GenerateEntityWithTimestampsProvider.GetByID(ctx.Clone(), e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, "V3", persisted.GetName())
}

// Setting a field back to the value the database already holds is a no-op, so a
// reload that failed to refresh the origin snapshot would make this save write
// nothing - the silent half of the stale-handle bug.
func TestReloadRefreshesTheOriginSnapshot(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})

	e := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	handle, err := entities.GenerateEntityWithTimestampsProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)

	writer := ctx.Clone()
	toEdit, err := entities.GenerateEntityWithTimestampsProvider.GetByID(writer, e.GetID())
	assert.NoError(t, err)
	toEdit.SetName("V2")
	assert.NoError(t, writer.Save(toEdit))

	assert.NoError(t, reader.Reload(handle))

	// Against the refreshed snapshot ("V2") this is a real change; against the
	// stale one ("V1") the setter would discard it as unchanged.
	handle.SetName("V1")
	assert.NoError(t, reader.Save(handle))

	persisted, err := entities.GenerateEntityWithTimestampsProvider.GetByID(ctx.Clone(), e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, "V1", persisted.GetName())
}

// A read that straddles another write's commit can park the pre-commit row in the
// cache past both of Save's deletes. Reload is where a caller learns the truth, so
// it must not leave that copy behind for the next cached read.
func TestReloadDropsTheStaleCachedRow(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("V1")
	assert.NoError(t, ctx.Save(e))

	cachedRead := func() string {
		reader := ctx.Clone()
		loaded, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
		assert.NoError(t, err)

		return loaded.GetName()
	}

	assert.Equal(t, "V1", cachedRead(), "the read fills the row cache")

	_, err := ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx,
		"UPDATE `generateEntityWithTimestampsRedis` SET `Name` = ? WHERE `ID` = ?", "V2", e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, "V1", cachedRead(), "precondition: the cache serves the stale row")

	holder := ctx.Clone()
	handle, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(holder, e.GetID())
	assert.NoError(t, err)
	assert.NoError(t, holder.Reload(handle))
	assert.Equal(t, "V2", handle.GetName())

	assert.Equal(t, "V2", cachedRead(), "after the reload a cached read must return the committed row")
}
