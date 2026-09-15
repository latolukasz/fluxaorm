package test_generate

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/stretchr/testify/require"
)

func persistedRepeatedName(t *testing.T, ctx fluxaorm.Context, id uint64) string {
	t.Helper()
	var name string
	found, err := ctx.DB(fluxaorm.DefaultPoolCode).QueryRow(ctx,
		fluxaorm.NewWhere("SELECT `Name` FROM `generateEntityWithTimestamps` WHERE `ID` = ?", id), &name)
	require.NoError(t, err)
	require.True(t, found)
	return name
}

func TestRepeatedSaveWritesEachChangeAndPreservesUnsavedEdits(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	logs := captureQueries(ctx)
	e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("first")
	var snapshots []*entities.GenerateEntityWithTimestamps
	var before []any
	entities.GenerateEntityWithTimestampsProvider.OnAfterInsert(ctx.Engine(), func(_ fluxaorm.Context, view *entities.GenerateEntityWithTimestamps) error {
		snapshots = append(snapshots, view)
		return nil
	})
	entities.GenerateEntityWithTimestampsProvider.OnAfterUpdate(ctx.Engine(), func(_ fluxaorm.Context, view *entities.GenerateEntityWithTimestamps, changes map[string]any) error {
		snapshots = append(snapshots, view)
		before = append(before, changes["Name"])
		return nil
	})
	require.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(e))
		require.False(t, e.PrivateIsNew())
		require.NoError(t, tx.Save(e, e))
		e.SetName("second")
		require.NoError(t, tx.Save(e))
		e.SetName("first") // Returning to the INSERT value is still an UPDATE.
		require.NoError(t, tx.Save(e))
		require.Equal(t, "first", persistedRepeatedName(t, tx, e.GetID()))
		require.Empty(t, snapshots, "callbacks run only after commit")
		e.SetName("unsaved")
		return nil
	}))
	require.Equal(t, 1, logs.count("INSERT INTO `generateEntityWithTimestamps`"))
	require.Equal(t, 2, logs.count("UPDATE `generateEntityWithTimestamps`"))
	require.Equal(t, "first", persistedRepeatedName(t, ctx, e.GetID()))
	require.Equal(t, "unsaved", e.GetName())
	require.Contains(t, e.PrivateGetDatabaseBind(), "Name")
	require.Len(t, snapshots, 3)
	require.Equal(t, []string{"first", "second", "first"}, []string{snapshots[0].GetName(), snapshots[1].GetName(), snapshots[2].GetName()})
	require.Equal(t, []any{"first", "second"}, before)
	for _, snapshot := range snapshots {
		require.NotSame(t, e, snapshot)
		require.ErrorIs(t, ctx.Save(snapshot), fluxaorm.ErrEntityReadOnly)
		require.ErrorIs(t, ctx.Delete(snapshot), fluxaorm.ErrEntityReadOnly)
		require.ErrorIs(t, ctx.ForceDelete(snapshot), fluxaorm.ErrEntityReadOnly)
		require.ErrorIs(t, ctx.Reload(snapshot), fluxaorm.ErrEntityReadOnly)
	}
	require.NoError(t, ctx.Save(e))
	require.Equal(t, "unsaved", persistedRepeatedName(t, ctx, e.GetID()))
	require.Equal(t, "first", snapshots[0].GetName(), "retained callback views stay frozen")
}

func TestRepeatedSaveRollbackRetainsLatestIntent(t *testing.T) {
	for _, initiallyNew := range []bool{true, false} {
		name := "existing"
		if initiallyNew {
			name = "new"
		}
		t.Run(name, func(t *testing.T) {
			ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
			e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("original")
			if !initiallyNew {
				require.NoError(t, ctx.Save(e))
			}
			callbacks := 0
			entities.GenerateEntityWithTimestampsProvider.OnAfterUpdate(ctx.Engine(), func(fluxaorm.Context, *entities.GenerateEntityWithTimestamps, map[string]any) error {
				callbacks++
				return nil
			})
			err := ctx.Transaction(func(tx fluxaorm.Context) error {
				e.SetName("first")
				require.NoError(t, tx.Save(e))
				e.SetName("second")
				require.NoError(t, tx.Save(e))
				e.SetName("latest-unsaved")
				return errors.New("abort")
			})
			require.Error(t, err)
			require.Zero(t, callbacks)
			require.Equal(t, initiallyNew, e.PrivateIsNew())
			require.Equal(t, "latest-unsaved", e.GetName())
			if !initiallyNew {
				require.Equal(t, "original", persistedRepeatedName(t, ctx, e.GetID()))
			}
			require.NoError(t, ctx.Save(e))
			require.Equal(t, "latest-unsaved", persistedRepeatedName(t, ctx, e.GetID()))
			require.Empty(t, e.PrivateGetDatabaseBind())
		})
	}
}

func TestRepeatedSaveRollbackOfRevertedExistingValueIsClean(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("original")
	require.NoError(t, ctx.Save(e))
	require.Error(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		e.SetName("temporary")
		require.NoError(t, tx.Save(e))
		e.SetName("original")
		require.NoError(t, tx.Save(e))
		return errors.New("abort")
	}))
	require.NotContains(t, e.PrivateGetDatabaseBind(), "Name")
	require.Equal(t, "original", persistedRepeatedName(t, ctx, e.GetID()))
}

func TestRepeatedSaveRollbackOfRedisLoadedEntity(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx).SetName("original")
	require.NoError(t, ctx.Save(e))
	_, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(ctx.Clone(), e.GetID())
	require.NoError(t, err)
	require.True(t, found) // First read populates Redis.
	writer := ctx.Clone()
	loaded, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(writer, e.GetID())
	require.NoError(t, err)
	require.True(t, found)
	created := loaded.GetCreatedAt()
	require.Error(t, writer.Transaction(func(tx fluxaorm.Context) error {
		loaded.SetName("first")
		require.NoError(t, tx.Save(loaded))
		loaded.SetName("second")
		require.NoError(t, tx.Save(loaded))
		loaded.SetName("latest")
		return errors.New("abort")
	}))
	require.Equal(t, created, loaded.GetCreatedAt())
	require.NoError(t, writer.Save(loaded))
	readback, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(ctx.Clone(), e.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "latest", readback.GetName())
	require.Equal(t, created, readback.GetCreatedAt())
}

func TestRepeatedSaveThenDeleteInOneTransaction(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	logs := captureQueries(ctx)
	e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("inserted")
	var names []string
	entities.GenerateEntityWithTimestampsProvider.OnAfterDelete(ctx.Engine(), func(_ fluxaorm.Context, view *entities.GenerateEntityWithTimestamps) error {
		names = append(names, view.GetName())
		return nil
	})
	require.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(e))
		e.SetName("updated")
		require.NoError(t, tx.Save(e))
		require.NoError(t, tx.Delete(e))
		require.NoError(t, tx.Delete(e))
		return tx.Save(e)
	}))
	require.Equal(t, []string{"updated"}, names)
	require.Equal(t, 1, logs.count("DELETE FROM `generateEntityWithTimestamps`"))
	_, found, err := entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, e.GetID())
	require.NoError(t, err)
	require.False(t, found)
}

func TestRepeatedSaveDeleteRollbackCanRetry(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("original")
	require.NoError(t, ctx.Save(e))
	require.Error(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		e.SetName("updated")
		require.NoError(t, tx.Save(e))
		require.NoError(t, tx.Delete(e))
		return errors.New("abort")
	}))
	require.Equal(t, "original", persistedRepeatedName(t, ctx, e.GetID()))
	require.NoError(t, ctx.Save(e), "retained delete intent can be retried")
	_, found, err := entities.GenerateEntityWithTimestampsProvider.GetByID(ctx.Clone(), e.GetID())
	require.NoError(t, err)
	require.False(t, found)
}

func TestRepeatedSaveSoftDeleteCanBeReverted(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateReferenceEntity{})
	e := entities.GenerateReferenceEntityProvider.New(ctx).SetName("name")
	require.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(e))
		require.NoError(t, tx.Delete(e))
		e.SetFakeDelete(false).SetName("restored")
		require.NoError(t, tx.Save(e))
		e.SetName("pending")
		return nil
	}))
	identity, found, err := entities.GenerateReferenceEntityProvider.GetByID(ctx, e.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Same(t, e, identity, "historical delete must not evict a restored live handle")
	require.Equal(t, "pending", identity.GetName())
	loaded, found, err := entities.GenerateReferenceEntityProvider.GetByID(ctx.Clone(), e.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, loaded.GetFakeDelete())
	require.Equal(t, "restored", loaded.GetName())
}

func TestRepeatedSaveSwallowedSQLErrorRollsBackAndDiscardsQueues(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedUnique{})
	first := entities.GenerateEntityCachedUniqueProvider.New(ctx).SetEmail("same@example.com").SetName("first")
	second := entities.GenerateEntityCachedUniqueProvider.New(ctx).SetEmail("same@example.com").SetName("second")
	err := ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(first))
		require.Error(t, tx.Save(second))
		return nil // Deliberately swallow the database error.
	})
	require.ErrorIs(t, err, fluxaorm.ErrTxRollbackOnly)
	require.True(t, first.PrivateIsNew())
	require.True(t, second.PrivateIsNew())
	second.SetEmail("different@example.com")
	require.NoError(t, ctx.Save(first, second))
	all, err := entities.GenerateEntityCachedUniqueProvider.SearchMany(ctx.Clone(), fluxaorm.NewQuery())
	require.NoError(t, err)
	require.Len(t, all, 2)
}

func TestRepeatedSavePostCommitFailureKeepsCommittedBaseline(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	e := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("first")
	entities.GenerateEntityWithTimestampsProvider.OnAfterInsert(ctx.Engine(), func(fluxaorm.Context, *entities.GenerateEntityWithTimestamps) error {
		return errors.New("handler failed")
	})
	err := ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(e))
		e.SetName("second")
		require.NoError(t, tx.Save(e))
		e.SetName("unsaved")
		return nil
	})
	var postCommit *fluxaorm.PostCommitError
	require.ErrorAs(t, err, &postCommit)
	require.False(t, e.PrivateIsNew())
	require.Equal(t, "second", persistedRepeatedName(t, ctx, e.GetID()))
	require.Equal(t, "unsaved", e.GetName())
	require.NoError(t, ctx.Save(e))
	require.Equal(t, "unsaved", persistedRepeatedName(t, ctx, e.GetID()))
}

func TestRepeatedSaveBatchPreservesEditsAfterAnEarlierStatement(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	first := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("written")
	second := entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName("second")
	// Before callbacks are registered globally. Restrict this callback to these
	// exact pointers so it cannot affect subsequent tests or generated entities.
	entities.RegisterGenerateEntityWithTimestampsBeforeInsert(func(e *entities.GenerateEntityWithTimestamps) {
		if e == second {
			first.SetName("late-edit")
		}
	})
	require.NoError(t, ctx.Save(first, second))
	require.Equal(t, "written", persistedRepeatedName(t, ctx, first.GetID()))
	require.Equal(t, "late-edit", first.GetName())
	require.Contains(t, first.PrivateGetDatabaseBind(), "Name")
	require.NoError(t, ctx.Save(first))
	require.Equal(t, "late-edit", persistedRepeatedName(t, ctx, first.GetID()))
}

func TestRepeatedSaveOutboxCapturesEveryWrite(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()
	e := newOutboxEntity(ctx, "inserted")
	require.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		require.NoError(t, tx.Save(e))
		e.SetName("updated")
		require.NoError(t, tx.Save(e))
		e.SetName("inserted")
		require.NoError(t, tx.Save(e))
		require.NoError(t, tx.Save(e)) // No SQL and no extra outbox row.
		e.SetName("unsaved")
		return nil
	}))
	rows := outboxRows(t, ctx.Clone())
	require.Len(t, rows, 3)
	var events []fluxaorm.DirtyEvent[map[string]any]
	for _, row := range rows {
		require.Equal(t, enums.CDCOutboxStatusList.Dispatched, row.GetStatus())
		var event fluxaorm.DirtyEvent[map[string]any]
		require.NoError(t, json.Unmarshal([]byte(row.GetPayload()), &event))
		events = append(events, event)
	}
	require.Equal(t, fluxaorm.DirtyInsert, events[0].Op)
	require.Nil(t, events[0].Before)
	require.Equal(t, "inserted", (*events[0].After)["Name"])
	require.Equal(t, fluxaorm.DirtyUpdate, events[1].Op)
	require.Equal(t, "inserted", (*events[1].Before)["Name"])
	require.Equal(t, "updated", (*events[1].After)["Name"])
	require.Equal(t, fluxaorm.DirtyUpdate, events[2].Op)
	require.Equal(t, "updated", (*events[2].Before)["Name"])
	require.Equal(t, "inserted", (*events[2].After)["Name"])
	readback, found, err := entities.GenerateEntityOutboxProvider.GetByID(ctx.Clone(), e.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "inserted", readback.GetName())
	require.Equal(t, "unsaved", e.GetName())
}
