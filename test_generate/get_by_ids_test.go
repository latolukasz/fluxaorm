package test_generate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/require"
)

func TestGetByIDsOrderDuplicatesAndMissing(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		checkGetByIDsOrder(t, generateEntityWithTimestamps{},
			func(ctx fluxaorm.Context, name string) *entities.GenerateEntityWithTimestamps {
				return entities.GenerateEntityWithTimestampsProvider.New(ctx).SetName(name)
			}, entities.GenerateEntityWithTimestampsProvider.GetByIDs)
	})
	t.Run("Redis", func(t *testing.T) {
		checkGetByIDsOrder(t, generateEntityWithTimestampsRedis{},
			func(ctx fluxaorm.Context, name string) *entities.GenerateEntityWithTimestampsRedis {
				return entities.GenerateEntityWithTimestampsRedisProvider.New(ctx).SetName(name)
			}, entities.GenerateEntityWithTimestampsRedisProvider.GetByIDs)
	})
}

type getByIDsTestEntity interface {
	fluxaorm.Entity
	GetID() uint64
	GetName() string
}

func checkGetByIDsOrder[T getByIDsTestEntity](t *testing.T, fixture any,
	create func(fluxaorm.Context, string) T, get func(fluxaorm.Context, ...uint64) ([]T, error),
) {
	t.Helper()
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), fixture)
	ids := make([]uint64, 40)
	seeded := make([]T, len(ids))
	names := make(map[uint64]string, len(ids))
	for i := range ids {
		name := fmt.Sprintf("row-%02d", i)
		seeded[i] = create(ctx, name)
		ids[i] = seeded[i].GetID()
		names[ids[i]] = name
		require.NoError(t, ctx.Save(seeded[i]))
	}
	missing := ctx.Engine().NextID()
	reversed := slices.Clone(ids)
	slices.Reverse(reversed)
	large := append(slices.Clone(reversed), ids[0], missing, ids[39], missing, 0, 0)
	cases := []struct {
		name string
		ids  []uint64
		want []uint64
	}{
		{"empty", nil, nil},
		{"single", []uint64{ids[4]}, []uint64{ids[4]}},
		{"duplicates", []uint64{ids[2], ids[0], ids[2], ids[0]}, []uint64{ids[2], ids[0]}},
		{"missing", []uint64{missing, ids[2], missing, ids[0]}, []uint64{ids[2], ids[0]}},
		{"zero_id", []uint64{0, ids[2], 0}, []uint64{ids[2]}},
		{"all_missing", []uint64{missing, missing}, nil},
		{"small_boundary", reversed[:32], reversed[:32]},
		{"large_boundary", reversed[:33], reversed[:33]},
		{"large_duplicates_missing", large, reversed},
	}
	for _, useContextCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("context_cache_%t", useContextCache), func(t *testing.T) {
			reader := ctx
			if !useContextCache {
				reader = ctx.Clone()
				reader.DisableContextCache()
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					input := slices.Clone(tc.ids)
					got, err := get(reader, input...)
					require.NoError(t, err)
					require.Equal(t, tc.ids, input, "GetByIDs must not overwrite its caller's ID slice")
					require.Len(t, got, len(tc.want))
					if tc.ids == nil {
						require.Nil(t, got)
					}
					for i, row := range got {
						require.Equal(t, tc.want[i], row.GetID())
						require.Equal(t, names[tc.want[i]], row.GetName(), "each SQL row must retain its own field storage")
						if useContextCache {
							require.Same(t, seeded[slices.Index(ids, tc.want[i])], row)
						}
					}
					// A later call may not replace a prior result's slice or entity storage.
					_, err = get(reader, ids[10], ids[11])
					require.NoError(t, err)
					for i, row := range got {
						require.Equal(t, tc.want[i], row.GetID())
						require.Equal(t, names[tc.want[i]], row.GetName())
					}
				})
			}
		})
	}
}

func TestGetByIDsMixedCachesAndNegativeHits(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	provider := entities.GenerateEntityWithTimestampsRedisProvider
	contextRow := provider.New(ctx).SetName("context")
	redisRow := provider.New(ctx).SetName("redis")
	sqlRow := provider.New(ctx).SetName("sql")
	require.NoError(t, ctx.Save(contextRow, redisRow, sqlRow))
	negativeID, missingID := ctx.Engine().NextID(), ctx.Engine().NextID()
	warm := ctx.Clone()
	warm.DisableContextCache()
	_, err := provider.GetByIDs(warm, redisRow.GetID(), negativeID)
	require.NoError(t, err)

	reader := ctx.Clone()
	contextHandle, found, err := provider.GetByID(reader, contextRow.GetID())
	require.NoError(t, err)
	require.True(t, found)
	sqlLog := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sqlLog, fluxaorm.QueryLoggerOptions{MySQL: true})
	input := []uint64{negativeID, sqlRow.GetID(), contextRow.GetID(), redisRow.GetID(), missingID, sqlRow.GetID(), negativeID}
	before := slices.Clone(input)
	got, err := provider.GetByIDs(reader, input...)
	require.NoError(t, err)
	require.Equal(t, before, input)
	require.Len(t, got, 3)
	require.Equal(t, []uint64{sqlRow.GetID(), contextRow.GetID(), redisRow.GetID()},
		[]uint64{got[0].GetID(), got[1].GetID(), got[2].GetID()})
	require.Equal(t, []string{"sql", "context", "redis"}, []string{got[0].GetName(), got[1].GetName(), got[2].GetName()})
	require.Same(t, contextHandle, got[1])
	require.Equal(t, 1, countSelects(sqlLog))
	for _, entry := range sqlLog.Logs {
		query, _ := entry["query"].(string)
		require.NotContains(t, query, strconv.FormatUint(negativeID, 10), "negative cache entries must not be queried in SQL")
		require.NotContains(t, query, strconv.FormatUint(redisRow.GetID(), 10), "Redis hits must not be queried in SQL")
	}
	// A new cache-disabled context proves the SQL fill and the new negative entry.
	probe := ctx.Clone()
	probe.DisableContextCache()
	probeLog := &fluxaorm.MockLogHandler{}
	probe.RegisterQueryLogger(probeLog, fluxaorm.QueryLoggerOptions{MySQL: true})
	loaded, err := provider.GetByIDs(probe, input...)
	require.NoError(t, err)
	require.Len(t, loaded, 3)
	require.Zero(t, countSelects(probeLog))
}

func TestGetByIDsTransactionBypassesRedis(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	provider := entities.GenerateEntityWithTimestampsRedisProvider
	row := provider.New(ctx).SetName("cached")
	require.NoError(t, ctx.Save(row))
	warm := ctx.Clone()
	warm.DisableContextCache()
	_, err := provider.GetByIDs(warm, row.GetID())
	require.NoError(t, err)
	reader := ctx.Clone()
	reader.DisableContextCache()
	redisLog := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(redisLog, fluxaorm.QueryLoggerOptions{Redis: true})
	require.NoError(t, reader.Transaction(func(tx fluxaorm.Context) error {
		_, err := tx.DB(fluxaorm.DefaultPoolCode).Exec(tx,
			"UPDATE `generateEntityWithTimestampsRedis` SET `Name` = ? WHERE `ID` = ?", "transaction", row.GetID())
		if err != nil {
			return err
		}
		rows, err := provider.GetByIDs(tx, row.GetID(), row.GetID())
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "transaction", rows[0].GetName())
		require.Empty(t, redisLog.Logs)
		return nil
	}))
}

func TestGetByIDsPropagatesReadErrors(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	expected := errors.New("get-by-ids query failed")
	db := ctx.Engine().DB(fluxaorm.DefaultPoolCode)
	original := db.GetDBClient()
	db.SetMockDBClient(&fluxaorm.MockDBClient{OriginDB: original,
		QueryMock: func(string, ...any) (*sql.Rows, error) { return nil, expected },
	})
	defer db.SetMockDBClient(original)
	rows, err := entities.GenerateEntityWithTimestampsProvider.GetByIDs(ctx, 123)
	require.ErrorIs(t, err, expected)
	require.Nil(t, rows)
	// A query can succeed while Scan fails; the shared destination slice must
	// still propagate that failure instead of returning a partially filled row.
	db.SetMockDBClient(&fluxaorm.MockDBClient{OriginDB: original,
		QueryMock: func(string, ...any) (*sql.Rows, error) { return original.Query("SELECT 123") },
	})
	rows, err = entities.GenerateEntityWithTimestampsProvider.GetByIDs(ctx, 123)
	require.Error(t, err)
	require.Nil(t, rows)
}

func TestGetByIDsPropagatesRedisErrors(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	rows, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByIDs(ctx.CloneWithContext(canceled), 123)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, rows)
}
