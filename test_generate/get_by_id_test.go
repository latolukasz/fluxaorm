package test_generate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/require"
)

func TestGetByIDUint64Boundaries(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		checkGetByIDUint64(t, generateEntityWithTimestamps{}, entities.GenerateEntityWithTimestampsProvider.TableName(),
			entities.GenerateEntityWithTimestampsProvider.GetByID)
	})
	t.Run("Redis", func(t *testing.T) {
		checkGetByIDUint64(t, generateEntityWithTimestampsRedis{}, entities.GenerateEntityWithTimestampsRedisProvider.TableName(),
			entities.GenerateEntityWithTimestampsRedisProvider.GetByID)
	})
}

func checkGetByIDUint64[T getByIDsTestEntity](t *testing.T, fixture any, table string,
	get func(fluxaorm.Context, uint64) (T, bool, error),
) {
	t.Helper()
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), fixture)
	ids := []uint64{0, uint64(math.MaxInt64) + 1, math.MaxUint64}
	for _, id := range ids {
		literal := strconv.FormatUint(id, 10)
		_, err := ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx,
			"INSERT INTO `"+table+"` (`ID`,`Name`,`CreatedAt`,`UpdatedAt`) VALUES ("+literal+", ?, ?, ?)",
			literal, "2026-01-02 03:04:05", "2026-01-02 03:04:05")
		require.NoError(t, err)
	}
	reader := ctx.Clone()
	reader.DisableContextCache()
	for index, id := range ids {
		first, found, err := get(reader, id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, id, first.GetID())
		require.Equal(t, strconv.FormatUint(id, 10), first.GetName())
		// A later read must not reuse the first entity's field storage.
		second, found, err := get(reader, id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotSame(t, first, second)
		_, _, err = get(reader, ids[(index+1)%len(ids)])
		require.NoError(t, err)
		require.Equal(t, strconv.FormatUint(id, 10), first.GetName())
		cached := ctx.Clone()
		original, found, err := get(cached, id)
		require.NoError(t, err)
		require.True(t, found)
		repeated, found, err := get(cached, id)
		require.NoError(t, err)
		require.True(t, found)
		require.Same(t, original, repeated)
	}
	_, found, err := get(reader, 123)
	require.NoError(t, err)
	require.False(t, found)
}

func TestGetByIDNegativeCacheAndStamp(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	provider := entities.GenerateEntityWithTimestampsRedisProvider
	reader := ctx.Clone()
	reader.DisableContextCache()
	logs := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(logs, fluxaorm.QueryLoggerOptions{MySQL: true})
	missingID := ctx.Engine().NextID()
	row, found, err := provider.GetByID(reader, missingID)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, row)
	require.Equal(t, 1, countSelects(logs))
	logs.Clear()
	row, found, err = provider.GetByID(reader, missingID)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, row)
	require.Zero(t, countSelects(logs), "the second missing read must use the negative cache")
	created := provider.New(ctx).SetName("persisted")
	require.NoError(t, ctx.Save(created))
	row, found, err = provider.GetByID(reader, created.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "persisted", row.GetName())
	key := provider.RedisCachePrefix() + strconv.FormatUint(created.GetID(), 10)
	require.NoError(t, ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LSet(ctx, key, 0, "old-stamp"))
	logs.Clear()
	row, found, err = provider.GetByID(reader, created.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "persisted", row.GetName())
	require.Equal(t, 1, countSelects(logs), "an incompatible Redis stamp must fall through to MySQL")
	logs.Clear()
	_, found, err = provider.GetByID(reader, created.GetID())
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, countSelects(logs), "the fallback must refill the row cache")
}

func TestGetByIDTransactionReadsOwnWrites(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	provider := entities.GenerateEntityWithTimestampsRedisProvider
	row := provider.New(ctx).SetName("cached")
	require.NoError(t, ctx.Save(row))
	reader := ctx.Clone()
	reader.DisableContextCache()
	_, found, err := provider.GetByID(reader, row.GetID())
	require.NoError(t, err)
	require.True(t, found)
	redisLog := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(redisLog, fluxaorm.QueryLoggerOptions{Redis: true})
	abort := errors.New("rollback the parity update")
	err = reader.Transaction(func(tx fluxaorm.Context) error {
		_, err := tx.DB(fluxaorm.DefaultPoolCode).Exec(tx,
			"UPDATE `generateEntityWithTimestampsRedis` SET `Name` = ? WHERE `ID` = ?", "inside", row.GetID())
		if err != nil {
			return err
		}
		loaded, found, err := provider.GetByID(tx, row.GetID())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "inside", loaded.GetName())
		require.Empty(t, redisLog.Logs)
		return abort
	})
	require.ErrorIs(t, err, abort)
}

func TestGetByIDPropagatesQueryAndScanErrors(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestamps{})
	db := ctx.Engine().DB(fluxaorm.DefaultPoolCode)
	original := db.GetDBClient()
	failed := original.QueryRow("SELECT get_by_id_deliberately_missing_column")
	expected := failed.Err()
	require.Error(t, expected)
	db.SetMockDBClient(&fluxaorm.MockDBClient{OriginDB: original,
		QueryRowMock: func(string, ...any) *sql.Row { return failed },
	})
	defer db.SetMockDBClient(original)
	row, found, err := entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, 123)
	require.ErrorIs(t, err, expected)
	require.False(t, found)
	require.Nil(t, row)
	db.SetMockDBClient(&fluxaorm.MockDBClient{OriginDB: original,
		QueryRowMock: func(string, ...any) *sql.Row { return original.QueryRow("SELECT 123") },
	})
	row, found, err = entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, 123)
	require.Error(t, err)
	require.False(t, found)
	require.Nil(t, row)
}

func TestGetByIDPropagatesRedisError(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	row, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(ctx.CloneWithContext(canceled), 123)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, found)
	require.Nil(t, row)
}

type getByIDProtocolValues struct {
	id       uint64
	double   float64
	decimal  float64
	nullInt  sql.NullInt64
	nullReal sql.NullFloat64
	dateTime time.Time
	date     time.Time
	bytes    []byte
	json     sql.NullString
}

func (v *getByIDProtocolValues) destinations(adapt bool) []any {
	values := []any{&v.id, &v.double, &v.decimal, &v.nullInt, &v.nullReal, &v.dateTime, &v.date, &v.bytes, &v.json}
	if adapt {
		for i := range values {
			values[i] = fluxaorm.SQLScanTarget(values[i])
		}
	}
	return values
}

// Parameterized reads use MySQL's binary prepared protocol by default. The
// literal-ID query uses text protocol; compare real driver results with the old
// standard Scan destinations. FLOAT columns retain the parameterized query;
// their separate regression below covers MySQL's text protocol rounding.
func TestGetByIDMySQLProtocolParity(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry())
	db := ctx.Engine().DB(fluxaorm.DefaultPoolCode).GetDBClient().(*sql.DB)
	for _, tc := range []struct {
		name   string
		double string
	}{
		{"decimal", "0.1"},
		{"negative_zero", "-0"},
		{"large", "1.7976931348623157e308"},
		{"small", "4.9406564584124654e-324"},
		{"negative", "-123456.78912345678"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := fmt.Sprintf("SELECT CAST('18446744073709551615' AS UNSIGNED), CAST('%s' AS DOUBLE), CAST('1234567.89123' AS DECIMAL(12,5)), "+
				"CAST(NULL AS SIGNED), CAST(NULL AS DOUBLE), CAST('2026-01-02 03:04:05.123456' AS DATETIME(6)), "+
				"CAST('2026-01-02' AS DATE), X'00ff5c2722', CAST('{\"value\":\"sample\"}' AS JSON)", tc.double)
			prepared, err := db.Prepare(query)
			require.NoError(t, err)
			defer prepared.Close()
			var before, after getByIDProtocolValues
			require.NoError(t, prepared.QueryRow().Scan(before.destinations(false)...))
			require.NoError(t, db.QueryRow(query).Scan(after.destinations(true)...))
			require.Equal(t, math.Float64bits(before.double), math.Float64bits(after.double), "DOUBLE must preserve binary-read values")
			require.Equal(t, before, after)
			require.Equal(t, uint64(math.MaxUint64), after.id)
			require.False(t, after.nullInt.Valid)
			require.False(t, after.nullReal.Valid)
			require.Equal(t, 123456000, after.dateTime.Nanosecond())
		})
	}
	// Unsigned values outside NullInt64's range must remain errors in both
	// protocols, even though the driver's source type in the message differs.
	query := "SELECT CAST('18446744073709551615' AS UNSIGNED)"
	prepared, err := db.Prepare(query)
	require.NoError(t, err)
	defer prepared.Close()
	before := sql.NullInt64{Int64: 99}
	after := before
	require.Error(t, prepared.QueryRow().Scan(&before))
	require.Error(t, db.QueryRow(query).Scan(fluxaorm.SQLScanTarget(&after)))
	require.Equal(t, before, after)
}

type getByIDFloat32 struct {
	ID    uint64
	Value float32
}

type getByIDNullableFloat32 struct {
	ID    uint64 `orm:"redisCache"`
	Value *float32
}

type getByIDFloat32NestedValue struct {
	Value *float32
}

type getByIDNestedFloat32 struct {
	ID     uint64
	Nested getByIDFloat32NestedValue
}

type getByIDDecimalFloat32 struct {
	ID    uint64
	Value float32 `orm:"decimal=12,5"`
}

type getByIDDouble struct {
	ID    uint64
	Value float64
}

// FLOAT's text encoding can lose significant digits. Check the generated
// query choice for all supported field forms so the precision guard cannot
// silently disappear while the normal DOUBLE fixture keeps passing.
func TestGetByIDFloatQueryProtocol(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), getByIDFloat32{}, getByIDNullableFloat32{},
		getByIDNestedFloat32{}, getByIDDecimalFloat32{}, getByIDDouble{})
	directory, err := os.MkdirTemp(".", "get_by_id_protocol_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	require.NoError(t, fluxaorm.Generate(ctx.Engine(), directory))
	for _, tc := range []struct {
		name      string
		parameter bool
	}{
		{"getByIDFloat32", true},
		{"getByIDNullableFloat32", true},
		{"getByIDNestedFloat32", true},
		{"getByIDDecimalFloat32", false},
		{"getByIDDouble", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(directory, tc.name+".go"))
			require.NoError(t, err)
			source := string(content)
			start := strings.Index(source, ") GetByID(")
			require.NotEqual(t, -1, start)
			source = source[start:]
			end := strings.Index(source, "\nfunc ")
			require.NotEqual(t, -1, end)
			source = source[:end]
			if tc.parameter {
				require.Contains(t, source, "WHERE `ID` = ? LIMIT 1\", id)")
				require.NotContains(t, source, "NewWhere(query)")
			} else {
				require.Contains(t, source, "NewWhere(query)")
			}
		})
	}
}

func TestGetByIDMySQLFloatColumnParity(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry())
	db := ctx.Engine().DB(fluxaorm.DefaultPoolCode).GetDBClient().(*sql.DB)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(context.Background(), "CREATE TEMPORARY TABLE get_by_id_float_parity (`ID` BIGINT UNSIGNED PRIMARY KEY, F FLOAT, D DOUBLE)")
	require.NoError(t, err)
	defer conn.ExecContext(context.Background(), "DROP TEMPORARY TABLE get_by_id_float_parity")
	for i, values := range [][2]string{
		{"0.1", "0.1"},
		{"1.2345678", "1.2345678901234567"},
		{"-123456.789", "-123456.78912345678"},
		{"3.4028234e38", "1.7976931348623157e308"},
		{"1.401298464324817e-45", "4.9406564584124654e-324"},
		{"-0", "-0"},
		{"NULL", "NULL"},
	} {
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO get_by_id_float_parity VALUES (%d,%s,%s)", i, values[0], values[1]))
		require.NoError(t, err)
		for _, column := range []string{"F", "D"} {
			query := "SELECT " + column + " FROM get_by_id_float_parity WHERE ID = "
			var before, after sql.NullFloat64
			// A parameter exercises the original binary protocol. FLOAT must
			// keep it; DOUBLE can use a literal and avoid a transient prepare.
			require.NoError(t, conn.QueryRowContext(context.Background(), query+"?", i).Scan(&before))
			if column == "F" {
				err = conn.QueryRowContext(context.Background(), query+"?", i).Scan(fluxaorm.SQLScanTarget(&after))
			} else {
				err = conn.QueryRowContext(context.Background(), query+strconv.Itoa(i)).Scan(fluxaorm.SQLScanTarget(&after))
			}
			require.NoError(t, err)
			require.Equal(t, before.Valid, after.Valid)
			require.Equal(t, math.Float64bits(before.Float64), math.Float64bits(after.Float64), "column %s, value %s", column, values)
		}
	}
}
