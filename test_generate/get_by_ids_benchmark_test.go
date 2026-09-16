package test_generate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/latolukasz/fluxaorm/v2/test_generate/models"
)

var (
	getByIDs10Sink        []*entities.GenerateEntity
	getByIDs10NoRedisSink []*entities.GenerateEntityNoRedis
)

// BenchmarkGetByIDs10 measures one GetByIDs call returning ten existing rows.
// Both fixtures have the same 27 columns and fully populated values, including
// nullable fields, references, a 32-byte blob, a 64-byte comment and JSON.
// Getters (including lazy decoding), context creation, logging and setup are
// outside the timer. MySQL uses the fixture without Redis caching; it does not
// include the Redis miss and cache-fill work of the Redis-enabled fixture.
func BenchmarkGetByIDs10(b *testing.B) {
	engine, ids, referenceID := prepareGetByIDs10(b)
	b.Run("ContextCacheHit", func(b *testing.B) {
		benchmarkGetByIDs10(b, engine, ids, referenceID, false, 0, 0,
			entities.GenerateEntityProvider.GetByIDs, &getByIDs10Sink)
	})
	b.Run("RedisCacheHit", func(b *testing.B) {
		benchmarkGetByIDs10(b, engine, ids, referenceID, true, 0, 1,
			entities.GenerateEntityProvider.GetByIDs, &getByIDs10Sink)
	})
	b.Run("MySQL", func(b *testing.B) {
		benchmarkGetByIDs10(b, engine, ids, referenceID, true, 1, 0,
			entities.GenerateEntityNoRedisProvider.GetByIDs, &getByIDs10NoRedisSink)
	})
}

func benchmarkGetByIDs10[T getByIDsBenchmarkEntity](b *testing.B, engine fluxaorm.Engine,
	ids []uint64, referenceID uint64, disableContextCache bool, wantSQL, wantRedis int,
	get func(fluxaorm.Context, ...uint64) ([]T, error), sink *[]T,
) {
	ctx := engine.NewContext(context.Background())
	if disableContextCache {
		ctx.DisableContextCache()
	}
	// Populate caches and open connections before the measured steady-state reads.
	rows, err := get(ctx, ids...)
	if err != nil {
		b.Fatal(err)
	}
	checkGetByIDs10(b, rows, ids, referenceID)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		*sink, err = get(ctx, ids...)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	checkGetByIDs10(b, *sink, ids, referenceID)

	// Attach loggers only after stopping the timer: they allocate per query.
	// Probe the actual measured context to catch an unintended cache/fallback path.
	sqlLog, redisLog := &fluxaorm.MockLogHandler{}, &fluxaorm.MockLogHandler{}
	ctx.RegisterQueryLogger(sqlLog, fluxaorm.QueryLoggerOptions{MySQL: true})
	ctx.RegisterQueryLogger(redisLog, fluxaorm.QueryLoggerOptions{Redis: true})
	rows, err = get(ctx, ids...)
	if err != nil {
		b.Fatal(err)
	}
	checkGetByIDs10(b, rows, ids, referenceID)
	if countSelects(sqlLog) != wantSQL || len(redisLog.Logs) != wantRedis {
		b.Fatalf("wrong read path: got %d SELECTs and %d Redis operations; want %d and %d",
			countSelects(sqlLog), len(redisLog.Logs), wantSQL, wantRedis)
	}
	if wantRedis == 1 {
		query, _ := redisLog.Logs[0]["query"].(string)
		if strings.Count(strings.ToUpper(query), "LRANGE ") != len(ids) {
			b.Fatalf("expected one Redis pipeline with %d LRANGE commands: %q", len(ids), query)
		}
	}
	*sink = nil
}

func prepareGetByIDs10(b *testing.B) (fluxaorm.Engine, []uint64, uint64) {
	b.Helper()
	dsn := os.Getenv("FLUXAORM_BENCH_MYSQL_DSN")
	redisAddress := os.Getenv("FLUXAORM_BENCH_REDIS_ADDR")
	if dsn == "" || redisAddress == "" {
		b.Skip("set FLUXAORM_BENCH_MYSQL_DSN and FLUXAORM_BENCH_REDIS_ADDR to benchmark services")
	}
	redisDB := 0
	if value := os.Getenv("FLUXAORM_BENCH_REDIS_DB"); value != "" {
		var err error
		redisDB, err = strconv.Atoi(value)
		if err != nil || redisDB < 0 {
			b.Fatal("FLUXAORM_BENCH_REDIS_DB must be a non-negative integer")
		}
	}
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL(dsn, fluxaorm.DefaultPoolCode, &fluxaorm.MySQLOptions{})
	registry.RegisterRedis(redisAddress, redisDB, fluxaorm.DefaultPoolCode, nil)
	registry.RegisterEntity(generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{})
	engine, err := registry.Validate()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := engine.Redis(fluxaorm.DefaultPoolCode).Client().Close(); err != nil {
			b.Error(err)
		}
		if err := engine.DB(fluxaorm.DefaultPoolCode).GetDBClient().(*sql.DB).Close(); err != nil {
			b.Error(err)
		}
	})
	ctx := engine.NewContext(context.Background())
	tables := map[string]bool{
		entities.GenerateEntityProvider.TableName():          true,
		entities.GenerateEntityNoRedisProvider.TableName():   true,
		entities.GenerateReferenceEntityProvider.TableName(): true,
	}
	alters, err := fluxaorm.GetAlters(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for _, alter := range alters {
		if !tables[alter.Table] {
			continue
		}
		if alter.Kind != fluxaorm.AlterKindCreateTable {
			b.Fatalf("benchmark fixture %s needs %s; use a dedicated database with current fixture tables",
				alter.Table, alter.Kind)
		}
		if err := alter.Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}

	referenceID := engine.NextID()
	referenceTable := entities.GenerateReferenceEntityProvider.TableName()
	_, err = ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx,
		"INSERT INTO `"+referenceTable+"` (`ID`,`Name`,`FakeDelete`) VALUES (?, ?, false)", referenceID, "getbyids-reference")
	if err != nil {
		b.Fatal(err)
	}
	cleanupGetByIDsRow(b, ctx, referenceTable, referenceID, "")

	ids := make([]uint64, 10)
	for i := range ids {
		ids[i] = engine.NextID()
		for _, table := range []string{entities.GenerateEntityProvider.TableName(), entities.GenerateEntityNoRedisProvider.TableName()} {
			query := "INSERT INTO `" + table + "` (`ID`,`Age`,`ReferenceRequired`,`ReferenceOptional`,`Tags`,`TagsOptional`," +
				"`Balance`,`Bool`,`Float`,`Time`,`Date`,`Name`,`Comment`,`AgeNullable`,`BalanceNullable`,`TestEnum`," +
				"`TestEnumOptional`,`Byte`,`TestSet`,`TestSetOptional`,`BoolNullable`,`FloatNullable`,`TimeNullable`," +
				"`DateNullable`,`JsonAddress`,`Size`,`TestSubSize`) VALUES (" + strings.TrimSuffix(strings.Repeat("?,", 27), ",") + ")"
			tags := "[" + strconv.FormatUint(referenceID, 10) + "]"
			_, err = ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx, query,
				ids[i], 20+i, referenceID, referenceID, tags, tags,
				-5, true, 12.5, "2026-01-02 03:04:05", "2026-01-02", fmt.Sprintf("getbyids-entity-%02d", i), strings.Repeat("x", 64),
				30, -7, "b", "c", strings.Repeat("b", 32), "a,c", "b,c", true, 3.25,
				"2026-01-02 03:04:05", "2026-01-02", `{"Street":"Benchmark Street","City":"Warsaw","Zip":"00-001"}`, 4, 8)
			if err != nil {
				b.Fatal(err)
			}
			cacheKey := ""
			if table == entities.GenerateEntityProvider.TableName() {
				cacheKey = entities.GenerateEntityProvider.RedisCachePrefix() + strconv.FormatUint(ids[i], 10)
			}
			cleanupGetByIDsRow(b, ctx, table, ids[i], cacheKey)
		}
	}
	return engine, ids, referenceID
}

func cleanupGetByIDsRow(b *testing.B, ctx fluxaorm.Context, table string, id uint64, cacheKey string) {
	b.Helper()
	// Registered only after a successful INSERT, so an ID/index conflict cannot
	// make cleanup delete a pre-existing row. No tables or whole caches are cleared.
	b.Cleanup(func() {
		_, err := ctx.DB(fluxaorm.DefaultPoolCode).Exec(ctx, "DELETE FROM `"+table+"` WHERE `ID` = ?", id)
		if err != nil {
			b.Error(err)
		}
		if cacheKey != "" {
			if err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).Del(ctx, cacheKey); err != nil {
				b.Error(err)
			}
		}
	})
}

type getByIDsBenchmarkEntity interface {
	GetID() uint64
	GetAge() uint64
	GetReferenceRequiredID() uint64
	GetReferenceOptionalID() uint64
	GetTagsIDs() []uint64
	GetTagsOptionalIDs() []uint64
	GetBalance() int64
	GetBool() bool
	GetFloat() float64
	GetTime() time.Time
	GetDate() time.Time
	GetName() string
	GetComment() string
	GetAgeNullable() *uint64
	GetBalanceNullable() *int64
	GetTestEnum() enums.TestEnum
	GetTestEnumOptional() *enums.TestEnum
	GetByte() []uint8
	GetTestSet() []enums.TestEnum
	GetTestSetOptional() []enums.TestEnum
	GetBoolNullable() *bool
	GetFloatNullable() *float64
	GetTimeNullable() *time.Time
	GetDateNullable() *time.Time
	GetJsonAddress() *models.GenerateJsonAddress
	GetSize() uint64
	GetTestSubSize() uint64
}

func checkGetByIDs10[T getByIDsBenchmarkEntity](b *testing.B, rows []T, ids []uint64, referenceID uint64) {
	b.Helper()
	if len(rows) != len(ids) {
		b.Fatalf("GetByIDs returned %d rows, want %d", len(rows), len(ids))
	}
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	date := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	age, balance, boolean, number, enum := uint64(30), int64(-7), true, 3.25, enums.TestEnumList.C
	for i, row := range rows {
		if reflect.ValueOf(row).IsNil() {
			b.Fatalf("GetByIDs returned nil at index %d", i)
		}
		got := []any{row.GetID(), row.GetAge(), row.GetReferenceRequiredID(), row.GetReferenceOptionalID(),
			row.GetTagsIDs(), row.GetTagsOptionalIDs(), row.GetBalance(), row.GetBool(), row.GetFloat(), row.GetTime(), row.GetDate(),
			row.GetName(), row.GetComment(), row.GetAgeNullable(), row.GetBalanceNullable(), row.GetTestEnum(), row.GetTestEnumOptional(),
			row.GetByte(), row.GetTestSet(), row.GetTestSetOptional(), row.GetBoolNullable(), row.GetFloatNullable(),
			row.GetTimeNullable(), row.GetDateNullable(), row.GetJsonAddress(), row.GetSize(), row.GetTestSubSize()}
		want := []any{ids[i], uint64(20 + i), referenceID, referenceID, []uint64{referenceID}, []uint64{referenceID},
			int64(-5), true, 12.5, when, date, fmt.Sprintf("getbyids-entity-%02d", i), strings.Repeat("x", 64), &age, &balance,
			enums.TestEnumList.B, &enum, []byte(strings.Repeat("b", 32)), []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.C},
			[]enums.TestEnum{enums.TestEnumList.B, enums.TestEnumList.C}, &boolean, &number, &when, &date,
			&models.GenerateJsonAddress{Street: "Benchmark Street", City: "Warsaw", Zip: "00-001"}, uint64(4), uint64(8)}
		for column := range want {
			if !reflect.DeepEqual(want[column], got[column]) {
				b.Fatalf("row %d column %d: got %#v, want %#v", i, column, got[column], want[column])
			}
		}
	}
}
