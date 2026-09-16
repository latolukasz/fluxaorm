package test_generate

import (
	"context"
	"strings"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
)

var (
	getByID1Sink        *entities.GenerateEntity
	getByID1NoRedisSink *entities.GenerateEntityNoRedis
)

// BenchmarkGetByID1 measures one GetByID call returning one existing entity.
// It shares the populated 27-column fixtures with BenchmarkGetByIDs10, but
// fetches only the first ID. Preparing all ten fixture rows, opening connections,
// creating contexts, reading getters and checking query paths are outside timing.
func BenchmarkGetByID1(b *testing.B) {
	engine, ids, referenceID := prepareGetByIDs10(b)
	ids = ids[:1]
	b.Run("ContextCacheHit", func(b *testing.B) {
		benchmarkGetByID1(b, engine, ids, referenceID, false, 0, 0,
			entities.GenerateEntityProvider.GetByID, &getByID1Sink)
	})
	b.Run("RedisCacheHit", func(b *testing.B) {
		benchmarkGetByID1(b, engine, ids, referenceID, true, 0, 1,
			entities.GenerateEntityProvider.GetByID, &getByID1Sink)
	})
	b.Run("MySQL", func(b *testing.B) {
		benchmarkGetByID1(b, engine, ids, referenceID, true, 1, 0,
			entities.GenerateEntityNoRedisProvider.GetByID, &getByID1NoRedisSink)
	})
}

func benchmarkGetByID1[T getByIDsBenchmarkEntity](b *testing.B, engine fluxaorm.Engine,
	ids []uint64, referenceID uint64, disableContextCache bool, wantSQL, wantRedis int,
	get func(fluxaorm.Context, uint64) (T, bool, error), sink *T,
) {
	ctx := engine.NewContext(context.Background())
	if disableContextCache {
		ctx.DisableContextCache()
	}
	id := ids[0]
	entity, found, err := get(ctx, id)
	if err != nil || !found {
		b.Fatalf("warm GetByID: found=%t, err=%v", found, err)
	}
	checkGetByIDs10(b, []T{entity}, ids, referenceID)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		*sink, found, err = get(ctx, id)
		if err != nil || !found {
			b.Fatalf("GetByID: found=%t, err=%v", found, err)
		}
	}
	b.StopTimer()
	checkGetByIDs10(b, []T{*sink}, ids, referenceID)

	// Check the actual measured context after stopping the timer, so logger and
	// getter allocations cannot contribute to the reported GetByID allocations.
	sqlLog, redisLog := &fluxaorm.MockLogHandler{}, &fluxaorm.MockLogHandler{}
	ctx.RegisterQueryLogger(sqlLog, fluxaorm.QueryLoggerOptions{MySQL: true})
	ctx.RegisterQueryLogger(redisLog, fluxaorm.QueryLoggerOptions{Redis: true})
	entity, found, err = get(ctx, id)
	if err != nil || !found {
		b.Fatalf("verification GetByID: found=%t, err=%v", found, err)
	}
	checkGetByIDs10(b, []T{entity}, ids, referenceID)
	if countSelects(sqlLog) != wantSQL || len(redisLog.Logs) != wantRedis {
		b.Fatalf("wrong read path: got %d SELECTs and %d Redis operations; want %d and %d",
			countSelects(sqlLog), len(redisLog.Logs), wantSQL, wantRedis)
	}
	if wantRedis == 1 {
		operation, _ := redisLog.Logs[0]["operation"].(string)
		query, _ := redisLog.Logs[0]["query"].(string)
		if !strings.EqualFold(operation, "lrange") || strings.Count(strings.ToUpper(query), "LRANGE ") != 1 {
			b.Fatalf("expected one direct Redis LRANGE: operation=%q, query=%q", operation, query)
		}
	}
	var zero T
	*sink = zero
}
