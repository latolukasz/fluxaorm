package test_generate

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/assert"
)

func countSelects(logger *fluxaorm.MockLogHandler) int {
	n := 0
	for _, entry := range logger.Logs {
		if query, ok := entry["query"].(string); ok && strings.HasPrefix(query, "SELECT") {
			n++
		}
	}

	return n
}

// Writes only invalidate the row cache, so the fill happens on read. If it silently stopped
// happening, every read would fall through to MySQL and the cache would be decorative.
func TestRedisCacheServesTheSecondRead(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("Cached")
	assert.NoError(t, ctx.Save(e))

	wantName := e.GetName()
	wantCreated := e.GetCreatedAt()
	wantUpdated := e.GetUpdatedAt()

	// The identity map would answer the second read on its own and prove nothing about Redis.
	reader := ctx.Clone()
	reader.DisableContextCache()

	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})

	first, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, wantName, first.GetName())
	assert.Equal(t, 1, countSelects(sql), "the write invalidated the key, so the first read must miss and hit MySQL")

	key := entities.GenerateEntityWithTimestampsRedisProvider.RedisCachePrefix() + strconv.FormatUint(e.GetID(), 10)
	cached, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, key, 0, -1)
	assert.NoError(t, err)
	// Stamp plus every column of the fixture. Asserting the exact width is what catches a fill that
	// quietly stops writing a column - the getters would just return that column's zero value.
	assert.Len(t, cached, 5, "the miss must leave a complete row behind")

	sql.Clear()

	second, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 0, countSelects(sql), "the second read must be served from Redis, not MySQL")

	// Every column has to survive the round trip through originRedisValues, not just the one asserted
	// above: a fill that drops a column reads back as a zero value with no error anywhere.
	assert.Equal(t, wantName, second.GetName())
	assert.Equal(t, wantCreated.Unix(), second.GetCreatedAt().Unix())
	assert.Equal(t, wantUpdated.Unix(), second.GetUpdatedAt().Unix())
}

// The stamp is what makes a schema change invalidate every cached row at once, so a hit requires it
// to match rather than merely to be present.
func TestRedisCacheHitRequiresMatchingStamp(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("Stamped")
	assert.NoError(t, ctx.Save(e))

	reader := ctx.Clone()
	reader.DisableContextCache()

	_, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)

	redis := ctx.Engine().Redis(fluxaorm.DefaultPoolCode)
	key := entities.GenerateEntityWithTimestampsRedisProvider.RedisCachePrefix() + strconv.FormatUint(e.GetID(), 10)

	assert.NoError(t, redis.LSet(ctx, key, 0, "stale-stamp"))

	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})

	reloaded, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 1, countSelects(sql), "a stamp mismatch must fall through to MySQL")
	assert.Equal(t, "Stamped", reloaded.GetName())
}

// The prefix no longer encodes the column set, so two versions of one entity share a key space and
// each other's invalidations. The stamp is then the only thing separating their layouts, and a
// value list written under the other shape must be treated as a miss — reading it positionally
// would be an out-of-range panic or a silently wrong field.
func TestStampMismatchFallsThroughAndRewrites(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("Shaped")
	assert.NoError(t, ctx.Save(e))

	key := entities.GenerateEntityWithTimestampsRedisProvider.RedisCachePrefix() + strconv.FormatUint(e.GetID(), 10)
	redis := ctx.Engine().Redis(fluxaorm.DefaultPoolCode)
	assert.NoError(t, redis.Del(ctx, key))
	// One element short and stamped by a layout this binary has never seen.
	_, err := redis.RPush(ctx, key, "stamp-from-another-version", "Wrong")
	assert.NoError(t, err)

	reader := ctx.Clone()
	reader.DisableContextCache()
	sql := &fluxaorm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, fluxaorm.QueryLoggerOptions{MySQL: true})

	loaded, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Shaped", loaded.GetName(), "the foreign value list must not be read")
	assert.Equal(t, 1, countSelects(sql), "a stamp mismatch must fall through to MySQL")

	// And the fall-through must leave the key usable for this binary rather than re-missing.
	second := ctx.Clone()
	second.DisableContextCache()
	sql2 := &fluxaorm.MockLogHandler{}
	second.RegisterQueryLogger(sql2, fluxaorm.QueryLoggerOptions{MySQL: true})
	_, _, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(second, e.GetID())
	assert.NoError(t, err)
	assert.Equal(t, 0, countSelects(sql2), "the rewrite must have restored a usable entry")
}

// A cache fill leaves exactly one value list under the key, however many callers race to write it.
//
// The rewrite is a single Lua call rather than a DEL + RPUSH + EXPIRE pipeline: go-redis's plain
// Pipeline is a batch, not a transaction, so nothing in the contract stops another client's
// commands landing between them and leaving k copies under one key. In practice the batch arrives
// in one write and Redis drains a client's buffer before serving the next, so this test passes
// either way — it guards the invariant, it does not demonstrate the race.
func TestConcurrentFillsLeaveOneCopy(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})

	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("Contended")
	assert.NoError(t, ctx.Save(e))

	key := entities.GenerateEntityWithTimestampsRedisProvider.RedisCachePrefix() + strconv.FormatUint(e.GetID(), 10)
	redis := ctx.Engine().Redis(fluxaorm.DefaultPoolCode)

	// One uncontended fill establishes what a single copy looks like.
	assert.NoError(t, redis.Del(ctx, key))
	warm := ctx.Clone()
	warm.DisableContextCache()
	_, _, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(warm, e.GetID())
	assert.NoError(t, err)
	oneCopy, err := redis.LRange(ctx, key, 0, -1)
	assert.NoError(t, err)
	assert.NotEmpty(t, oneCopy)

	assert.NoError(t, redis.Del(ctx, key))

	// Contexts are built up front and every filler waits on one barrier, so the DELs and RPUSHes
	// have the best chance to interleave.
	const fillers = 64
	readers := make([]fluxaorm.Context, fillers)
	for i := range readers {
		readers[i] = ctx.Clone()
		readers[i].DisableContextCache()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < fillers; i++ {
		wg.Add(1)
		go func(reader fluxaorm.Context) {
			defer wg.Done()
			<-start
			_, _, _ = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(reader, e.GetID())
		}(readers[i])
	}
	close(start)
	wg.Wait()

	values, err := redis.LRange(ctx, key, 0, -1)
	assert.NoError(t, err)
	assert.Len(t, values, len(oneCopy), "16 concurrent fillers must leave exactly one value list")
}
