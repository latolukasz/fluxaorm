package test_generate

import (
	"strconv"
	"strings"
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
