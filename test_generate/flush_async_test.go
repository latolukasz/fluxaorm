package test_generate

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/stretchr/testify/assert"
)

// newTestEntity creates a valid generateEntityNoRedis with required fields set.
func newTestEntity(t *testing.T, ctx fluxaorm.Context, name string) *entities.GenerateEntityNoRedis {
	t.Helper()
	now := time.Now().UTC()
	e := entities.GenerateEntityNoRedisProvider.New(ctx)
	e.SetName(name)
	e.SetTestEnum(enums.TestEnumList.A)
	e.SetTime(now)
	e.SetDate(now)
	return e
}

func TestFlushAsync(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithNats(t, fluxaorm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	defer ctx.Engine().Nats("nats").Close()

	// Test 1: FlushAsync queues SQL to NATS, NOT directly to MySQL
	e := newTestEntity(t, ctx, "async-test")
	assert.NoError(t, ctx.FlushAsync(true))

	id := e.GetID()

	freshCtx := ctx.Engine().NewContext(context.Background())
	freshCtx.DisableContextCache()
	_, found, err := entities.GenerateEntityNoRedisProvider.GetByID(freshCtx, id)
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be in MySQL before consumer runs")

	// Test 2: Consumer processes event → entity appears in MySQL
	consumer, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, consumer.Consume(10, 5*time.Second))

	freshCtx2 := ctx.Engine().NewContext(context.Background())
	freshCtx2.DisableContextCache()
	_, found, err = entities.GenerateEntityNoRedisProvider.GetByID(freshCtx2, id)
	assert.NoError(t, err)
	assert.True(t, found, "entity should be in MySQL after consumer runs")

	// Test 3: Deserialization error → event moved to dead-letter subject
	natsPool := ctx.Engine().Nats("nats")
	badMsg := fluxaorm.NewNatsMessage(fluxaorm.AsyncSQLSubject)
	badMsg.Data = []byte("invalid-body")
	assert.NoError(t, natsPool.Publish(ctx, badMsg))

	consumer3, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer3.Close()
	assert.NoError(t, consumer3.Consume(10, 5*time.Second))

	// Test 4: Permanent MySQL error (SQL parse error) → event moved to dead-letter
	badOp := fluxaorm.AsyncSQLOperation{
		Pool: fluxaorm.DefaultPoolCode,
		Queries: []fluxaorm.AsyncSQLQuery{
			{Q: "THIS IS NOT VALID SQL"},
		},
	}
	badOpBytes, err := json.Marshal(badOp)
	assert.NoError(t, err)
	badSQLMsg := fluxaorm.NewNatsMessage(fluxaorm.AsyncSQLSubject)
	badSQLMsg.Data = badOpBytes
	assert.NoError(t, natsPool.Publish(ctx, badSQLMsg))

	consumer4, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer4.Close()
	assert.NoError(t, consumer4.Consume(10, 5*time.Second))

	// Test 5: FlushAsync with no tracked entities is a no-op
	assert.NoError(t, ctx.FlushAsync(true))
}

func TestFlushAsyncDeferredCache(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithNats(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	defer ctx.Engine().Nats("nats").Close()

	// Test 1: Insert with FlushAsync(false) → entity NOT in Redis cache AND NOT in MySQL
	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("deferred-insert")
	assert.NoError(t, ctx.FlushAsync(false))

	id := e.GetID()

	redisKey := "d6cd7:" + strconv.FormatUint(id, 10)
	redisValues, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, redisKey, 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, redisValues, "entity should not be in Redis cache before consumer runs")

	freshCtx := ctx.Engine().NewContext(context.Background())
	freshCtx.DisableContextCache()
	_, found, err := entities.GenerateEntityWithTimestampsRedisProvider.SearchOne(freshCtx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("`ID` = ?", id)))
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be in MySQL before consumer runs")

	// Test 2: Consumer processes event → entity appears in BOTH MySQL and Redis cache
	consumer, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, consumer.Consume(10, 5*time.Second))

	freshCtx2 := ctx.Engine().NewContext(context.Background())
	freshCtx2.DisableContextCache()
	loaded, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx2, id)
	assert.NoError(t, err)
	assert.True(t, found, "entity should be in MySQL after consumer runs")
	assert.Equal(t, "deferred-insert", loaded.GetName())

	redisValues, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, redisKey, 0, -1)
	assert.NoError(t, err)
	assert.NotEmpty(t, redisValues, "entity should be in Redis cache after consumer runs")

	// Test 3: Update with FlushAsync(false) → old cache values still present
	freshCtx3 := ctx.Engine().NewContext(context.Background())
	freshCtx3.DisableContextCache()
	eUpdate, _, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx3, id)
	assert.NoError(t, err)
	eUpdate.SetName("deferred-update")
	assert.NoError(t, freshCtx3.FlushAsync(false))

	freshCtx4 := ctx.Engine().NewContext(context.Background())
	freshCtx4.DisableContextCache()
	cached, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx4, id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "deferred-insert", cached.GetName(), "cache should still have old name before consumer runs")

	// Test 4: Consumer processes update → cache updated
	consumer2, err := freshCtx3.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer2.Close()
	assert.NoError(t, consumer2.Consume(10, 5*time.Second))

	freshCtx5 := ctx.Engine().NewContext(context.Background())
	freshCtx5.DisableContextCache()
	updated, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx5, id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "deferred-update", updated.GetName(), "cache should have new name after consumer runs")

	// Test 5: Delete with FlushAsync(false) → cache still has entity
	freshCtx6 := ctx.Engine().NewContext(context.Background())
	freshCtx6.DisableContextCache()
	eDel, _, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx6, id)
	assert.NoError(t, err)
	eDel.Delete()
	assert.NoError(t, freshCtx6.FlushAsync(false))

	freshCtx7 := ctx.Engine().NewContext(context.Background())
	freshCtx7.DisableContextCache()
	_, found, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx7, id)
	assert.NoError(t, err)
	assert.True(t, found, "cache should still have entity before consumer runs")

	// Test 6: Consumer processes delete → cache entry deleted
	consumer3, err := freshCtx6.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer3.Close()
	assert.NoError(t, consumer3.Consume(10, 5*time.Second))

	freshCtx8 := ctx.Engine().NewContext(context.Background())
	freshCtx8.DisableContextCache()
	_, found, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx8, id)
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be found after consumer processes delete")
}
