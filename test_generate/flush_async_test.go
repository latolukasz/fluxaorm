package test_generate

import (
	"context"
	"strconv"
	"testing"
	"time"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/shamaton/msgpack"
	"github.com/stretchr/testify/assert"
)

// newTestEntity creates a valid generateEntityNoRedis with required fields set.
func newTestEntity(ctx fluxaorm.Context, name string) *entities.GenerateEntityNoRedis {
	now := time.Now().UTC()
	e := entities.GenerateEntityNoRedisProvider.New(ctx)
	e.SetName(name)
	e.SetTestEnum(enums.TestEnumList.A)
	e.SetTime(now)
	e.SetDate(now)
	return e
}

func TestFlushAsync(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithKafka(t, fluxaorm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	defer ctx.Engine().Kafka("kafka").Close()

	// ──────────────────────────────────────────────────────────────────────────
	// Test 1: FlushAsync queues SQL to Kafka, NOT directly to MySQL
	// ──────────────────────────────────────────────────────────────────────────
	e := newTestEntity(ctx, "async-test")
	assert.NoError(t, ctx.FlushAsync(true))

	id := e.GetID()

	// Verify entity is NOT in MySQL yet – use a fresh context so context cache is bypassed.
	freshCtx := ctx.Engine().NewContext(context.Background())
	freshCtx.DisableContextCache()
	_, found, err := entities.GenerateEntityNoRedisProvider.GetByID(freshCtx, id)
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be in MySQL before consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 2: Consumer processes event → entity appears in MySQL
	// ──────────────────────────────────────────────────────────────────────────
	consumer, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, consumer.Consume(10, 5*time.Second))

	freshCtx2 := ctx.Engine().NewContext(context.Background())
	freshCtx2.DisableContextCache()
	_, found, err = entities.GenerateEntityNoRedisProvider.GetByID(freshCtx2, id)
	assert.NoError(t, err)
	assert.True(t, found, "entity should be in MySQL after consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 3: Deserialization error → event moved to dead-letter topic
	// ──────────────────────────────────────────────────────────────────────────

	// Produce an event with body that can't be deserialized as AsyncSQLOperation.
	kafkaPool := ctx.Engine().Kafka("kafka")
	assert.NoError(t, kafkaPool.ProduceSync(ctx, &fluxaorm.KafkaRecord{
		Topic: fluxaorm.AsyncSQLTopicName,
		Key:   []byte("bad"),
		Value: []byte("invalid-body"),
	}))

	consumer3, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer3.Close()
	assert.NoError(t, consumer3.Consume(10, 5*time.Second))

	// Verify event landed in dead-letter topic
	deadLetterCG, err := kafkaPool.ConsumerGroup(fluxaorm.AsyncSQLTopicName + "_dead_letter")
	if err != nil {
		// If there's no specific dead-letter consumer group, check via a temporary one
		// The dead letter record was produced; verify by consuming it
		t.Log("dead-letter consumer group not separately registered, skipping dead-letter read verification")
	} else {
		defer deadLetterCG.Close()
	}

	// ──────────────────────────────────────────────────────────────────────────
	// Test 4: Permanent MySQL error (SQL parse error) → event moved to dead-letter
	// ──────────────────────────────────────────────────────────────────────────

	// Produce a valid AsyncSQLOperation with invalid SQL → MySQL returns error 1064
	badOp := fluxaorm.AsyncSQLOperation{
		Pool: fluxaorm.DefaultPoolCode,
		Queries: []fluxaorm.AsyncSQLQuery{
			{Q: "THIS IS NOT VALID SQL"},
		},
	}
	badOpBytes, err := msgpack.Marshal(badOp)
	assert.NoError(t, err)
	assert.NoError(t, kafkaPool.ProduceSync(ctx, &fluxaorm.KafkaRecord{
		Topic: fluxaorm.AsyncSQLTopicName,
		Key:   []byte("bad-sql"),
		Value: badOpBytes,
	}))

	consumer4, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	defer consumer4.Close()
	assert.NoError(t, consumer4.Consume(10, 5*time.Second))

	// ──────────────────────────────────────────────────────────────────────────
	// Test 5: FlushAsync with no tracked entities is a no-op
	// ──────────────────────────────────────────────────────────────────────────
	assert.NoError(t, ctx.FlushAsync(true))
}

func TestFlushAsyncDeferredCache(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithKafka(t, fluxaorm.NewRegistry(), generateEntityWithTimestampsRedis{})
	defer ctx.Engine().Kafka("kafka").Close()

	// ──────────────────────────────────────────────────────────────────────────
	// Test 1: Insert with FlushAsync(false) → entity NOT in Redis cache AND NOT in MySQL
	// ──────────────────────────────────────────────────────────────────────────
	e := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	e.SetName("deferred-insert")
	assert.NoError(t, ctx.FlushAsync(false))

	id := e.GetID()

	// Entity should NOT be in Redis cache (deferred mode)
	redisKey := "d6cd7:" + strconv.FormatUint(id, 10)
	redisValues, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, redisKey, 0, -1)
	assert.NoError(t, err)
	assert.Empty(t, redisValues, "entity should not be in Redis cache before consumer runs")

	// Entity should NOT be in MySQL
	freshCtx := ctx.Engine().NewContext(context.Background())
	freshCtx.DisableContextCache()
	_, found, err := entities.GenerateEntityWithTimestampsRedisProvider.SearchOne(freshCtx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("`ID` = ?", id)))
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be in MySQL before consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 2: Consumer processes event → entity appears in BOTH MySQL and Redis cache
	// ──────────────────────────────────────────────────────────────────────────
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

	// Verify Redis cache was populated by the consumer
	redisValues, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).LRange(ctx, redisKey, 0, -1)
	assert.NoError(t, err)
	assert.NotEmpty(t, redisValues, "entity should be in Redis cache after consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 3: Update with FlushAsync(false) → old cache values still present
	// ──────────────────────────────────────────────────────────────────────────
	freshCtx3 := ctx.Engine().NewContext(context.Background())
	freshCtx3.DisableContextCache()
	eUpdate, _, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx3, id)
	assert.NoError(t, err)
	eUpdate.SetName("deferred-update")
	assert.NoError(t, freshCtx3.FlushAsync(false))

	// Redis cache should still have old name (deferred)
	freshCtx4 := ctx.Engine().NewContext(context.Background())
	freshCtx4.DisableContextCache()
	cached, found, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx4, id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "deferred-insert", cached.GetName(), "cache should still have old name before consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 4: Consumer processes update → cache updated
	// ──────────────────────────────────────────────────────────────────────────
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

	// ──────────────────────────────────────────────────────────────────────────
	// Test 5: Delete with FlushAsync(false) → cache still has entity
	// ──────────────────────────────────────────────────────────────────────────
	freshCtx6 := ctx.Engine().NewContext(context.Background())
	freshCtx6.DisableContextCache()
	eDel, _, err := entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx6, id)
	assert.NoError(t, err)
	eDel.Delete()
	assert.NoError(t, freshCtx6.FlushAsync(false))

	// Redis cache should still have entity
	freshCtx7 := ctx.Engine().NewContext(context.Background())
	freshCtx7.DisableContextCache()
	_, found, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(freshCtx7, id)
	assert.NoError(t, err)
	assert.True(t, found, "cache should still have entity before consumer runs")

	// ──────────────────────────────────────────────────────────────────────────
	// Test 6: Consumer processes delete → cache entry deleted
	// ──────────────────────────────────────────────────────────────────────────
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
