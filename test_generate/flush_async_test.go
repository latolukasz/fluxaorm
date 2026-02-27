package test_generate

import (
	"context"
	"errors"
	"testing"
	"time"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
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
	ctx := fluxaorm.PrepareTablesBeta(t, fluxaorm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})

	// ──────────────────────────────────────────────────────────────────────────
	// Test 1: FlushAsync queues SQL to stream, NOT directly to MySQL
	// ──────────────────────────────────────────────────────────────────────────
	e := newTestEntity(ctx, "async-test")
	assert.NoError(t, ctx.FlushAsync())

	id := e.GetID()

	// Verify entity is NOT in MySQL yet – use a fresh context so context cache is bypassed.
	freshCtx := ctx.Engine().NewContext(context.Background())
	freshCtx.DisableContextCache()
	_, found, err := entities.GenerateEntityNoRedisProvider.GetByID(freshCtx, id)
	assert.NoError(t, err)
	assert.False(t, found, "entity should not be in MySQL before consumer runs")

	// Verify the SQL event landed in the async stream.
	streamLen, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), streamLen)

	// ──────────────────────────────────────────────────────────────────────────
	// Test 2: Consumer processes event → entity appears in MySQL
	// ──────────────────────────────────────────────────────────────────────────
	consumer, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	assert.NoError(t, consumer.Consume(10, time.Millisecond))

	freshCtx2 := ctx.Engine().NewContext(context.Background())
	freshCtx2.DisableContextCache()
	_, found, err = entities.GenerateEntityNoRedisProvider.GetByID(freshCtx2, id)
	assert.NoError(t, err)
	assert.True(t, found, "entity should be in MySQL after consumer runs")

	// Stream should be empty after processing.
	streamLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen)

	// ──────────────────────────────────────────────────────────────────────────
	// Test 3: Deserialization error → event moved to dead-letter
	// ──────────────────────────────────────────────────────────────────────────

	// Publish an event with body that can't be deserialized as asyncSQLOperation.
	// A raw string will fail to unmarshal into the expected struct.
	_, err = ctx.GetEventBroker().Publish(fluxaorm.AsyncSQLStreamName, "invalid-body")
	assert.NoError(t, err)

	consumer3, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	assert.NoError(t, consumer3.Consume(10, time.Millisecond))

	// Main stream should be empty (event was acked and moved to dead-letter).
	streamLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen)

	// Dead-letter stream should contain the failed event.
	deadLen, err := ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLDeadLetterStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deadLen)

	// ──────────────────────────────────────────────────────────────────────────
	// Test 4: Permanent MySQL error (SQL parse error) → event moved to dead-letter
	// ──────────────────────────────────────────────────────────────────────────

	// Publish a valid AsyncSQLOperation with invalid SQL → MySQL returns error 1064
	// (ER_PARSE_ERROR), which is classified as permanent.
	badOp := fluxaorm.AsyncSQLOperation{
		Pool: fluxaorm.DefaultPoolCode,
		Queries: []fluxaorm.AsyncSQLQuery{
			{Q: "THIS IS NOT VALID SQL"},
		},
	}
	_, err = ctx.GetEventBroker().Publish(fluxaorm.AsyncSQLStreamName, badOp)
	assert.NoError(t, err)

	consumer4, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	assert.NoError(t, consumer4.Consume(10, time.Millisecond))

	// Main stream should be empty (event consumed and acked).
	streamLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen)

	// Dead-letter stream should have 2 events now (1 from test 3, 1 from test 4).
	deadLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLDeadLetterStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deadLen)

	// ──────────────────────────────────────────────────────────────────────────
	// Test 5: FlushAsync with no tracked entities is a no-op
	// ──────────────────────────────────────────────────────────────────────────
	assert.NoError(t, ctx.FlushAsync())
	streamLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen)

	// ──────────────────────────────────────────────────────────────────────────
	// Test 6: AutoClaim reclaims events left in pending by a failed consumer
	// ──────────────────────────────────────────────────────────────────────────

	// Flush Redis to get a clean slate.
	assert.NoError(t, ctx.Engine().Redis(fluxaorm.DefaultPoolCode).FlushDB(ctx))

	e6 := newTestEntity(ctx, "autoclaim-test")
	assert.NoError(t, ctx.FlushAsync())

	// Use the raw EventBroker consumer to read the event but fail to process it.
	// This leaves the event in the pending-entry list (PEL) for this consumer.
	rawConsumer, err := ctx.GetEventBroker().ConsumerSingle(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	err = rawConsumer.Consume(10, time.Millisecond, func(events []fluxaorm.Event) error {
		return errors.New("simulated crash")
	})
	assert.Error(t, err)

	// Give the event a moment so the minIdle condition is satisfied for AutoClaim.
	time.Sleep(5 * time.Millisecond)

	// A fresh AsyncSQLConsumer calls AutoClaim and re-processes the pending event.
	claimConsumer, err := ctx.GetAsyncSQLConsumer()
	assert.NoError(t, err)
	assert.NoError(t, claimConsumer.AutoClaim(10, time.Millisecond))

	// Stream should be empty after AutoClaim processed the event.
	streamLen, err = ctx.Engine().Redis(fluxaorm.DefaultPoolCode).XLen(ctx, fluxaorm.AsyncSQLStreamName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), streamLen)

	// The entity should now be in MySQL.
	freshCtx6 := ctx.Engine().NewContext(context.Background())
	freshCtx6.DisableContextCache()
	id6 := e6.GetID()
	_, found, err = entities.GenerateEntityNoRedisProvider.GetByID(freshCtx6, id6)
	assert.NoError(t, err)
	assert.True(t, found, "entity should be in MySQL after AutoClaim processes the event")
}
