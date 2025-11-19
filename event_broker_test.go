package fluxaorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerClean(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 0, DefaultPoolCode, nil)
	registry.RegisterRedisStream("test-stream", "default")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	ctx := validatedRegistry.NewContext(context.Background())
	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	broker := ctx.GetEventBroker()
	eventFlusher := broker.NewFlusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()

	consumer1 := broker.ConsumerSingle(ctx, "test-stream")
	consumer1.Consume(1, time.Millisecond, func(events []Event) {})
	assert.Equal(t, int64(9), ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream"))
	consumer1.Consume(9, time.Millisecond, func(events []Event) {})
	assert.Equal(t, int64(0), ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream"))
	consumer1.Cleanup()

	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()
	consumer1.Consume(100, time.Millisecond, func(events []Event) {})
	consumer1.Cleanup()
	assert.Equal(t, int64(0), ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream"))
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 0, DefaultPoolCode, nil)
	registry.RegisterRedisStream("test-stream", "default")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	ctx := validatedRegistry.NewContext(context.Background())
	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	broker := ctx.GetEventBroker()

	consumer := broker.ConsumerSingle(ctx, "test-stream")
	consumer.Consume(1, time.Millisecond, func(events []Event) {})
	consumer.Consume(1, time.Millisecond, func(events []Event) {})
	consumer.Cleanup()
	type testEvent struct {
		Name string
	}

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	iterations1 := false
	iterations2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.ConsumerMany(ctx, "test-stream")
		consumer.Consume(5, time.Millisecond, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
		})
		consumer.Cleanup()
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.ConsumerMany(ctx, "test-stream")
		consumer.Consume(5, time.Millisecond, func(events []Event) {
			iterations2 = true
		})
		consumer.Cleanup()
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	iterations1 = false
	iterations2 = false
	wg = &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.ConsumerMany(ctx, "test-stream")
		consumer.Consume(5, time.Millisecond, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
			assert.NotEmpty(t, events[0].ID())
		})
		consumer.Cleanup()
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.ConsumerMany(ctx, "test-stream")
		consumer.Consume(5, time.Millisecond, func(events []Event) {
			iterations2 = true
		})
		consumer.Cleanup()
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	consumer = broker.ConsumerMany(ctx, "test-stream")
	assert.PanicsWithError(t, "stop", func() {
		consumer.Consume(3, time.Millisecond, func(events []Event) {
			panic(errors.New("stop"))
		})
	})
	pending := ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", consumerGroupName)
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers[consumer.Name()])
	consumer = broker.ConsumerMany(ctx, "test-stream")
	consumer.AutoClaim(3, time.Millisecond, func(events []Event) {
	})
	pending = ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", consumerGroupName)
	assert.Len(t, pending.Consumers, 0)
}
