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
	registry.RegisterRedisStream("test-stream", "default", "test-group-1")
	validatedRegistry, err := registry.Validate(1)
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

	consumer1 := broker.Consumer(ctx, "test-group-1")
	consumer1.(*eventsConsumer).blockTime = time.Millisecond
	consumer1.DisableBlockMode()

	consumer1.Consume(1, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	assert.Equal(t, int64(0), ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream"))

	for i := 1; i <= 10; i++ {
		eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()
	consumer1.Consume(100, func(events []Event) {})
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int64(0), ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream"))
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 0, DefaultPoolCode, nil)
	registry.RegisterRedisStream("test-stream", "default", "test-group")
	validatedRegistry, err := registry.Validate(1)
	assert.NoError(t, err)
	ctx := validatedRegistry.NewContext(context.Background())
	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	broker := ctx.GetEventBroker()

	consumer := broker.Consumer(ctx, "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableBlockMode()
	consumer.Consume(1, func(events []Event) {})
	consumer.Consume(1, func(events []Event) {})
	type testEvent struct {
		Name string
	}

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	iterations1 := false
	iterations2 := false
	consumed1 := false
	consumed2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer(ctx, "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableBlockMode()
		consumed1 = consumer.Consume(5, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer(ctx, "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableBlockMode()
		consumed2 = consumer.Consume(5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.False(t, iterations2)
	assert.True(t, consumed1)
	assert.False(t, consumed2)

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	iterations1 = false
	iterations2 = false
	consumed1 = false
	consumed2 = false
	wg = &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer(ctx, "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableBlockMode()
		consumed1 = consumer.ConsumeMany(1, 5, func(events []Event) {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
			assert.NotEmpty(t, events[0].ID())
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer(ctx, "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableBlockMode()
		consumed2 = consumer.ConsumeMany(2, 5, func(events []Event) {
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)
	assert.True(t, consumed1)
	assert.True(t, consumed2)

	ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	for i := 1; i <= 10; i++ {
		ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
	}
	consumer = broker.Consumer(ctx, "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableBlockMode()
	assert.PanicsWithError(t, "stop", func() {
		consumed2 = consumer.ConsumeMany(1, 3, func(events []Event) {
			panic(errors.New("stop"))
		})
	})
	pending := ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-1"])

	consumer.Claim(1, 2)
	pending = ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers["consumer-2"])
	consumer.Claim(7, 2)

	consumer = broker.Consumer(ctx, "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableBlockMode()
}
