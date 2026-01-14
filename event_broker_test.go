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
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterRedisStream("test-stream", "default")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	ctx := validatedRegistry.NewContext(context.Background())
	err = ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	assert.NoError(t, err)
	broker := ctx.GetEventBroker()
	eventFlusher := broker.NewFlusher()
	type testEvent struct {
		Name string
	}
	for i := 1; i <= 10; i++ {
		err = eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
		assert.NoError(t, err)
	}
	err = eventFlusher.Flush()
	assert.NoError(t, err)

	consumer1, err := broker.ConsumerSingle(ctx, "test-stream")
	assert.NoError(t, err)
	err = consumer1.Consume(1, time.Millisecond, func(events []Event) error { return nil })
	assert.NoError(t, err)
	l, err := ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(9), l)
	err = consumer1.Consume(9, time.Millisecond, func(events []Event) error { return nil })
	assert.NoError(t, err)
	l, err = ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), l)
	err = consumer1.Cleanup()
	assert.NoError(t, err)

	for i := 1; i <= 10; i++ {
		err = eventFlusher.Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
		assert.NoError(t, err)
	}
	err = eventFlusher.Flush()
	assert.NoError(t, err)
	err = consumer1.Consume(100, time.Millisecond, func(events []Event) error { return nil })
	assert.NoError(t, err)
	err = consumer1.Cleanup()
	assert.NoError(t, err)
	l, err = ctx.Engine().Redis(DefaultPoolCode).XLen(ctx, "test-stream")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), l)
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterRedisStream("test-stream", "default")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	ctx := validatedRegistry.NewContext(context.Background())
	err = ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	assert.NoError(t, err)
	broker := ctx.GetEventBroker()

	consumer, err := broker.ConsumerSingle(ctx, "test-stream")
	assert.NoError(t, err)
	err = consumer.Consume(1, time.Millisecond, func(events []Event) error { return nil })
	assert.NoError(t, err)
	err = consumer.Consume(1, time.Millisecond, func(events []Event) error { return nil })
	assert.NoError(t, err)
	err = consumer.Cleanup()
	assert.NoError(t, err)
	type testEvent struct {
		Name string
	}

	err = ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	assert.NoError(t, err)
	for i := 1; i <= 10; i++ {
		_, err = ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
		assert.NoError(t, err)
	}
	iterations1 := false
	iterations2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer, err := broker.ConsumerMany(ctx, "test-stream")
		assert.NoError(t, err)
		err = consumer.Consume(5, time.Millisecond, func(events []Event) error {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
			return nil
		})
		assert.NoError(t, err)
		err = consumer.Cleanup()
		assert.NoError(t, err)
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer, err := broker.ConsumerMany(ctx, "test-stream")
		assert.NoError(t, err)
		err = consumer.Consume(5, time.Millisecond, func(events []Event) error {
			iterations2 = true
			return nil
		})
		assert.NoError(t, err)
		err = consumer.Cleanup()
		assert.NoError(t, err)
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	err = ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	assert.NoError(t, err)
	for i := 1; i <= 10; i++ {
		_, err = ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
		assert.NoError(t, err)
	}
	iterations1 = false
	iterations2 = false
	wg = &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer, err := broker.ConsumerMany(ctx, "test-stream")
		assert.NoError(t, err)
		err = consumer.Consume(5, time.Millisecond, func(events []Event) error {
			iterations1 = true
			time.Sleep(time.Millisecond * 100)
			assert.NotEmpty(t, events[0].ID())
			return nil
		})
		assert.NoError(t, err)
		err = consumer.Cleanup()
		assert.NoError(t, err)
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer, err := broker.ConsumerMany(ctx, "test-stream")
		assert.NoError(t, err)
		err = consumer.Consume(5, time.Millisecond, func(events []Event) error {
			iterations2 = true
			return nil
		})
		assert.NoError(t, err)
		err = consumer.Cleanup()
		assert.NoError(t, err)
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	err = ctx.Engine().Redis(DefaultPoolCode).FlushDB(ctx)
	assert.NoError(t, err)
	for i := 1; i <= 10; i++ {
		_, err = ctx.GetEventBroker().Publish("test-stream", testEvent{fmt.Sprintf("a%d", i)})
		assert.NoError(t, err)
	}
	consumer, err = broker.ConsumerMany(ctx, "test-stream")
	assert.NoError(t, err)
	err = consumer.Consume(3, time.Millisecond, func(events []Event) error {
		return errors.New("stop")
	})
	assert.EqualError(t, err, "stop")
	pending, err := ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", consumerGroupName)
	assert.NoError(t, err)
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(3), pending.Consumers[consumer.Name()])
	consumer, err = broker.ConsumerMany(ctx, "test-stream")
	assert.NoError(t, err)
	err = consumer.AutoClaim(3, time.Millisecond, func(events []Event) error {
		return nil
	})
	assert.NoError(t, err)
	pending, err = ctx.Engine().Redis(DefaultPoolCode).XPending(ctx, "test-stream", consumerGroupName)
	assert.NoError(t, err)
	assert.Len(t, pending.Consumers, 0)
}
