package fluxaorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6385", 15, DefaultPoolCode, nil)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	orm := validatedRegistry.NewContext(context.Background())
	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	testLogger := &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, true, false)

	l := orm.Engine().Redis(DefaultPoolCode).GetLocker()
	lock, has, err := l.Obtain(orm, "test_key", time.Second, 0)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has = lock.Refresh(orm, time.Second)
	assert.True(t, has)

	_, has, err = l.Obtain(orm, "test_key", time.Second, time.Millisecond*100)
	assert.NoError(t, err)
	assert.False(t, has)

	left := lock.TTL(orm)
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())
	lock.Release(orm) // dragonfly-db fix

	_, has, err = l.Obtain(orm, "test_key", time.Second*10, time.Second*10)
	assert.NoError(t, err)
	assert.True(t, has)

	lock.Release(orm)
	lock.Release(orm)
	has = lock.Refresh(orm, time.Second)
	assert.False(t, has)

	_, _, err = l.Obtain(orm, "test_key", 0, time.Millisecond)
	assert.EqualError(t, err, "ttl must be higher than zero")

	_, _, err = l.Obtain(orm, "test_key", time.Second, time.Second*2)
	assert.EqualError(t, err, "waitTimeout can't be higher than ttl")
}
