package fluxaorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 15, DefaultPoolCode, nil)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	orm := validatedRegistry.NewContext(context.Background())
	err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)
	testLogger := &MockLogHandler{}
	orm.RegisterQueryLogger(testLogger, false, true, false)

	l := orm.Engine().Redis(DefaultPoolCode).GetLocker()
	lock, has, err := l.Obtain(orm, "test_key", time.Second, 0)
	assert.NoError(t, err)
	assert.True(t, has)
	assert.NotNil(t, lock)
	has, err = lock.Refresh(orm, time.Second)
	assert.NoError(t, err)
	assert.True(t, has)

	_, has, err = l.Obtain(orm, "test_key", time.Second, time.Millisecond*100)
	assert.NoError(t, err)
	assert.False(t, has)

	left, err := lock.TTL(orm)
	assert.NoError(t, err)
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())
	lock.Release(orm) // dragonfly-db fix

	_, has, err = l.Obtain(orm, "test_key", time.Second*10, time.Second*10)
	assert.NoError(t, err)
	assert.True(t, has)

	lock.Release(orm)
	lock.Release(orm)
	has, err = lock.Refresh(orm, time.Second)
	assert.NoError(t, err)
	assert.False(t, has)

	_, _, err = l.Obtain(orm, "test_key", 0, time.Millisecond)
	assert.EqualError(t, err, "ttl must be higher than zero")

	_, _, err = l.Obtain(orm, "test_key", time.Second, time.Second*2)
	assert.EqualError(t, err, "waitTimeout can't be higher than ttl")
}
