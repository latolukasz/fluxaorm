package fluxaorm

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisPipelineReuse(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 15, DefaultPoolCode, nil)
	engine, err := registry.Validate()
	require.NoError(t, err)
	ctx := engine.NewContext(context.Background())
	r := engine.Redis(DefaultPoolCode)
	key := t.Name() + ":" + strconv.FormatUint(engine.NextID(), 10)
	badKey := key + ":wrong-type"
	t.Cleanup(func() {
		require.NoError(t, r.Del(ctx, key, badKey))
		require.NoError(t, r.Client().Close())
	})
	p := ctx.RedisPipeLine(DefaultPoolCode)
	p.RPush(key, "first")
	first := p.LRangeValue(key, 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err := first.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, values)

	// A later execution must neither replay writes nor invalidate old handles.
	p.RPush(key, "second")
	second := p.LRange(key, 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err = second.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, values)
	values, err = first.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, values)
	commands, err := p.Exec(ctx)
	require.NoError(t, err)
	require.Empty(t, commands)

	// A failed batch also consumes its commands; successful sibling writes
	// happen once and the following execution must be independent of the error.
	require.NoError(t, r.Set(ctx, badKey, "not-a-list", 0))
	failed := p.LRangeValue(badKey, 0, -1)
	p.RPush(key, "third")
	_, err = p.Exec(ctx)
	require.ErrorContains(t, err, "WRONGTYPE")
	_, err = failed.Result()
	require.ErrorContains(t, err, "WRONGTYPE")
	afterError := p.LRangeValue(key, 0, -1)
	missing := p.LRangeValue(key+":missing", 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err = afterError.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second", "third"}, values)
	values, err = missing.Result()
	require.NoError(t, err)
	require.Empty(t, values)
	_, err = failed.Result()
	require.ErrorContains(t, err, "WRONGTYPE")

	// Transaction rollback discards queued writes before the next read.
	p.RPush(key, "discarded")
	p.discard()
	afterDiscard := p.LRangeValue(key, 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err = afterDiscard.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second", "third"}, values)
}
