package fluxaorm

import (
	"context"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisPipelineLRangeBatch(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 15, DefaultPoolCode, nil)
	engine, err := registry.Validate()
	require.NoError(t, err)
	ctx := engine.NewContext(context.Background())
	r := engine.Redis(DefaultPoolCode)
	prefix := t.Name() + ":" + strconv.FormatUint(engine.NextID(), 10)
	firstKey, secondKey, missingKey, badKey := prefix+":one", prefix+":two", prefix+":missing", prefix+":wrong"
	t.Cleanup(func() {
		require.NoError(t, r.Del(ctx, firstKey, secondKey, badKey))
		require.NoError(t, r.Client().Close())
	})
	_, err = r.RPush(ctx, firstKey, "a", "b", "c")
	require.NoError(t, err)
	_, err = r.RPush(ctx, secondKey, "x", "y")
	require.NoError(t, err)
	require.NoError(t, r.Set(ctx, badKey, "not-a-list", 0))
	p := ctx.RedisPipeLine(DefaultPoolCode)

	// A batch appends to already queued work, preserving order and normal
	// go-redis command types, argument types, decoder and logging behavior.
	p.RPush(firstKey, "d")
	keys := []string{secondKey, firstKey, missingKey, secondKey}
	results := make([]PipeLineSlice, len(keys))
	p.LRangeBatchInto(results, keys, 1, -1)
	keys[0] = badKey // queued argument values must not alias the input slice
	queued := p.pipeLine.Cmds()
	require.Len(t, queued, 5)
	for i, key := range []string{secondKey, firstKey, missingKey, secondKey} {
		require.IsType(t, &redis.StringSliceCmd{}, queued[i+1])
		require.Equal(t, []any{"lrange", key, int64(1), int64(-1)}, queued[i+1].Args())
	}
	commands, err := p.Exec(ctx)
	require.NoError(t, err)
	require.Len(t, commands, 5)
	for i, want := range [][]string{{"y"}, {"b", "c", "d"}, {}, {"y"}} {
		values, resultErr := results[i].Result()
		require.NoError(t, resultErr)
		require.Equal(t, want, values)
	}

	// Neither a later batch nor an error may overwrite previous results or
	// replay the successful write staged alongside an erroneous command.
	oldResult := results[1]
	p.RPush(firstKey, "e")
	p.LRangeBatchInto(results[:2], []string{badKey, firstKey}, 0, -1)
	_, err = p.Exec(ctx)
	require.ErrorContains(t, err, "WRONGTYPE")
	_, err = results[0].Result()
	require.ErrorContains(t, err, "WRONGTYPE")
	values, err := results[1].Result()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, values)
	values, err = oldResult.Result()
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c", "d"}, values)
	p.LRangeBatchInto(results[:1], []string{firstKey}, 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err = results[0].Result()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, values)

	for _, tc := range []struct {
		start, stop int64
		want        []string
	}{
		{0, 0, []string{"a"}},
		{-2, -1, []string{"d", "e"}},
		{0, -2, []string{"a", "b", "c", "d"}},
		{9, 20, []string{}},
	} {
		p.LRangeBatchInto(results[:1], []string{firstKey}, tc.start, tc.stop)
		_, err = p.Exec(ctx)
		require.NoError(t, err)
		values, err = results[0].Result()
		require.NoError(t, err)
		require.Equal(t, tc.want, values)
	}

	p.LRangeBatchInto(nil, nil, 0, -1)
	commands, err = p.Exec(ctx)
	require.NoError(t, err)
	require.Empty(t, commands)
	require.Panics(t, func() { p.LRangeBatchInto(nil, []string{firstKey}, 0, -1) })
	require.Zero(t, p.commands)
	require.Empty(t, p.pipeLine.Cmds())

	p.RPush(firstKey, "discarded")
	p.LRangeBatchInto(results[:1], []string{firstKey}, 0, -1)
	p.discard()
	p.LRangeBatchInto(results[:1], []string{firstKey}, 0, -1)
	_, err = p.Exec(ctx)
	require.NoError(t, err)
	values, err = results[0].Result()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, values)
}
