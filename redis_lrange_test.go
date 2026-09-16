package fluxaorm

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Holding completed commands simulates an instrumentation hook that retains
// command metadata; a later LRange must not overwrite its arguments or result.
type lrangeRecordingHook struct {
	commands []redis.Cmder
	failKey  string
	failure  error
}

func (h *lrangeRecordingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *lrangeRecordingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *lrangeRecordingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "lrange" {
			h.commands = append(h.commands, cmd)
			if cmd.Args()[1] == h.failKey {
				return h.failure
			}
		}
		return next(ctx, cmd)
	}
}

func TestRedisLRangeNativeCommandSemantics(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 15, DefaultPoolCode, nil)
	metricsRegistry := prometheus.NewRegistry()
	registry.EnableMetrics(promauto.With(metricsRegistry))
	engine, err := registry.Validate()
	require.NoError(t, err)
	ctx := engine.NewContext(context.Background())
	r := engine.Redis(DefaultPoolCode)
	prefix := t.Name() + ":" + strconv.FormatUint(engine.NextID(), 10)
	key, badKey, failKey := prefix+":list", prefix+":wrong", prefix+":hook-error"
	t.Cleanup(func() {
		require.NoError(t, r.Del(ctx, key, badKey))
		require.NoError(t, r.Client().Close())
	})
	_, err = r.RPush(ctx, key, "first", "", "a\x00b", "żółć", "last")
	require.NoError(t, err)
	require.NoError(t, r.Set(ctx, badKey, "string", 0))
	failure := errors.New("LRANGE hook error")
	hook := &lrangeRecordingHook{failKey: failKey, failure: failure}
	r.Client().AddHook(hook)
	logger := &MockLogHandler{}
	ctx.RegisterQueryLogger(logger, QueryLoggerOptions{Redis: true})
	metrics, ok := engine.Registry().getMetricsRegistry()
	require.True(t, ok)
	metrics.queriesRedis.Reset()
	metrics.queriesRedisErrors.Reset()

	for _, tc := range []struct {
		key         string
		start, stop int64
		want        []string
	}{
		{key, 0, -1, []string{"first", "", "a\x00b", "żółć", "last"}},
		{key, 1, 2, []string{"", "a\x00b"}},
		{key, -2, -1, []string{"żółć", "last"}},
		{key, 0, -2, []string{"first", "", "a\x00b", "żółć"}},
		{key, 0, 0, []string{"first"}},
		{key, math.MinInt64, math.MaxInt64, []string{"first", "", "a\x00b", "żółć", "last"}},
		{key, 20, 30, []string{}},
		{key + ":missing", 0, -1, []string{}},
	} {
		values, readErr := r.LRange(ctx, tc.key, tc.start, tc.stop)
		require.NoError(t, readErr)
		require.Equal(t, tc.want, values)
		cmd := hook.commands[len(hook.commands)-1]
		require.IsType(t, &redis.StringSliceCmd{}, cmd)
		require.Equal(t, []any{"lrange", tc.key, tc.start, tc.stop}, cmd.Args())
		require.NoError(t, cmd.Err())
	}
	values, err := r.LRange(ctx, badKey, 0, -1)
	require.ErrorContains(t, err, "WRONGTYPE")
	require.Nil(t, values)
	require.ErrorContains(t, hook.commands[len(hook.commands)-1].Err(), "WRONGTYPE")
	values, err = r.LRange(ctx, failKey, 0, -1)
	require.ErrorIs(t, err, failure)
	require.Nil(t, values)
	require.ErrorIs(t, hook.commands[len(hook.commands)-1].Err(), failure)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	values, err = r.LRange(ctx.CloneWithContext(canceled), key, 0, -1)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, values)
	values, err = r.LRange(ctx, key, 0, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "", "a\x00b", "żółć", "last"}, values)
	first := hook.commands[0].(*redis.StringSliceCmd)
	require.Equal(t, []any{"lrange", key, int64(0), int64(-1)}, first.Args())
	require.Equal(t, values, first.Val())
	require.NoError(t, first.Err())
	// The canceled clone may carry independent loggers; check the original context.
	require.NotEmpty(t, logger.Logs)
	for _, entry := range logger.Logs {
		require.Equal(t, "lrange", entry["operation"])
	}
	families, err := metricsRegistry.Gather()
	require.NoError(t, err)
	var reads uint64
	var errorsCount float64
	for _, family := range families {
		switch family.GetName() {
		case "fluxaorm_redis_queries_seconds":
			for _, metric := range family.Metric {
				reads += metric.GetHistogram().GetSampleCount()
			}
		case "fluxaorm_redis_queries_errors":
			for _, metric := range family.Metric {
				errorsCount += metric.GetCounter().GetValue()
			}
		}
	}
	require.Equal(t, uint64(12), reads)
	require.Equal(t, float64(3), errorsCount)
}
