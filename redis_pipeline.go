package fluxaorm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPipeLine struct {
	ctx         Context
	r           *redisCache
	pool        string
	pipeLine    redis.Pipeliner
	commands    int
	metricsGets []*PipeLineGet
}

func (rp *RedisPipeLine) LPush(key string, values ...any) {
	rp.commands++
	rp.pipeLine.LPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) RPush(key string, values ...any) {
	rp.commands++
	rp.pipeLine.RPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) Del(key ...string) {
	rp.commands++
	rp.pipeLine.Del(rp.ctx.Context(), key...)
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	res := &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.ctx.Context(), key)}
	_, hasMetrics := rp.ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		rp.metricsGets = append(rp.metricsGets, res)
	}
	return res
}

func (rp *RedisPipeLine) LRange(key string, start, stop int64) *PipeLineSlice {
	result := rp.LRangeValue(key, start, stop)
	return &result
}

// LRangeValue queues LRANGE and returns its result handle by value, allowing
// generated batch readers to store handles without allocating a wrapper per key.
func (rp *RedisPipeLine) LRangeValue(key string, start, stop int64) PipeLineSlice {
	rp.commands++
	return PipeLineSlice{p: rp, cmd: rp.pipeLine.LRange(rp.ctx.Context(), key, start, stop)}
}

// LRangeBatchInto queues one LRANGE per key and fills caller-owned result handles.
// The result and key slices must have equal lengths. Commands keep independent
// results after Exec; their temporary command storage is shared within the batch.
func (rp *RedisPipeLine) LRangeBatchInto(results []PipeLineSlice, keys []string, start, stop int64) {
	if len(results) != len(keys) {
		panic("fluxaorm: LRANGE batch result and key lengths differ")
	}
	if len(keys) == 0 {
		return
	}
	type entry struct {
		command redis.StringSliceCmd
		args    [4]any
	}
	entries := make([]entry, len(keys))
	commands := make([]redis.Cmder, len(keys))
	// Full-list reads use -1. Keep that boxed constant instead of allocating
	// another interface value for the common upper bound on every batch.
	var startArg, stopArg any = start, int64(-1)
	if stop != -1 {
		stopArg = stop
	}
	ctx := rp.ctx.Context()
	for i, key := range keys {
		item := &entries[i]
		item.args = [4]any{"lrange", key, startArg, stopArg}
		// Construct every command in its final location before queuing pointers.
		item.command = *redis.NewStringSliceCmd(ctx, item.args[:]...)
		commands[i] = &item.command
		results[i] = PipeLineSlice{p: rp, cmd: &item.command}
	}
	rp.commands += len(keys)
	_ = rp.pipeLine.BatchProcess(ctx, commands...)
}

func (rp *RedisPipeLine) Set(key string, value any, expiration time.Duration) {
	rp.commands++
	rp.pipeLine.Set(rp.ctx.Context(), key, value, expiration)
}

func (rp *RedisPipeLine) SAdd(key string, members ...any) {
	rp.commands++
	rp.pipeLine.SAdd(rp.ctx.Context(), key, members...)
}

func (rp *RedisPipeLine) SRem(key string, members ...any) {
	rp.commands++
	rp.pipeLine.SRem(rp.ctx.Context(), key, members...)
}

func (rp *RedisPipeLine) MSet(pairs ...any) {
	rp.commands++
	rp.pipeLine.MSet(rp.ctx.Context(), pairs...)
}

func (rp *RedisPipeLine) Expire(key string, expiration time.Duration) *PipeLineBool {
	rp.commands++
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(rp.ctx.Context(), key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(key, field string, incr int64) *PipeLineInt {
	rp.commands++
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(rp.ctx.Context(), key, field, incr)}
}

func (rp *RedisPipeLine) HSet(key string, values ...any) {
	rp.commands++
	rp.pipeLine.HSet(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) HDel(key string, values ...string) {
	rp.commands++
	rp.pipeLine.HDel(rp.ctx.Context(), key, values...)
}

// discard drops staged commands without running them, so a rolled-back
// transaction cannot have its search-index writes published by a later Exec.
func (rp *RedisPipeLine) discard() {
	if rp.commands == 0 {
		return
	}
	rp.pipeLine = rp.r.client.Pipeline()
	rp.commands = 0
	rp.metricsGets = nil
}

func (rp *RedisPipeLine) Exec(ctx Context) (response []redis.Cmder, err error) {
	if rp.commands == 0 {
		return make([]redis.Cmder, 0), nil
	}
	hasLog, loggers := rp.ctx.getRedisLoggers()
	start := time.Now()
	// go-redis detaches the queued commands before executing them, including
	// on errors. Reuse the empty pipeline; previous result handles stay valid.
	res, err := rp.pipeLine.Exec(rp.ctx.Context())
	end := time.Since(start)
	if err != nil && errors.Is(err, redis.Nil) {
		err = nil
	}
	if hasLog {
		query := ""
		for i, v := range res {
			if i > 0 {
				query += "\n"
			}
			query += "\u001B[38;2;255;255;155m"
			query += formatRedisCommandLog(v)
			if v.Err() != nil {
				query += " " + fmt.Sprintf(strings.TrimRight(errorTemplate, "\n"), v.Err())
			}
		}
		fillLogFields(ctx, loggers, rp.pool, sourceRedis, "PIPELINE EXEC", query, &end, false, nil)
	}
	if rp.fillMetrics(ctx, end, res, err) {
		rp.metricsGets = make([]*PipeLineGet, 0)
	}
	rp.commands = 0
	return res, err
}

func (rp *RedisPipeLine) fillMetrics(ctx Context, end time.Duration, res []redis.Cmder, err error) bool {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	endSingle := end.Seconds() / float64(rp.commands)
	if hasMetrics {
		i := 0
		for _, v := range res {
			isMiss := false
			isSet := false
			operation := metricsOperationKey
			switch v.Name() {
			case "get":
				_, isMiss, _ = rp.metricsGets[i].Result()
				isMiss = !isMiss
				i++
				break
			case "set", "del", "mset", "expire":
				isSet = true
				break
			case "lpush", "rpush", "lset":
				isSet = true
				operation = metricsOperationList
				break
			case "lrange":
				operation = "list"
				break
			case "sadd", "srem":
				operation = metricsOperationSet
				isSet = true
				break
			case "hincrby", "hset", "hdel":
				operation = metricsOperationHash
				isSet = true
				break
			case "xadd":
				operation = metricsOperationStream
				isSet = true
				break
			}
			setValue := "0"
			if isSet {
				setValue = "1"
			}
			missValue := "0"
			if isMiss {
				missValue = "1"
			}
			metrics.queriesRedis.WithLabelValues(operation, rp.r.config.GetCode(), setValue, missValue, "1", ctx.getMetricsSourceTag()).Observe(endSingle)
		}
		if err != nil {
			metrics.queriesRedisErrors.WithLabelValues(rp.r.config.GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
		return true
	}
	return false
}

type PipeLineGet struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineGet) Result() (value string, has bool, err error) {
	val, err := c.cmd.Result()
	if errors.Is(err, redis.Nil) {
		return val, false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

type PipeLineString struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineString) Result() (string, error) {
	return c.cmd.Result()
}

type PipeLineSlice struct {
	p   *RedisPipeLine
	cmd *redis.StringSliceCmd
}

func (c *PipeLineSlice) Result() ([]string, error) {
	return c.cmd.Result()
}

type PipeLineInt struct {
	p   *RedisPipeLine
	cmd *redis.IntCmd
}

func (c *PipeLineInt) Result() (int64, error) {
	return c.cmd.Result()
}

type PipeLineBool struct {
	p   *RedisPipeLine
	cmd *redis.BoolCmd
}

func (c *PipeLineBool) Result() (bool, error) {
	return c.cmd.Result()
}
