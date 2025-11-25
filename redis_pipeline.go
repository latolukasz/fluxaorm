package fluxaorm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPipeLine struct {
	ctx      Context
	r        *redisCache
	pool     string
	pipeLine redis.Pipeliner
	commands int
}

func (rp *RedisPipeLine) LPush(key string, values ...any) {
	rp.commands++
	rp.pipeLine.LPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) RPush(key string, values ...any) {
	rp.commands++
	rp.pipeLine.RPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) LSet(key string, index int64, value any) {
	rp.commands++
	rp.pipeLine.LSet(rp.ctx.Context(), key, index, value)
}

func (rp *RedisPipeLine) Del(key ...string) {
	rp.commands++
	rp.pipeLine.Del(rp.ctx.Context(), key...)
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.ctx.Context(), key)}
}

func (rp *RedisPipeLine) LRange(key string, start, stop int64) *PipeLineSlice {
	rp.commands++
	return &PipeLineSlice{p: rp, cmd: rp.pipeLine.LRange(rp.ctx.Context(), key, start, stop)}
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

func (rp *RedisPipeLine) XAdd(stream string, values []string) *PipeLineString {
	rp.commands++
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(rp.ctx.Context(), &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec(ctx Context) (response []redis.Cmder, err error) {
	if rp.commands == 0 {
		return make([]redis.Cmder, 0), nil
	}
	hasLog, loggers := rp.ctx.getRedisLoggers()
	start := getNow(hasLog)
	res, err := rp.pipeLine.Exec(rp.ctx.Context())
	rp.pipeLine = rp.r.client.Pipeline()
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
		fillLogFields(ctx, loggers, rp.pool, sourceRedis, "PIPELINE EXEC", query, start, false, nil)
	}
	rp.commands = 0
	return res, err
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
