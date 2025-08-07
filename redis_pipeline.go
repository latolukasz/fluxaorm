package orm

import (
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
	log      []string
}

func (rp *RedisPipeLine) LPush(key string, values ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LPUSH %s %v", key, values))
	}
	rp.pipeLine.LPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) RPush(key string, values ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("RPUSH %s %v", key, values))
	}
	rp.pipeLine.RPush(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) LSet(key string, index int64, value any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LSET %s %d %v", key, index, value))
	}
	rp.pipeLine.LSet(rp.ctx.Context(), key, index, value)
}

func (rp *RedisPipeLine) Del(key ...string) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "DEL "+strings.Join(key, " "))
	}
	rp.pipeLine.Del(rp.ctx.Context(), key...)
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, "GET "+key)
	}
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.ctx.Context(), key)}
}

func (rp *RedisPipeLine) LRange(key string, start, stop int64) *PipeLineSlice {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("LRANGE %s %d %d", key, start, stop))
	}
	return &PipeLineSlice{p: rp, cmd: rp.pipeLine.LRange(rp.ctx.Context(), key, start, stop)}
}

func (rp *RedisPipeLine) Set(key string, value any, expiration time.Duration) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("SET %s %v %s", key, value, expiration.String()))
	}
	rp.pipeLine.Set(rp.ctx.Context(), key, value, expiration)
}

func (rp *RedisPipeLine) SAdd(key string, members ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("SADD %s %v", key, members))
	}
	rp.pipeLine.SAdd(rp.ctx.Context(), key, members...)
}

func (rp *RedisPipeLine) SRem(key string, members ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("SREM %s %v", key, members))
	}
	rp.pipeLine.SRem(rp.ctx.Context(), key, members...)
}

func (rp *RedisPipeLine) MSet(pairs ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		rp.log = append(rp.log, "MSET "+message)
	}
	rp.pipeLine.MSet(rp.ctx.Context(), pairs...)
}

func (rp *RedisPipeLine) Expire(key string, expiration time.Duration) *PipeLineBool {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("EXPIRE %s %s", key, expiration.String()))
	}
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(rp.ctx.Context(), key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(key, field string, incr int64) *PipeLineInt {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HINCRBY %s %s %d", key, field, incr))
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(rp.ctx.Context(), key, field, incr)}
}

func (rp *RedisPipeLine) HSet(key string, values ...any) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HSET %s %v", key, values))
	}
	rp.pipeLine.HSet(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) HDel(key string, values ...string) {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("HDEL %s %s", key, strings.Join(values, " ")))
	}
	rp.pipeLine.HDel(rp.ctx.Context(), key, values...)
}

func (rp *RedisPipeLine) XAdd(stream string, values []string) *PipeLineString {
	rp.commands++
	hasLog, _ := rp.ctx.getRedisLoggers()
	if hasLog {
		rp.log = append(rp.log, fmt.Sprintf("XADD %s %s", stream, strings.Join(values, " ")))
	}
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(rp.ctx.Context(), &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec(ctx Context) []redis.Cmder {
	res, err := rp.ExecNoPanic(ctx)
	checkError(err)
	return res
}

func (rp *RedisPipeLine) ExecNoPanic(ctx Context) (response []redis.Cmder, firstErr error) {
	if rp.commands == 0 {
		return make([]redis.Cmder, 0), nil
	}
	hasLog, loggers := rp.ctx.getRedisLoggers()
	start := getNow(hasLog)
	res, err := rp.pipeLine.Exec(rp.ctx.Context())
	rp.pipeLine = rp.r.client.Pipeline()
	if err != nil && err == redis.Nil {
		err = nil
	}
	if hasLog {
		query := ""
		for i, v := range res {
			if i > 0 {
				query += "\n"
			}
			query += "\u001B[38;2;255;255;155m"
			query += v.String()
			if v.Err() != nil {
				query += " " + fmt.Sprintf(strings.TrimRight(errorTemplate, "\n"), v.Err())
			}
		}
		fillLogFields(ctx, loggers, rp.pool, sourceRedis, "PIPELINE EXEC", query, start, false, nil)
	}
	rp.log = nil
	rp.commands = 0
	return res, err
}

type PipeLineGet struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineGet) Result() (value string, has bool) {
	val, err := c.cmd.Result()
	if err == redis.Nil {
		return val, false
	}
	checkError(err)
	return val, true
}

type PipeLineString struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineString) Result() string {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}

type PipeLineSlice struct {
	p   *RedisPipeLine
	cmd *redis.StringSliceCmd
}

func (c *PipeLineSlice) Result() []string {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}

type PipeLineInt struct {
	p   *RedisPipeLine
	cmd *redis.IntCmd
}

func (c *PipeLineInt) Result() int64 {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}

type PipeLineBool struct {
	p   *RedisPipeLine
	cmd *redis.BoolCmd
}

func (c *PipeLineBool) Result() bool {
	val, err := c.cmd.Result()
	checkError(err)
	return val
}
