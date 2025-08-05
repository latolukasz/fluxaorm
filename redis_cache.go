package orm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shamaton/msgpack"
)

type RedisCache interface {
	Set(ctx Context, key string, value any, expiration time.Duration)
	MSet(ctx Context, pairs ...any)
	Del(ctx Context, keys ...string)
	HSet(ctx Context, key string, values ...any)
	HDel(ctx Context, key string, keys ...string)
	GetSet(ctx Context, key string, expiration time.Duration, provider func() any) any
	Info(ctx Context, section ...string) string
	GetConfig() RedisPoolConfig
	Get(ctx Context, key string) (value string, has bool)
	Eval(ctx Context, script string, keys []string, args ...any) any
	EvalSha(ctx Context, sha1 string, keys []string, args ...any) (res any, exists bool)
	SetNX(ctx Context, key string, value any, expiration time.Duration) bool
	ScriptExists(ctx Context, sha1 string) bool
	ScriptLoad(ctx Context, script string) string
	LPush(ctx Context, key string, values ...any) int64
	LPop(ctx Context, key string) string
	RPush(ctx Context, key string, values ...any) int64
	LLen(ctx Context, key string) int64
	Exists(ctx Context, keys ...string) int64
	Type(ctx Context, key string) string
	LRange(ctx Context, key string, start, stop int64) []string
	LIndex(ctx Context, key string, index int64) (string, bool)
	LSet(ctx Context, key string, index int64, value any)
	RPop(ctx Context, key string) (value string, found bool)
	BLMove(ctx Context, source, destination, srcPos, destPos string, timeout time.Duration) string
	LMove(ctx Context, source, destination, srcPos, destPos string) string
	LRem(ctx Context, key string, count int64, value any)
	Ltrim(ctx Context, key string, start, stop int64)
	HSetNx(ctx Context, key, field string, value any) bool
	HMGet(ctx Context, key string, fields ...string) map[string]any
	HGetAll(ctx Context, key string) map[string]string
	HGet(ctx Context, key, field string) (value string, has bool)
	HLen(ctx Context, key string) int64
	HIncrBy(ctx Context, key, field string, incr int64) int64
	IncrBy(ctx Context, key string, incr int64) int64
	Incr(ctx Context, key string) int64
	IncrWithExpire(ctx Context, key string, expire time.Duration) int64
	Expire(ctx Context, key string, expiration time.Duration) bool
	ZAdd(ctx Context, key string, members ...redis.Z) int64
	ZRevRange(ctx Context, key string, start, stop int64) []string
	ZRevRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z
	ZRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z
	ZCard(ctx Context, key string) int64
	ZCount(ctx Context, key string, min, max string) int64
	ZScore(ctx Context, key, member string) float64
	MGet(ctx Context, keys ...string) []any
	SAdd(ctx Context, key string, members ...any) int64
	SMembers(ctx Context, key string) []string
	SIsMember(ctx Context, key string, member any) bool
	SCard(ctx Context, key string) int64
	SPop(ctx Context, key string) (string, bool)
	SPopN(ctx Context, key string, max int64) []string
	XTrim(ctx Context, stream string, maxLen int64) (deleted int64)
	XRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage
	XRevRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage
	XInfoStream(ctx Context, stream string) *redis.XInfoStream
	XInfoGroups(ctx Context, stream string) []redis.XInfoGroup
	XGroupCreate(ctx Context, stream, group, start string) (key string, exists bool)
	XGroupCreateMkStream(ctx Context, stream, group, start string) (key string, exists bool)
	XGroupDestroy(ctx Context, stream, group string) int64
	XRead(ctx Context, a *redis.XReadArgs) []redis.XStream
	XDel(ctx Context, stream string, ids ...string) int64
	XGroupDelConsumer(ctx Context, stream, group, consumer string) int64
	XReadGroup(ctx Context, a *redis.XReadGroupArgs) (streams []redis.XStream)
	XPending(ctx Context, stream, group string) *redis.XPending
	XPendingExt(ctx Context, a *redis.XPendingExtArgs) []redis.XPendingExt
	XLen(ctx Context, stream string) int64
	XClaim(ctx Context, a *redis.XClaimArgs) []redis.XMessage
	XClaimJustID(ctx Context, a *redis.XClaimArgs) []string
	XAck(ctx Context, stream, group string, ids ...string) int64
	FlushAll(ctx Context)
	FlushDB(ctx Context)
	GetLocker() *Locker
	Process(ctx Context, cmd redis.Cmder) error
	GetCode() string
	FTList(ctx Context) []string
	FTDrop(ctx Context, index string, dropDocuments bool)
	FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema)
	FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool)
}

type redisCache struct {
	client *redis.Client
	locker *Locker
	config RedisPoolConfig
}

func (r *redisCache) GetSet(ctx Context, key string, expiration time.Duration, provider func() any) any {
	val, has := r.Get(ctx, key)
	if !has {
		userVal := provider()
		encoded, _ := msgpack.Marshal(userVal)
		r.Set(ctx, key, string(encoded), expiration)
		return userVal
	}
	var data any
	_ = msgpack.Unmarshal([]byte(val), &data)
	return data
}

func (r *redisCache) Info(ctx Context, section ...string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Info(ctx.Context(), section...).Result()
	checkError(err)
	if hasLogger {
		message := "INFO"
		if len(section) > 0 {
			message += " " + strings.Join(section, " ")
		}
		r.fillLogFields(ctx, "INFO", message, start, false, nil)
	}
	return val
}

func (r *redisCache) GetConfig() RedisPoolConfig {
	return r.config
}

func (r *redisCache) Get(ctx Context, key string) (value string, has bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Get(ctx.Context(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, "GET", "GET "+key, start, true, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(ctx, "GET", "GET "+key, start, false, err)
	}
	return val, true
}

func (r *redisCache) Eval(ctx Context, script string, keys []string, args ...any) any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.Eval(ctx.Context(), script, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVAL "+script+" %v %v", keys, args)
		r.fillLogFields(ctx, "EVAL", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) EvalSha(ctx Context, sha1 string, keys []string, args ...any) (res any, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.EvalSha(ctx.Context(), sha1, keys, args...).Result()
	if hasLogger {
		message := fmt.Sprintf("EVALSHA "+sha1+" %v %v", keys, args)
		r.fillLogFields(ctx, "EVALSHA", message, start, false, err)
	}
	if err != nil && !r.ScriptExists(ctx, sha1) {
		return nil, false
	}
	checkError(err)
	return res, true
}

func (r *redisCache) ScriptExists(ctx Context, sha1 string) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptExists(ctx.Context(), sha1).Result()
	if hasLogger {
		r.fillLogFields(ctx, "SCRIPTEXISTS", "SCRIPTEXISTS "+sha1, start, false, err)
	}
	checkError(err)
	return res[0]
}

func (r *redisCache) ScriptLoad(ctx Context, script string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.ScriptLoad(ctx.Context(), script).Result()
	if hasLogger {
		r.fillLogFields(ctx, "SCRIPTLOAD", "SCRIPTLOAD "+script, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) Set(ctx Context, key string, value any, expiration time.Duration) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Set(ctx.Context(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET %s %v %s", key, value, expiration)
		r.fillLogFields(ctx, "SET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) SetNX(ctx Context, key string, value any, expiration time.Duration) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	isSet, err := r.client.SetNX(ctx.Context(), key, value, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("SET NX %s %v %s", key, value, expiration)
		r.fillLogFields(ctx, "SETNX", message, start, false, err)
	}
	checkError(err)
	return isSet
}

func (r *redisCache) LPush(ctx Context, key string, values ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPush(ctx.Context(), key, values...).Result()
	if hasLogger {
		message := "LPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(ctx, "LPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LPop(ctx Context, key string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LPop(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "LPOP", "LPOP "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) RPush(ctx Context, key string, values ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPush(ctx.Context(), key, values...).Result()
	if hasLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(ctx, "RPUSH", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LLen(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.LLen(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "LLEN", "LLEN", start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Exists(ctx Context, keys ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Exists(ctx.Context(), keys...).Result()
	if hasLogger {
		r.fillLogFields(ctx, "EXISTS", "EXISTS "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Type(ctx Context, key string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Type(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "TYPE", "TYPE "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LRange(ctx Context, key string, start, stop int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	val, err := r.client.LRange(ctx.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LRANGE %s %d %d", key, start, stop)
		r.fillLogFields(ctx, "LRANGE", message, s, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LIndex(ctx Context, key string, index int64) (string, bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	val, err := r.client.LIndex(ctx.Context(), key, index).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if hasLogger {
		message := fmt.Sprintf("LINDEX %s %d", key, index)
		r.fillLogFields(ctx, "LINDEX", message, s, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) LSet(ctx Context, key string, index int64, value any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LSet(ctx.Context(), key, index, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LSET %s %d %v", key, index, value)
		r.fillLogFields(ctx, "LSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) BLMove(ctx Context, source, destination, srcPos, destPos string, timeout time.Duration) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.BLMove(ctx.Context(), source, destination, srcPos, destPos, timeout).Result()
	if hasLogger {
		message := fmt.Sprintf("BLMOVE %s %s %s %s %s", source, destination, srcPos, destPos, timeout)
		r.fillLogFields(ctx, "BLMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) LMove(ctx Context, source, destination, srcPos, destPos string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	value, err := r.client.LMove(ctx.Context(), source, destination, srcPos, destPos).Result()
	if hasLogger {
		message := fmt.Sprintf("LMOVE %s %s %s %s", source, destination, srcPos, destPos)
		r.fillLogFields(ctx, "LMOVE", message, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) RPop(ctx Context, key string) (value string, found bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.RPop(ctx.Context(), key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, "RPOP", "RPOP", start, false, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(ctx, "RPOP", "RPOP", start, false, err)
	}
	return val, true
}

func (r *redisCache) LRem(ctx Context, key string, count int64, value any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.LRem(ctx.Context(), key, count, value).Result()
	if hasLogger {
		message := fmt.Sprintf("LREM %d %v", count, value)
		r.fillLogFields(ctx, "LREM", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Ltrim(ctx Context, key string, start, stop int64) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	_, err := r.client.LTrim(ctx.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("LTRIM %s %d %d", key, start, stop)
		r.fillLogFields(ctx, "LTRIM", message, s, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSet(ctx Context, key string, values ...any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HSet(ctx.Context(), key, values...).Result()
	if hasLogger {
		message := "HSET " + key + " "
		for _, v := range values {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(ctx, "HSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSetNx(ctx Context, key, field string, value any) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.HSetNX(ctx.Context(), key, field, value).Result()
	if hasLogger {
		message := "HSETNX " + key + " " + field + " " + fmt.Sprintf(" %v", value)
		r.fillLogFields(ctx, "HSETNX", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) HDel(ctx Context, key string, fields ...string) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.HDel(ctx.Context(), key, fields...).Result()
	if hasLogger {
		message := "HDEL " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(ctx, "HDEL", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HMGet(ctx Context, key string, fields ...string) map[string]any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HMGet(ctx.Context(), key, fields...).Result()
	results := make(map[string]any, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if hasLogger {
		message := "HMGET " + key + " " + strings.Join(fields, " ")
		r.fillLogFields(ctx, "HMGET", message, start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) HGetAll(ctx Context, key string) map[string]string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HGetAll(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "HGETALL", "HGETALL "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HGet(ctx Context, key, field string) (value string, has bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	misses := false
	start := getNow(hasLogger)
	val, err := r.client.HGet(ctx.Context(), key, field).Result()
	if err == redis.Nil {
		err = nil
		misses = true
	}
	if hasLogger {
		r.fillLogFields(ctx, "HGET", "HGET "+key+" "+field, start, misses, err)
	}
	checkError(err)
	return val, !misses
}

func (r *redisCache) HLen(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HLen(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "HLEN", "HLEN "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HIncrBy(ctx Context, key, field string, incr int64) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.HIncrBy(ctx.Context(), key, field, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("HINCRBY %s %s %d", key, field, incr)
		r.fillLogFields(ctx, "HINCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrBy(ctx Context, key string, incr int64) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.IncrBy(ctx.Context(), key, incr).Result()
	if hasLogger {
		message := fmt.Sprintf("INCRBY %s %d", key, incr)
		r.fillLogFields(ctx, "INCRBY", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Incr(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Incr(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "INCR", "INCR "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrWithExpire(ctx Context, key string, expire time.Duration) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	p := r.client.Pipeline()
	res := p.Incr(ctx.Context(), key)
	p.Expire(ctx.Context(), key, expire)
	_, err := p.Exec(ctx.Context())
	if hasLogger {
		r.fillLogFields(ctx, "INCR_EXPIRE", "INCR EXP "+key+" "+expire.String(), start, false, err)
	}
	checkError(err)
	value, err := res.Result()
	checkError(err)
	return value
}

func (r *redisCache) Expire(ctx Context, key string, expiration time.Duration) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.Expire(ctx.Context(), key, expiration).Result()
	if hasLogger {
		message := fmt.Sprintf("EXPIRE %s %s", key, expiration.String())
		r.fillLogFields(ctx, "EXPIRE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZAdd(ctx Context, key string, members ...redis.Z) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZAdd(ctx.Context(), key, members...).Result()
	if hasLogger {
		message := "ZADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %f %v", v.Score, v.Member)
		}
		r.fillLogFields(ctx, "ZADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRange(ctx Context, key string, start, stop int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRange(ctx.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGE %s %d %d", key, start, stop)
		r.fillLogFields(ctx, "ZREVRANGE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRevRangeWithScores(ctx.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZREVRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(ctx, "ZREVRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	val, err := r.client.ZRangeWithScores(ctx.Context(), key, start, stop).Result()
	if hasLogger {
		message := fmt.Sprintf("ZRANGESCORE %s %d %d", key, start, stop)
		r.fillLogFields(ctx, "ZRANGESCORE", message, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCard(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCard(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "ZCARD", "ZCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCount(ctx Context, key string, min, max string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZCount(ctx.Context(), key, min, max).Result()
	if hasLogger {
		message := fmt.Sprintf("ZCOUNT %s %s %s", key, min, max)
		r.fillLogFields(ctx, "ZCOUNT", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZScore(ctx Context, key, member string) float64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.ZScore(ctx.Context(), key, member).Result()
	if hasLogger {
		message := fmt.Sprintf("ZSCORE %s %s", key, member)
		r.fillLogFields(ctx, "ZSCORE", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) MSet(ctx Context, pairs ...any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.MSet(ctx.Context(), pairs...).Result()
	if hasLogger {
		message := "MSET"
		for _, v := range pairs {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(ctx, "MSET", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) MGet(ctx Context, keys ...string) []any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.MGet(ctx.Context(), keys...).Result()
	results := make([]any, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if hasLogger {
		r.fillLogFields(ctx, "MGET", "MGET "+strings.Join(keys, " "), start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) SAdd(ctx Context, key string, members ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SAdd(ctx.Context(), key, members...).Result()
	if hasLogger {
		message := "SADD " + key
		for _, v := range members {
			message += fmt.Sprintf(" %v", v)
		}
		r.fillLogFields(ctx, "SADD", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SMembers(ctx Context, key string) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SMembers(ctx.Context(), key).Result()
	if hasLogger {
		message := "SMEMBERS " + key
		r.fillLogFields(ctx, "SMEMBERS", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SIsMember(ctx Context, key string, member any) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SIsMember(ctx.Context(), key, member).Result()
	if hasLogger {
		r.fillLogFields(ctx, "SISMEMBER", fmt.Sprintf("SISMEMBER %s %v", key, member), start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SCard(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SCard(ctx.Context(), key).Result()
	if hasLogger {
		r.fillLogFields(ctx, "SCARD", "SCARD "+key, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SPop(ctx Context, key string) (string, bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPop(ctx.Context(), key).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(ctx, "SPOP", "SPOP "+key, start, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) SPopN(ctx Context, key string, max int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	val, err := r.client.SPopN(ctx.Context(), key, max).Result()
	if hasLogger {
		message := fmt.Sprintf("SPOPN %s %d", key, max)
		r.fillLogFields(ctx, "SPOPN", message, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Del(ctx Context, keys ...string) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.Del(ctx.Context(), keys...).Result()
	if hasLogger {
		r.fillLogFields(ctx, "DEL", "DEL "+strings.Join(keys, " "), start, false, err)
	}
	checkError(err)
}

func (r *redisCache) XTrim(ctx Context, stream string, maxLen int64) (deleted int64) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	var err error
	deleted, err = r.client.XTrimMaxLen(ctx.Context(), stream, maxLen).Result()
	if hasLogger {
		message := fmt.Sprintf("XTREAM %s %d", stream, maxLen)
		r.fillLogFields(ctx, "XTREAM", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRangeN(ctx.Context(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(ctx, "XTREAM", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRevRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	deleted, err := r.client.XRevRangeN(ctx.Context(), stream, start, stop, count).Result()
	if hasLogger {
		message := fmt.Sprintf("XREVRANGE %s %s %s %d", stream, start, stop, count)
		r.fillLogFields(ctx, "XREVRANGE", message, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XInfoStream(ctx Context, stream string) *redis.XInfoStream {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoStream(ctx.Context(), stream).Result()
	if hasLogger {
		r.fillLogFields(ctx, "XINFOSTREAM", "XINFOSTREAM "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XInfoGroups(ctx Context, stream string) []redis.XInfoGroup {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XInfoGroups(ctx.Context(), stream).Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil && err.Error() == "ERR no such key" {
		if hasLogger {
			r.fillLogFields(ctx, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
		}
		return make([]redis.XInfoGroup, 0)
	}
	if hasLogger {
		r.fillLogFields(ctx, "XINFOGROUPS", "XINFOGROUPS "+stream, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XGroupCreate(ctx Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreate(ctx.Context(), stream, group, start).Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if hasLogger {
			message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
			r.fillLogFields(ctx, "XGROUPCREATE", message, s, false, err)
		}
		return "OK", true
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCREATE %s %s %s", stream, group, start)
		r.fillLogFields(ctx, "XGROUPCREATE", message, s, false, err)
	}
	checkError(err)
	return res, false
}

func (r *redisCache) XGroupCreateMkStream(ctx Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	res, err := r.client.XGroupCreateMkStream(ctx.Context(), stream, group, start).Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if hasLogger {
		message := fmt.Sprintf("XGROUPCRMKSM %s %s %s", stream, group, start)
		r.fillLogFields(ctx, "XGROUPCREATEMKSTREAM", message, s, false, err)
	}
	checkError(err)
	return res, created
}

func (r *redisCache) XGroupDestroy(ctx Context, stream, group string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XGroupDestroy(ctx.Context(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPCDESTROY %s %s", stream, group)
		r.fillLogFields(ctx, "XGROUPCDESTROY", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XRead(ctx Context, a *redis.XReadArgs) []redis.XStream {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	info, err := r.client.XRead(ctx.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XREAD %s COUNT %d BLOCK %d", strings.Join(a.Streams, " "), a.Count, a.Block)
		r.fillLogFields(ctx, "XREAD", message, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XDel(ctx Context, stream string, ids ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XDel(ctx.Context(), stream, ids...).Result()
	if hasLogger {
		r.fillLogFields(ctx, "XDEL", "XDEL "+stream+" "+strings.Join(ids, " "), start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XGroupDelConsumer(ctx Context, stream, group, consumer string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	deleted, err := r.client.XGroupDelConsumer(ctx.Context(), stream, group, consumer).Result()
	if hasLogger {
		message := fmt.Sprintf("XGROUPDELCONSUMER %s %s %s", stream, group, consumer)
		r.fillLogFields(ctx, "XGROUPDELCONSUMER", message, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XReadGroup(ctx Context, a *redis.XReadGroupArgs) (streams []redis.XStream) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	if hasLogger && a.Block >= 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d BLOCK %s NOACK %v", a.Count, a.Block.String(), a.NoAck)
		r.fillLogFields(ctx, "XREADGROUP", message, start, false, nil)
	}

	var err error
	if a.Block >= 0 {
		ch := make(chan int)
		go func() {
			streams, err = r.client.XReadGroup(ctx.Context(), a).Result()
			close(ch)
		}()
		select {
		case <-ctx.Context().Done():
			return
		case <-ch:
			break
		}
	} else {
		streams, err = r.client.XReadGroup(ctx.Context(), a).Result()
	}

	if err == redis.Nil {
		err = nil
	}
	if hasLogger && a.Block < 0 {
		message := fmt.Sprintf("XREADGROUP %s %s STREAMS %s", a.Group, a.Consumer, strings.Join(a.Streams, " "))
		message += fmt.Sprintf(" COUNT %d NOACK %v", a.Count, a.NoAck)
		r.fillLogFields(ctx, "XREADGROUP", message, start, false, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	checkError(err)
	return streams
}

func (r *redisCache) XPending(ctx Context, stream, group string) *redis.XPending {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPending(ctx.Context(), stream, group).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDING %s %s", stream, group)
		r.fillLogFields(ctx, "XPENDING", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XPendingExt(ctx Context, a *redis.XPendingExtArgs) []redis.XPendingExt {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XPendingExt(ctx.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XPENDINGEXT %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" START %s END %s COUNT %d IDLE %s", a.Start, a.End, a.Count, a.Idle.String())
		r.fillLogFields(ctx, "XPENDINGEXT", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XLen(ctx Context, stream string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	l, err := r.client.XLen(ctx.Context(), stream).Result()
	if hasLogger {
		r.fillLogFields(ctx, "XLEN", "XLEN "+stream, start, false, err)
	}
	checkError(err)
	return l
}

func (r *redisCache) XClaim(ctx Context, a *redis.XClaimArgs) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaim(ctx.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIM %s %s %s", a.Stream, a.Group, a.Consumer)
		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(ctx, "XCLAIM", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XClaimJustID(ctx Context, a *redis.XClaimArgs) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XClaimJustID(ctx.Context(), a).Result()
	if hasLogger {
		message := fmt.Sprintf("XCLAIMJUSTID %s %s %s", a.Stream, a.Group, a.Consumer)

		message += fmt.Sprintf(" MINIDLE %s MESSAGES ", a.MinIdle.String()) + strings.Join(a.Messages, " ")
		r.fillLogFields(ctx, "XCLAIMJUSTID", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XAck(ctx Context, stream, group string, ids ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.XAck(ctx.Context(), stream, group, ids...).Result()
	if hasLogger {
		message := fmt.Sprintf("XACK %s %s %s", stream, group, strings.Join(ids, " "))
		r.fillLogFields(ctx, "XACK", message, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) FTList(ctx Context) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.FT_List(ctx.Context()).Result()
	if hasLogger {
		r.fillLogFields(ctx, "FT_LIST", "FT_LIST", start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) FTDrop(ctx Context, index string, dropDocuments bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	if dropDocuments {
		_, err := r.client.FTDropIndexWithArgs(ctx.Context(), index, &redis.FTDropIndexOptions{DeleteDocs: true}).Result()
		if hasLogger {
			r.fillLogFields(ctx, "FT.DROP", "FT.DROP DD "+index, start, false, err)
		}
		checkError(err)
		return
	}
	_, err := r.client.FTDropIndex(ctx.Context(), index).Result()
	if hasLogger {
		r.fillLogFields(ctx, "FT.DROP", "FT.DROP "+index, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res, err := r.client.FTInfo(ctx.Context(), index).RawResult()
	if res == nil {
		err = nil
		if hasLogger {
			r.fillLogFields(ctx, "FT_INFO", "FT_INFO "+index, start, true, err)
		}
		return nil, false
	}
	checkError(err)
	info = &redis.FTInfoResult{}
	asMap := res.(map[any]any)
	info.SortableValuesSizeMB = asMap["sortable_values_size_mb"].(float64)
	info.TotalIndexMemorySzMB = asMap["total_index_memory_sz_mb"].(float64)
	info.NumDocs = int(asMap["num_docs"].(int64))
	info.InvertedSzMB = asMap["inverted_sz_mb"].(float64)
	info.BytesPerRecordAvg = strconv.FormatFloat(asMap["bytes_per_record_avg"].(float64), 'f', 2, 64)
	info.Indexing = int(asMap["indexing"].(int64))
	info.SortableValuesSizeMB = asMap["sortable_values_size_mb"].(float64)
	indexOptions := make([]string, len(asMap["index_options"].([]any)))
	for i, v := range asMap["index_options"].([]any) {
		indexOptions[i] = v.(string)
	}
	info.IndexOptions = indexOptions
	info.DocTableSizeMB = asMap["doc_table_size_mb"].(float64)
	info.OffsetVectorsSzMB = asMap["offset_vectors_sz_mb"].(float64)
	info.GeoshapesSzMB = asMap["geoshapes_sz_mb"].(float64)
	info.NumRecords = int(asMap["num_records"].(int64))
	info.TotalIndexMemorySzMB = asMap["total_index_memory_sz_mb"].(float64)
	info.PercentIndexed = asMap["percent_indexed"].(float64)
	info.Cleaning = int(asMap["cleaning"].(int64))
	info.IndexName = asMap["index_name"].(string)
	info.TextOverheadSzMB = asMap["text_overhead_sz_mb"].(float64)
	indexErrors := asMap["Index Errors"].(map[any]any)
	info.IndexErrors = redis.IndexErrors{
		IndexingFailures:     int(indexErrors["indexing failures"].(int64)),
		LastIndexingError:    indexErrors["last indexing error"].(string),
		LastIndexingErrorKey: indexErrors["last indexing error key"].(string),
	}
	info.NumTerms = int(asMap["num_terms"].(int64))
	info.OffsetsPerTermAvg = strconv.FormatFloat(asMap["offsets_per_term_avg"].(float64), 'f', 2, 64)
	info.KeyTableSizeMB = asMap["key_table_size_mb"].(float64)
	info.TagOverheadSzMB = asMap["tag_overhead_sz_mb"].(float64)
	info.VectorIndexSzMB = asMap["vector_index_sz_mb"].(float64)
	info.MaxDocID = int(asMap["num_terms"].(int64))
	info.TotalInvertedIndexBlocks = int(asMap["total_inverted_index_blocks"].(int64))
	info.RecordsPerDocAvg = strconv.FormatFloat(asMap["records_per_doc_avg"].(float64), 'f', 2, 64)
	info.OffsetBitsPerRecordAvg = strconv.FormatFloat(asMap["offset_bits_per_record_avg"].(float64), 'f', 2, 64)
	info.HashIndexingFailures = int(asMap["hash_indexing_failures"].(int64))
	info.NumberOfUses = int(asMap["number_of_uses"].(int64))
	info.TotalIndexingTime = int(asMap["total_indexing_time"].(float64))
	cursorStats := asMap["cursor_stats"].(map[any]any)
	info.CursorStats = redis.CursorStats{
		GlobalIdle:    int(cursorStats["global_idle"].(int64)),
		GlobalTotal:   int(cursorStats["global_total"].(int64)),
		IndexCapacity: int(cursorStats["index_capacity"].(int64)),
		IndexTotal:    int(cursorStats["index_total"].(int64)),
	}
	dialectStats := asMap["dialect_stats"].(map[any]any)
	info.DialectStats = map[string]int{
		"dialect_1": int(dialectStats["dialect_1"].(int64)),
		"dialect_2": int(dialectStats["dialect_2"].(int64)),
		"dialect_3": int(dialectStats["dialect_3"].(int64)),
		"dialect_4": int(dialectStats["dialect_4"].(int64)),
	}
	gcStats := asMap["gc_stats"].(map[any]any)
	info.GCStats = redis.GCStats{
		BytesCollected:       int(gcStats["bytes_collected"].(float64)),
		TotalMsRun:           int(gcStats["total_ms_run"].(float64)),
		TotalCycles:          int(gcStats["total_cycles"].(float64)),
		AverageCycleTimeMs:   strconv.FormatFloat(gcStats["average_cycle_time_ms"].(float64), 'f', 2, 64),
		LastRunTimeMs:        int(gcStats["last_run_time_ms"].(float64)),
		GCNumericTreesMissed: int(gcStats["gc_numeric_trees_missed"].(float64)),
		GCBlocksDenied:       int(gcStats["gc_blocks_denied"].(float64)),
	}
	fieldStatistics := asMap["field statistics"].([]any)
	info.FieldStatistics = make([]redis.FieldStatistic, len(fieldStatistics))
	for i, v := range fieldStatistics {
		row := v.(map[any]any)
		fieldErrors := row["Index Errors"].(map[any]any)
		info.FieldStatistics[i] = redis.FieldStatistic{
			Identifier: row["identifier"].(string),
			Attribute:  row["attribute"].(string),
			IndexErrors: redis.IndexErrors{
				IndexingFailures:     int(fieldErrors["indexing failures"].(int64)),
				LastIndexingError:    fieldErrors["last indexing error"].(string),
				LastIndexingErrorKey: fieldErrors["last indexing error key"].(string),
			},
		}
	}
	indexDef := asMap["index_definition"].(map[any]any)
	prefixes := indexDef["prefixes"].([]any)
	prefixesAsStrings := make([]string, len(prefixes))
	for i, v := range prefixes {
		prefixesAsStrings[i] = v.(string)
	}
	info.IndexDefinition = redis.IndexDefinition{
		KeyType:      indexDef["key_type"].(string),
		Prefixes:     prefixesAsStrings,
		DefaultScore: indexDef["default_score"].(float64),
	}
	attributes := asMap["attributes"].([]any)
	info.Attributes = make([]redis.FTAttribute, len(attributes))
	for i, v := range attributes {
		row := v.(map[any]any)
		weight := float64(0)
		rowWeight, has := row["WEIGHT"]
		if has {
			weight = rowWeight.(float64)
		}
		flags := row["flags"].([]any)
		info.Attributes[i] = redis.FTAttribute{
			Identifier:      row["identifier"].(string),
			Attribute:       row["attribute"].(string),
			Type:            row["type"].(string),
			Weight:          weight,
			Sortable:        slices.Contains(flags, "SORTABLE"),
			NoStem:          slices.Contains(flags, "NOSTEM"),
			NoIndex:         slices.Contains(flags, "NOINDEX"),
			UNF:             slices.Contains(flags, "UNF"),
			PhoneticMatcher: "",
			CaseSensitive:   slices.Contains(flags, "CASESENSITIVE"),
			WithSuffixtrie:  slices.Contains(flags, "WITHSUFFIXTRIE"),
		}
	}

	if hasLogger {
		r.fillLogFields(ctx, "FT_INFO", "FT_INFO "+index, start, false, err)
	}
	return info, true
}

func (r *redisCache) FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FTCreate(ctx.Context(), index, options, schema...).Result()
	if hasLogger {
		message := fmt.Sprintf("FT_CREATE %s", index)
		r.fillLogFields(ctx, "FT_CREATE", message, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushAll(ctx Context) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushAll(ctx.Context()).Result()
	if hasLogger {
		r.fillLogFields(ctx, "FLUSHALL", "FLUSHALL", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushDB(ctx Context) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	_, err := r.client.FlushDB(ctx.Context()).Result()
	if hasLogger {
		r.fillLogFields(ctx, "FLUSHDB", "FLUSHDB", start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Process(ctx Context, cmd redis.Cmder) error {
	return r.client.Process(ctx.Context(), cmd)
}

func (r *redisCache) GetCode() string {
	return r.config.GetCode()
}

func (r *redisCache) fillLogFields(ctx Context, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := ctx.getRedisLoggers()
	fillLogFields(ctx, loggers, r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
