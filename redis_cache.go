package fluxaorm

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
	ZRangeArgsWithScores(ctx Context, z redis.ZRangeArgs) []redis.Z
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
	xAdd(ctx Context, stream string, values interface{}) (id string)
	FlushAll(ctx Context)
	FlushDB(ctx Context)
	Scan(ctx Context, cursor uint64, match string, count int64) (keys []string, cursorNext uint64)
	GetLocker() *Locker
	Process(ctx Context, cmd redis.Cmder) error
	GetCode() string
	FTList(ctx Context) []string
	FTDrop(ctx Context, index string, dropDocuments bool)
	FTSearch(ctx Context, index string, query string, options *redis.FTSearchOptions) redis.FTSearchResult
	FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema)
	FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool)
}

type redisCache struct {
	client    *redis.Client
	dragonfly bool
	locker    *Locker
	config    RedisPoolConfig
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
	res := r.client.Info(ctx.Context(), section...)
	val, err := res.Result()
	checkError(err)
	if hasLogger {
		r.fillLogFields(ctx, res, start, false, nil)
	}
	return val
}

func (r *redisCache) GetConfig() RedisPoolConfig {
	return r.config
}

func (r *redisCache) Get(ctx Context, key string) (value string, has bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	res := r.client.Get(ctx.Context(), key)
	val, err := res.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, res, start, true, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(ctx, res, start, false, err)
	}
	return val, true
}

func (r *redisCache) Eval(ctx Context, script string, keys []string, args ...any) any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Eval(ctx.Context(), script, keys, args...)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) EvalSha(ctx Context, sha1 string, keys []string, args ...any) (res any, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.EvalSha(ctx.Context(), sha1, keys, args...)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
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
	req := r.client.ScriptExists(ctx.Context(), sha1)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res[0]
}

func (r *redisCache) ScriptLoad(ctx Context, script string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.ScriptLoad(ctx.Context(), script)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) Set(ctx Context, key string, value any, expiration time.Duration) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Set(ctx.Context(), key, value, expiration)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) SetNX(ctx Context, key string, value any, expiration time.Duration) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SetNX(ctx.Context(), key, value, expiration)
	isSet, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return isSet
}

func (r *redisCache) LPush(ctx Context, key string, values ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LPush(ctx.Context(), key, values...)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LPop(ctx Context, key string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LPop(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) RPush(ctx Context, key string, values ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.RPush(ctx.Context(), key, values...)
	val, err := req.Result()
	if hasLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LLen(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LLen(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Exists(ctx Context, keys ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Exists(ctx.Context(), keys...)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Type(ctx Context, key string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Type(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LRange(ctx Context, key string, start, stop int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.LRange(ctx.Context(), key, start, stop)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) LIndex(ctx Context, key string, index int64) (string, bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.LIndex(ctx.Context(), key, index)
	val, err := req.Result()
	found := true
	if errors.Is(err, redis.Nil) {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) LSet(ctx Context, key string, index int64, value any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LSet(ctx.Context(), key, index, value)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) BLMove(ctx Context, source, destination, srcPos, destPos string, timeout time.Duration) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.BLMove(ctx.Context(), source, destination, srcPos, destPos, timeout)
	value, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) LMove(ctx Context, source, destination, srcPos, destPos string) string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LMove(ctx.Context(), source, destination, srcPos, destPos)
	value, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return value
}

func (r *redisCache) RPop(ctx Context, key string) (value string, found bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.RPop(ctx.Context(), key)
	val, err := req.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, req, start, false, err)
		}
		checkError(err)
		return "", false
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	return val, true
}

func (r *redisCache) LRem(ctx Context, key string, count int64, value any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.LRem(ctx.Context(), key, count, value)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Ltrim(ctx Context, key string, start, stop int64) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.LTrim(ctx.Context(), key, start, stop)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSet(ctx Context, key string, values ...any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HSet(ctx.Context(), key, values...)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HSetNx(ctx Context, key, field string, value any) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HSetNX(ctx.Context(), key, field, value)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) HDel(ctx Context, key string, fields ...string) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HDel(ctx.Context(), key, fields...)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) HMGet(ctx Context, key string, fields ...string) map[string]any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HMGet(ctx.Context(), key, fields...)
	val, err := req.Result()
	results := make(map[string]any, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) HGetAll(ctx Context, key string) map[string]string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HGetAll(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HGet(ctx Context, key, field string) (value string, has bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	misses := false
	start := getNow(hasLogger)
	req := r.client.HGet(ctx.Context(), key, field)
	val, err := req.Result()
	if errors.Is(err, redis.Nil) {
		err = nil
		misses = true
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, misses, err)
	}
	checkError(err)
	return val, !misses
}

func (r *redisCache) HLen(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HLen(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) HIncrBy(ctx Context, key, field string, incr int64) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.HIncrBy(ctx.Context(), key, field, incr)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) IncrBy(ctx Context, key string, incr int64) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.IncrBy(ctx.Context(), key, incr)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Incr(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Incr(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
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
		r.fillLogFields(ctx, res, start, false, err)
	}
	checkError(err)
	value, err := res.Result()
	checkError(err)
	return value
}

func (r *redisCache) Expire(ctx Context, key string, expiration time.Duration) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Expire(ctx.Context(), key, expiration)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZAdd(ctx Context, key string, members ...redis.Z) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.ZAdd(ctx.Context(), key, members...)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRange(ctx Context, key string, start, stop int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	req := r.client.ZRevRange(ctx.Context(), key, start, stop)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRevRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	req := r.client.ZRevRangeWithScores(ctx.Context(), key, start, stop)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRangeWithScores(ctx Context, key string, start, stop int64) []redis.Z {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	req := r.client.ZRangeWithScores(ctx.Context(), key, start, stop)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZRangeArgsWithScores(ctx Context, z redis.ZRangeArgs) []redis.Z {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := getNow(hasLogger)
	req := r.client.ZRangeArgsWithScores(ctx.Context(), z)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, startTime, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCard(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.ZCard(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZCount(ctx Context, key string, min, max string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.ZCount(ctx.Context(), key, min, max)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) ZScore(ctx Context, key, member string) float64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.ZScore(ctx.Context(), key, member)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) MSet(ctx Context, pairs ...any) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.MSet(ctx.Context(), pairs...)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) MGet(ctx Context, keys ...string) []any {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.MGet(ctx.Context(), keys...)
	val, err := req.Result()
	results := make([]any, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, misses > 0, err)
	}
	checkError(err)
	return results
}

func (r *redisCache) SAdd(ctx Context, key string, members ...any) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SAdd(ctx.Context(), key, members...)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SMembers(ctx Context, key string) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SMembers(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SIsMember(ctx Context, key string, member any) bool {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SIsMember(ctx.Context(), key, member)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SCard(ctx Context, key string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SCard(ctx.Context(), key)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) SPop(ctx Context, key string) (string, bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SPop(ctx.Context(), key)
	val, err := req.Result()
	found := true
	if errors.Is(err, redis.Nil) {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val, found
}

func (r *redisCache) SPopN(ctx Context, key string, max int64) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.SPopN(ctx.Context(), key, max)
	val, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return val
}

func (r *redisCache) Del(ctx Context, keys ...string) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Del(ctx.Context(), keys...)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) XTrim(ctx Context, stream string, maxLen int64) (deleted int64) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	var err error
	req := r.client.XTrimMaxLen(ctx.Context(), stream, maxLen)
	deleted, err = req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.XRangeN(ctx.Context(), stream, start, stop, count)
	deleted, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XRevRange(ctx Context, stream, start, stop string, count int64) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.XRevRangeN(ctx.Context(), stream, start, stop, count)
	deleted, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XInfoStream(ctx Context, stream string) *redis.XInfoStream {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XInfoStream(ctx.Context(), stream)
	info, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XInfoGroups(ctx Context, stream string) []redis.XInfoGroup {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XInfoGroups(ctx.Context(), stream)
	info, err := req.Result()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil && err.Error() == "ERR no such key" {
		if hasLogger {
			r.fillLogFields(ctx, req, start, false, err)
		}
		return make([]redis.XInfoGroup, 0)
	}
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XGroupCreate(ctx Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.XGroupCreate(ctx.Context(), stream, group, start)
	res, err := req.Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if hasLogger {
			r.fillLogFields(ctx, req, s, false, err)
		}
		return "OK", true
	}
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return res, false
}

func (r *redisCache) XGroupCreateMkStream(ctx Context, stream, group, start string) (key string, exists bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := getNow(hasLogger)
	req := r.client.XGroupCreateMkStream(ctx.Context(), stream, group, start)
	res, err := req.Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if hasLogger {
		r.fillLogFields(ctx, req, s, false, err)
	}
	checkError(err)
	return res, created
}

func (r *redisCache) XGroupDestroy(ctx Context, stream, group string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XGroupDestroy(ctx.Context(), stream, group)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XRead(ctx Context, a *redis.XReadArgs) []redis.XStream {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XRead(ctx.Context(), a)
	info, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return info
}

func (r *redisCache) XDel(ctx Context, stream string, ids ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XDel(ctx.Context(), stream, ids...)
	deleted, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XGroupDelConsumer(ctx Context, stream, group, consumer string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XGroupDelConsumer(ctx.Context(), stream, group, consumer)
	deleted, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return deleted
}

func (r *redisCache) XReadGroup(ctx Context, a *redis.XReadGroupArgs) (streams []redis.XStream) {
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
		req := r.client.XReadGroup(ctx.Context(), a)
		streams, err = req.Result()
	}

	if errors.Is(err, redis.Nil) {
		err = nil
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
	req := r.client.XPending(ctx.Context(), stream, group)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XPendingExt(ctx Context, a *redis.XPendingExtArgs) []redis.XPendingExt {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XPendingExt(ctx.Context(), a)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XLen(ctx Context, stream string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XLen(ctx.Context(), stream)
	l, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return l
}

func (r *redisCache) XClaim(ctx Context, a *redis.XClaimArgs) []redis.XMessage {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XClaim(ctx.Context(), a)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XClaimJustID(ctx Context, a *redis.XClaimArgs) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XClaimJustID(ctx.Context(), a)
	res, err := r.client.XClaimJustID(ctx.Context(), a).Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) XAck(ctx Context, stream, group string, ids ...string) int64 {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XAck(ctx.Context(), stream, group, ids...)
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) xAdd(ctx Context, stream string, values interface{}) (id string) {
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.XAdd(context.Background(), a)
	id, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return id
}

func (r *redisCache) FTList(ctx Context) []string {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.FT_List(ctx.Context())
	res, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return res
}

func (r *redisCache) FTDrop(ctx Context, index string, dropDocuments bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	if dropDocuments {
		req := r.client.FTDropIndexWithArgs(ctx.Context(), index, &redis.FTDropIndexOptions{DeleteDocs: true})
		_, err := req.Result()
		if hasLogger {
			r.fillLogFields(ctx, req, start, false, err)
		}
		checkError(err)
		return
	}
	req := r.client.FTDropIndex(ctx.Context(), index)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FTSearch(ctx Context, index string, query string, options *redis.FTSearchOptions) redis.FTSearchResult {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	if options == nil {
		options = &redis.FTSearchOptions{}
	}
	if !r.dragonfly {
		options.DialectVersion = 1
	}
	req := r.client.FTSearchWithArgs(ctx.Context(), index, query, options)
	rawRes, err := req.RawResult()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	res := redis.FTSearchResult{}
	if r.dragonfly && options.NoContent {
		asSlice := rawRes.([]any)
		res.Total = int(asSlice[0].(int64))
		res.Docs = make([]redis.Document, len(asSlice)-1)
		if len(asSlice)-1 > 0 {
			for i, id := range asSlice[1:] {
				res.Docs[i].ID = id.(string)
			}
		}
		return res
	}

	asMap := rawRes.(map[any]any)
	res.Total = int(asMap["total_results"].(int64))
	results := asMap["results"].([]any)
	res.Docs = make([]redis.Document, len(results))
	for i, result := range results {
		doc := result.(map[any]any)
		res.Docs[i].ID = doc["id"].(string)
		fields, hasFields := doc["extra_attributes"]
		if hasFields {
			fieldsAsMap := fields.(map[any]any)
			res.Docs[i].Fields = make(map[string]string, len(fieldsAsMap))
			for k, v := range fieldsAsMap {
				res.Docs[i].Fields[k.(string)] = v.(string)
			}
		}
		score, hasScore := doc["score"]
		if hasScore {
			scoreF := score.(float64)
			res.Docs[i].Score = &scoreF
		}
		payload, hasPayload := doc["payload"]
		if hasPayload && payload != nil {
			payloadS := payload.(string)
			res.Docs[i].Payload = &payloadS
		}
		sortkey, hasSortkey := doc["payload"]
		if hasSortkey && sortkey != nil {
			sortkeyS := sortkey.(string)
			res.Docs[i].SortKey = &sortkeyS
		}
	}
	return res
}

func (r *redisCache) FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.FTInfo(ctx.Context(), index)
	res, err := req.RawResult()
	if res == nil {
		err = nil
		if hasLogger {
			r.fillLogFields(ctx, req, start, true, err)
		}
		return nil, false
	}
	checkError(err)
	info = &redis.FTInfoResult{}
	asMap := res.(map[any]any)
	if r.dragonfly {
		info.IndexName = asMap["index_name"].(string)
		info.NumDocs = int(asMap["num_docs"].(int64))
		attributes := asMap["attributes"].([]any)
		info.Attributes = make([]redis.FTAttribute, len(attributes))
		for i, v := range attributes {
			asSlice := v.([]any)
			row := map[string]any{}
			sortable := false
			for k := 0; k < len(asSlice); k += 2 {
				key := asSlice[k].(string)
				if key == "SORTABLE" {
					sortable = true
					k++
					key = asSlice[k].(string)
					continue
				}
				row[key] = asSlice[k+1]
			}
			weight := float64(0)
			rowWeight, has := row["WEIGHT"]
			if has {
				weight = rowWeight.(float64)
			}
			info.Attributes[i] = redis.FTAttribute{
				Identifier:      row["identifier"].(string),
				Attribute:       row["attribute"].(string),
				Type:            row["type"].(string),
				Weight:          weight,
				Sortable:        sortable,
				NoStem:          false,
				NoIndex:         false,
				UNF:             false,
				PhoneticMatcher: "",
				CaseSensitive:   false,
				WithSuffixtrie:  false,
			}
		}
		return info, true
	}
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
		r.fillLogFields(ctx, req, start, false, err)
	}
	return info, true
}

func (r *redisCache) FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.FTCreate(ctx.Context(), index, options, schema...)
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushAll(ctx Context) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.FlushAll(ctx.Context())
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) FlushDB(ctx Context) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.FlushDB(ctx.Context())
	_, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
}

func (r *redisCache) Scan(ctx Context, cursor uint64, match string, count int64) (keys []string, cursorNext uint64) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	req := r.client.Scan(ctx.Context(), cursor, match, count)
	keys, cursorNext, err := req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, start, false, err)
	}
	checkError(err)
	return keys, cursorNext
}

func (r *redisCache) Process(ctx Context, cmd redis.Cmder) error {
	return r.client.Process(ctx.Context(), cmd)
}

func (r *redisCache) GetCode() string {
	return r.config.GetCode()
}

func (r *redisCache) fillLogFields(ctx Context, req redis.Cmder, start *time.Time, cacheMiss bool, err error) {
	_, loggers := ctx.getRedisLoggers()
	fillLogFields(ctx, loggers, r.config.GetCode(), sourceRedis, req.Name(), formatRedisCommandLog(req), start, cacheMiss, err)
}

func formatRedisCommandLog(req redis.Cmder) string {
	return req.String()[:strings.LastIndex(req.String(), ":")]
}
