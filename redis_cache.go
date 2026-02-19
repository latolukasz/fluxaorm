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

const metricsOperationOther = "other"
const metricsOperationKey = "key"
const metricsOperationList = "list"
const metricsOperationHash = "hash"
const metricsOperationSet = "set"
const metricsOperationStream = "stream"
const metricsOperationSearch = "search"
const metricsOperationLock = "lock"

type RedisCache interface {
	Set(ctx Context, key string, value any, expiration time.Duration) error
	MSet(ctx Context, pairs ...any) error
	Del(ctx Context, keys ...string) error
	HSet(ctx Context, key string, values ...any) error
	HDel(ctx Context, key string, keys ...string) error
	GetSet(ctx Context, key string, expiration time.Duration, provider func() any) (any, error)
	Info(ctx Context, section ...string) (string, error)
	GetConfig() RedisPoolConfig
	Get(ctx Context, key string) (value string, has bool, err error)
	Eval(ctx Context, script string, keys []string, args ...any) (any, error)
	EvalSha(ctx Context, sha1 string, keys []string, args ...any) (res any, exists bool, err error)
	SetNX(ctx Context, key string, value any, expiration time.Duration) (bool, error)
	ScriptExists(ctx Context, sha1 string) (bool, error)
	ScriptLoad(ctx Context, script string) (string, error)
	LPush(ctx Context, key string, values ...any) (int64, error)
	LPop(ctx Context, key string) (string, error)
	RPush(ctx Context, key string, values ...any) (int64, error)
	LLen(ctx Context, key string) (int64, error)
	Exists(ctx Context, keys ...string) (int64, error)
	Type(ctx Context, key string) (string, error)
	LRange(ctx Context, key string, start, stop int64) ([]string, error)
	LIndex(ctx Context, key string, index int64) (string, bool, error)
	LSet(ctx Context, key string, index int64, value any) error
	RPop(ctx Context, key string) (value string, found bool, err error)
	BLMove(ctx Context, source, destination, srcPos, destPos string, timeout time.Duration) (string, error)
	LMove(ctx Context, source, destination, srcPos, destPos string) (string, error)
	LRem(ctx Context, key string, count int64, value any) error
	Ltrim(ctx Context, key string, start, stop int64) error
	HSetNx(ctx Context, key, field string, value any) (bool, error)
	HMGet(ctx Context, key string, fields ...string) (map[string]any, error)
	HGetAll(ctx Context, key string) (map[string]string, error)
	HGet(ctx Context, key, field string) (value string, has bool, err error)
	HLen(ctx Context, key string) (int64, error)
	HIncrBy(ctx Context, key, field string, incr int64) (int64, error)
	IncrBy(ctx Context, key string, incr int64) (int64, error)
	Incr(ctx Context, key string) (int64, error)
	IncrWithExpire(ctx Context, key string, expire time.Duration) (int64, error)
	Expire(ctx Context, key string, expiration time.Duration) (bool, error)
	ZAdd(ctx Context, key string, members ...redis.Z) (int64, error)
	ZRevRange(ctx Context, key string, start, stop int64) ([]string, error)
	ZRevRangeWithScores(ctx Context, key string, start, stop int64) ([]redis.Z, error)
	ZRangeWithScores(ctx Context, key string, start, stop int64) ([]redis.Z, error)
	ZRangeArgsWithScores(ctx Context, z redis.ZRangeArgs) ([]redis.Z, error)
	ZCard(ctx Context, key string) (int64, error)
	ZCount(ctx Context, key string, min, max string) (int64, error)
	ZScore(ctx Context, key, member string) (float64, error)
	MGet(ctx Context, keys ...string) ([]any, error)
	SAdd(ctx Context, key string, members ...any) (int64, error)
	SMembers(ctx Context, key string) ([]string, error)
	SIsMember(ctx Context, key string, member any) (bool, error)
	SCard(ctx Context, key string) (int64, error)
	SPop(ctx Context, key string) (string, bool, error)
	SPopN(ctx Context, key string, max int64) ([]string, error)
	XTrim(ctx Context, stream string, maxLen int64) (deleted int64, err error)
	XRange(ctx Context, stream, start, stop string, count int64) ([]redis.XMessage, error)
	XRevRange(ctx Context, stream, start, stop string, count int64) ([]redis.XMessage, error)
	XInfoStream(ctx Context, stream string) (*redis.XInfoStream, error)
	XInfoGroups(ctx Context, stream string) ([]redis.XInfoGroup, error)
	XGroupCreate(ctx Context, stream, group, start string) (key string, exists bool, err error)
	XGroupCreateMkStream(ctx Context, stream, group, start string) (key string, exists bool, err error)
	XGroupDestroy(ctx Context, stream, group string) (int64, error)
	XRead(ctx Context, a *redis.XReadArgs) ([]redis.XStream, error)
	XDel(ctx Context, stream string, ids ...string) (int64, error)
	XAutoClaim(ctx Context, a *redis.XAutoClaimArgs) (messages []redis.XMessage, start string, err error)
	XGroupDelConsumer(ctx Context, stream, group, consumer string) (int64, error)
	XReadGroup(ctx Context, a *redis.XReadGroupArgs) (streams []redis.XStream, err error)
	XPending(ctx Context, stream, group string) (*redis.XPending, error)
	XPendingExt(ctx Context, a *redis.XPendingExtArgs) ([]redis.XPendingExt, error)
	XLen(ctx Context, stream string) (int64, error)
	XClaim(ctx Context, a *redis.XClaimArgs) ([]redis.XMessage, error)
	XClaimJustID(ctx Context, a *redis.XClaimArgs) ([]string, error)
	XAck(ctx Context, stream, group string, ids ...string) (int64, error)
	xAdd(ctx Context, stream string, values interface{}) (id string, err error)
	FlushAll(ctx Context) error
	FlushDB(ctx Context) error
	Scan(ctx Context, cursor uint64, match string, count int64) (keys []string, cursorNext uint64, err error)
	GetLocker() *Locker
	GetCode() string
	FTList(ctx Context) ([]string, error)
	FTDrop(ctx Context, index string, dropDocuments bool) error
	FTSearch(ctx Context, index string, query string, options *redis.FTSearchOptions) (redis.FTSearchResult, error)
	FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema) error
	FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool, err error)
	Client() *redis.Client
}

type redisCache struct {
	client *redis.Client
	locker *Locker
	config RedisPoolConfig
}

func (r *redisCache) GetSet(ctx Context, key string, expiration time.Duration, provider func() any) (any, error) {
	val, has, err := r.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !has {
		userVal := provider()
		encoded, _ := msgpack.Marshal(userVal)
		err = r.Set(ctx, key, string(encoded), expiration)
		if err != nil {
			return nil, err
		}
		return userVal, nil
	}
	var data any
	err = msgpack.Unmarshal([]byte(val), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *redisCache) Info(ctx Context, section ...string) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	res := r.client.Info(ctx.Context(), section...)
	val, err := res.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, res, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
	if err != nil {
		return "", err
	}
	return val, nil
}

func (r *redisCache) GetConfig() RedisPoolConfig {
	return r.config
}

func (r *redisCache) Get(ctx Context, key string) (value string, has bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	res := r.client.Get(ctx.Context(), key)
	val, err := res.Result()
	end := time.Since(start)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, res, end, true, err)
		}
		r.fillMetrics(ctx, end, metricsOperationKey, false, true, err)
		return "", false, err
	}
	if hasLogger {
		r.fillLogFields(ctx, res, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, false, false, err)
	return val, true, nil
}

func (r *redisCache) Eval(ctx Context, script string, keys []string, args ...any) (any, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Eval(ctx.Context(), script, keys, args...)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (r *redisCache) EvalSha(ctx Context, sha1 string, keys []string, args ...any) (res any, exists bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.EvalSha(ctx.Context(), sha1, keys, args...)
	res, err = req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	if err != nil {
		exists, err = r.ScriptExists(ctx, sha1)
		if err != nil {
			r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
			return nil, false, err
		}
		if !exists {
			r.fillMetrics(ctx, end, metricsOperationOther, false, false, nil)
			return nil, false, nil
		}
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, nil)
	return res, true, nil
}

func (r *redisCache) ScriptExists(ctx Context, sha1 string) (bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ScriptExists(ctx.Context(), sha1)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
	if err != nil {
		return false, err
	}
	return res[0], nil
}

func (r *redisCache) ScriptLoad(ctx Context, script string) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ScriptLoad(ctx.Context(), script)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (r *redisCache) Set(ctx Context, key string, value any, expiration time.Duration) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Set(ctx.Context(), key, value, expiration)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return err
}

func (r *redisCache) SetNX(ctx Context, key string, value any, expiration time.Duration) (bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SetNX(ctx.Context(), key, value, expiration)
	isSet, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return isSet, err
}

func (r *redisCache) LPush(ctx Context, key string, values ...any) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LPush(ctx.Context(), key, values...)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, true, false, err)
	return val, err
}

func (r *redisCache) LPop(ctx Context, key string) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LPop(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return val, err
}

func (r *redisCache) RPush(ctx Context, key string, values ...any) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.RPush(ctx.Context(), key, values...)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		message := "RPUSH " + key
		for _, v := range values {
			message += " " + fmt.Sprintf("%v", v)
		}
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, true, false, err)
	return val, err
}

func (r *redisCache) LLen(ctx Context, key string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LLen(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return val, err
}

func (r *redisCache) Exists(ctx Context, keys ...string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Exists(ctx.Context(), keys...)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, false, false, err)
	return val, err
}

func (r *redisCache) Type(ctx Context, key string) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Type(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, false, false, err)
	return val, err
}

func (r *redisCache) LRange(ctx Context, key string, start, stop int64) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.LRange(ctx.Context(), key, start, stop)
	val, err := req.Result()
	end := time.Since(s)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return val, err
}

func (r *redisCache) LIndex(ctx Context, key string, index int64) (string, bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.LIndex(ctx.Context(), key, index)
	val, err := req.Result()
	end := time.Since(s)
	found := true
	if errors.Is(err, redis.Nil) {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	if err != nil {
		r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
		return "", false, err
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, nil)
	return val, found, nil
}

func (r *redisCache) LSet(ctx Context, key string, index int64, value any) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LSet(ctx.Context(), key, index, value)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, true, false, err)
	return err
}

func (r *redisCache) BLMove(ctx Context, source, destination, srcPos, destPos string, timeout time.Duration) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.BLMove(ctx.Context(), source, destination, srcPos, destPos, timeout)
	value, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, true, false, err)
	return value, err
}

func (r *redisCache) LMove(ctx Context, source, destination, srcPos, destPos string) (string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LMove(ctx.Context(), source, destination, srcPos, destPos)
	value, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, true, false, err)
	return value, err
}

func (r *redisCache) RPop(ctx Context, key string) (value string, found bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.RPop(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}
		if hasLogger {
			r.fillLogFields(ctx, req, end, false, err)
		}
		r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
		return "", false, err
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return val, true, nil
}

func (r *redisCache) LRem(ctx Context, key string, count int64, value any) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.LRem(ctx.Context(), key, count, value)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return err
}

func (r *redisCache) Ltrim(ctx Context, key string, start, stop int64) error {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.LTrim(ctx.Context(), key, start, stop)
	_, err := req.Result()
	end := time.Since(s)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationList, false, false, err)
	return err
}

func (r *redisCache) HSet(ctx Context, key string, values ...any) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HSet(ctx.Context(), key, values...)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, true, false, err)
	return err
}

func (r *redisCache) HSetNx(ctx Context, key, field string, value any) (bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HSetNX(ctx.Context(), key, field, value)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, true, false, err)
	return res, err
}

func (r *redisCache) HDel(ctx Context, key string, fields ...string) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HDel(ctx.Context(), key, fields...)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, true, false, err)
	return err
}

func (r *redisCache) HMGet(ctx Context, key string, fields ...string) (map[string]any, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HMGet(ctx.Context(), key, fields...)
	val, err := req.Result()
	end := time.Since(start)
	results := make(map[string]any, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, misses > 0, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, false, misses > 0, err)
	return results, err
}

func (r *redisCache) HGetAll(ctx Context, key string) (map[string]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HGetAll(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, false, false, err)
	return val, err
}

func (r *redisCache) HGet(ctx Context, key, field string) (value string, has bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	misses := false
	start := time.Now()
	req := r.client.HGet(ctx.Context(), key, field)
	val, err := req.Result()
	end := time.Since(start)
	if errors.Is(err, redis.Nil) {
		err = nil
		misses = true
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, misses, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, false, misses, err)
	return val, !misses, err
}

func (r *redisCache) HLen(ctx Context, key string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HLen(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, false, false, err)
	return val, err
}

func (r *redisCache) HIncrBy(ctx Context, key, field string, incr int64) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.HIncrBy(ctx.Context(), key, field, incr)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationHash, true, false, err)
	return val, err
}

func (r *redisCache) IncrBy(ctx Context, key string, incr int64) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.IncrBy(ctx.Context(), key, incr)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return val, err
}

func (r *redisCache) Incr(ctx Context, key string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Incr(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return val, err
}

func (r *redisCache) IncrWithExpire(ctx Context, key string, expire time.Duration) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	p := r.client.Pipeline()
	res := p.Incr(ctx.Context(), key)
	p.Expire(ctx.Context(), key, expire)
	_, err := p.Exec(ctx.Context())
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, res, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	if err != nil {
		return 0, err
	}
	value, err := res.Result()
	return value, err
}

func (r *redisCache) Expire(ctx Context, key string, expiration time.Duration) (bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Expire(ctx.Context(), key, expiration)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return val, err
}

func (r *redisCache) ZAdd(ctx Context, key string, members ...redis.Z) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ZAdd(ctx.Context(), key, members...)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, true, false, err)
	return val, err
}

func (r *redisCache) ZRevRange(ctx Context, key string, start, stop int64) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := time.Now()
	req := r.client.ZRevRange(ctx.Context(), key, start, stop)
	val, err := req.Result()
	end := time.Since(startTime)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZRevRangeWithScores(ctx Context, key string, start, stop int64) ([]redis.Z, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := time.Now()
	req := r.client.ZRevRangeWithScores(ctx.Context(), key, start, stop)
	val, err := req.Result()
	end := time.Since(startTime)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZRangeWithScores(ctx Context, key string, start, stop int64) ([]redis.Z, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := time.Now()
	req := r.client.ZRangeWithScores(ctx.Context(), key, start, stop)
	val, err := req.Result()
	end := time.Since(startTime)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZRangeArgsWithScores(ctx Context, z redis.ZRangeArgs) ([]redis.Z, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := time.Now()
	req := r.client.ZRangeArgsWithScores(ctx.Context(), z)
	val, err := req.Result()
	end := time.Since(startTime)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZCard(ctx Context, key string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ZCard(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZCount(ctx Context, key string, min, max string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ZCount(ctx.Context(), key, min, max)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) ZScore(ctx Context, key, member string) (float64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.ZScore(ctx.Context(), key, member)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) MSet(ctx Context, pairs ...any) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.MSet(ctx.Context(), pairs...)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return err
}

func (r *redisCache) MGet(ctx Context, keys ...string) ([]any, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.MGet(ctx.Context(), keys...)
	val, err := req.Result()
	end := time.Since(start)
	results := make([]any, len(keys))
	misses := 0
	for i, v := range val {
		results[i] = v
		if v == nil {
			misses++
		}
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, misses > 0, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, false, misses > 0, err)
	return results, err
}

func (r *redisCache) SAdd(ctx Context, key string, members ...any) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SAdd(ctx.Context(), key, members...)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, true, false, err)
	return val, err
}

func (r *redisCache) SMembers(ctx Context, key string) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SMembers(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) SIsMember(ctx Context, key string, member any) (bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SIsMember(ctx.Context(), key, member)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) SCard(ctx Context, key string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SCard(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) SPop(ctx Context, key string) (string, bool, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SPop(ctx.Context(), key)
	val, err := req.Result()
	end := time.Since(start)
	found := true
	if errors.Is(err, redis.Nil) {
		err = nil
		found = false
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, found, err
}

func (r *redisCache) SPopN(ctx Context, key string, max int64) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.SPopN(ctx.Context(), key, max)
	val, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSet, false, false, err)
	return val, err
}

func (r *redisCache) Del(ctx Context, keys ...string) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Del(ctx.Context(), keys...)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationKey, true, false, err)
	return err
}

func (r *redisCache) XTrim(ctx Context, stream string, maxLen int64) (deleted int64, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XTrimMaxLen(ctx.Context(), stream, maxLen)
	deleted, err = req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return deleted, err
}

func (r *redisCache) XRange(ctx Context, stream, start, stop string, count int64) ([]redis.XMessage, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.XRangeN(ctx.Context(), stream, start, stop, count)
	deleted, err := req.Result()
	end := time.Since(s)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return deleted, err
}

func (r *redisCache) XRevRange(ctx Context, stream, start, stop string, count int64) ([]redis.XMessage, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.XRevRangeN(ctx.Context(), stream, start, stop, count)
	deleted, err := req.Result()
	end := time.Since(s)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return deleted, err
}

func (r *redisCache) XInfoStream(ctx Context, stream string) (*redis.XInfoStream, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XInfoStream(ctx.Context(), stream)
	info, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return info, err
}

func (r *redisCache) XInfoGroups(ctx Context, stream string) ([]redis.XInfoGroup, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XInfoGroups(ctx.Context(), stream)
	info, err := req.Result()
	end := time.Since(start)
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil && err.Error() == "ERR no such key" {
		if hasLogger {
			r.fillLogFields(ctx, req, end, false, err)
		}
		r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
		return make([]redis.XInfoGroup, 0), nil
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return info, err
}

func (r *redisCache) XGroupCreate(ctx Context, stream, group, start string) (key string, exists bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.XGroupCreate(ctx.Context(), stream, group, start)
	res, err := req.Result()
	end := time.Since(s)
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		if hasLogger {
			r.fillLogFields(ctx, req, end, false, err)
		}
		return "OK", true, nil
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return res, false, err
}

func (r *redisCache) XGroupCreateMkStream(ctx Context, stream, group, start string) (key string, exists bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	s := time.Now()
	req := r.client.XGroupCreateMkStream(ctx.Context(), stream, group, start)
	res, err := req.Result()
	end := time.Since(s)
	created := false
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return res, created, err
}

func (r *redisCache) XGroupDestroy(ctx Context, stream, group string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XGroupDestroy(ctx.Context(), stream, group)
	res, err := req.Result()
	end := time.Since(start)
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return res, err
}

func (r *redisCache) XRead(ctx Context, a *redis.XReadArgs) ([]redis.XStream, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XRead(ctx.Context(), a)
	info, err := req.Result()
	end := time.Since(start)
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return info, err
}

func (r *redisCache) XDel(ctx Context, stream string, ids ...string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XDel(ctx.Context(), stream, ids...)
	deleted, err := req.Result()
	end := time.Since(start)
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return deleted, err
}

func (r *redisCache) XAutoClaim(ctx Context, a *redis.XAutoClaimArgs) (messages []redis.XMessage, start string, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	startTime := time.Now()
	req := r.client.XAutoClaim(ctx.Context(), a)
	end := time.Since(startTime)
	messages, start, err = req.Result()
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return messages, start, err
}

func (r *redisCache) XGroupDelConsumer(ctx Context, stream, group, consumer string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XGroupDelConsumer(ctx.Context(), stream, group, consumer)
	deleted, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	return deleted, err
}

func (r *redisCache) XReadGroup(ctx Context, a *redis.XReadGroupArgs) (streams []redis.XStream, err error) {
	if a.Block >= 0 {
		streams, err = r.client.XReadGroup(ctx.Context(), a).Result()
		metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
		if hasMetrics {
			metrics.queriesRedisBlock.WithLabelValues("stream", r.config.GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
	} else {
		start := time.Now()
		req := r.client.XReadGroup(ctx.Context(), a)
		end := time.Since(start)
		r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
		streams, err = req.Result()
	}

	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	return streams, err
}

func (r *redisCache) XPending(ctx Context, stream, group string) (*redis.XPending, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XPending(ctx.Context(), stream, group)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return res, err
}

func (r *redisCache) XPendingExt(ctx Context, a *redis.XPendingExtArgs) ([]redis.XPendingExt, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XPendingExt(ctx.Context(), a)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return res, err
}

func (r *redisCache) XLen(ctx Context, stream string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XLen(ctx.Context(), stream)
	l, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return l, err
}

func (r *redisCache) XClaim(ctx Context, a *redis.XClaimArgs) ([]redis.XMessage, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XClaim(ctx.Context(), a)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return res, err
}

func (r *redisCache) XClaimJustID(ctx Context, a *redis.XClaimArgs) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XClaimJustID(ctx.Context(), a)
	res, err := r.client.XClaimJustID(ctx.Context(), a).Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, false, false, err)
	return res, err
}

func (r *redisCache) XAck(ctx Context, stream, group string, ids ...string) (int64, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XAck(ctx.Context(), stream, group, ids...)
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	return res, err
}

func (r *redisCache) xAdd(ctx Context, stream string, values interface{}) (id string, err error) {
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.XAdd(context.Background(), a)
	id, err = req.Result()
	end := time.Since(start)
	r.fillMetrics(ctx, end, metricsOperationStream, true, false, err)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	return id, err
}

func (r *redisCache) FTList(ctx Context) ([]string, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.FT_List(ctx.Context())
	res, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSearch, false, false, err)
	return res, err
}

func (r *redisCache) FTDrop(ctx Context, index string, dropDocuments bool) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	if dropDocuments {
		req := r.client.FTDropIndexWithArgs(ctx.Context(), index, &redis.FTDropIndexOptions{DeleteDocs: true})
		end := time.Since(start)
		_, err := req.Result()
		if hasLogger {
			r.fillLogFields(ctx, req, end, false, err)
		}
		r.fillMetrics(ctx, end, metricsOperationSearch, true, false, err)
		return err
	}
	req := r.client.FTDropIndex(ctx.Context(), index)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSearch, true, false, err)
	return err
}

func (r *redisCache) FTSearch(ctx Context, index string, query string, options *redis.FTSearchOptions) (redis.FTSearchResult, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	if options == nil {
		options = &redis.FTSearchOptions{DialectVersion: 1}
	}
	req := r.client.FTSearchWithArgs(ctx.Context(), index, query, options)
	rawRes, err := req.RawResult()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSearch, false, false, err)
	if err != nil {
		return redis.FTSearchResult{}, err
	}
	res := redis.FTSearchResult{}
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
	return res, err
}

func (r *redisCache) FTInfo(ctx Context, index string) (info *redis.FTInfoResult, found bool, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.FTInfo(ctx.Context(), index)
	res, err := req.RawResult()
	end := time.Since(start)
	if res == nil {
		err = nil
		if hasLogger {
			r.fillLogFields(ctx, req, end, true, err)
		}
		r.fillMetrics(ctx, end, metricsOperationSearch, false, false, err)
		return nil, false, nil
	}
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
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSearch, false, false, err)
	return info, true, nil
}

func (r *redisCache) FTCreate(ctx Context, index string, options *redis.FTCreateOptions, schema ...*redis.FieldSchema) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.FTCreate(ctx.Context(), index, options, schema...)
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationSearch, true, false, err)
	return err
}

func (r *redisCache) FlushAll(ctx Context) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.FlushAll(ctx.Context())
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, true, false, err)
	return err
}

func (r *redisCache) FlushDB(ctx Context) error {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.FlushDB(ctx.Context())
	_, err := req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, true, false, err)
	return err
}

func (r *redisCache) Scan(ctx Context, cursor uint64, match string, count int64) (keys []string, cursorNext uint64, err error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := time.Now()
	req := r.client.Scan(ctx.Context(), cursor, match, count)
	keys, cursorNext, err = req.Result()
	end := time.Since(start)
	if hasLogger {
		r.fillLogFields(ctx, req, end, false, err)
	}
	r.fillMetrics(ctx, end, metricsOperationOther, false, false, err)
	return keys, cursorNext, err
}

func (r *redisCache) GetCode() string {
	return r.config.GetCode()
}

func (r *redisCache) fillLogFields(ctx Context, req redis.Cmder, duration time.Duration, cacheMiss bool, err error) {
	_, loggers := ctx.getRedisLoggers()
	fillLogFields(ctx, loggers, r.config.GetCode(), sourceRedis, req.Name(), formatRedisCommandLog(req), &duration, cacheMiss, err)
}

func (r *redisCache) Client() *redis.Client {
	return r.client
}

func (r *redisCache) fillMetrics(ctx Context, end time.Duration, operation string, set, miss bool, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		missValue := "0"
		if miss {
			missValue = "1"
		}
		setValue := "0"
		if set {
			setValue = "1"
		}
		metrics.queriesRedis.WithLabelValues(operation, r.config.GetCode(), setValue, missValue, "0", ctx.getMetricsSourceTag()).Observe(end.Seconds())
		if err != nil {
			metrics.queriesRedisErrors.WithLabelValues(r.config.GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
	}
}

func formatRedisCommandLog(req redis.Cmder) string {
	return req.String()[:strings.LastIndex(req.String(), ":")]
}
