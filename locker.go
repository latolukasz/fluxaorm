package fluxaorm

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"

	"github.com/pkg/errors"
)

type lockerClient interface {
	Obtain(context context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error)
}

type standardLockerClient struct {
	client *redislock.Client
}

func (l *standardLockerClient) Obtain(context context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error) {
	return l.client.Obtain(context, key, ttl, opt)
}

type Locker struct {
	locker lockerClient
	r      *redisCache
}

func (r *redisCache) GetLocker() *Locker {
	if r.locker == nil {
		r.locker = &Locker{locker: &standardLockerClient{client: redislock.New(r.client)}, r: r}
	}
	return r.locker
}

func (l *Locker) Obtain(ctx Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool, err error) {
	if ttl == 0 {
		return nil, false, errors.New("ttl must be higher than zero")
	}
	if waitTimeout > ttl {
		return nil, false, errors.New("waitTimeout can't be higher than ttl")
	}
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	var options *redislock.Options
	if waitTimeout > 0 {
		options = &redislock.Options{}
		interval := time.Second
		limit := 1
		if waitTimeout < interval {
			interval = waitTimeout
		} else {
			limit = int(waitTimeout / time.Second)
		}
		options.RetryStrategy = redislock.LimitRetry(redislock.LinearBackoff(interval), limit)
	}
	redisLock, err := l.locker.Obtain(ctx.Context(), key, ttl, options)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			if hasLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields(ctx, "LOCK OBTAIN", message, start, true, nil)
			}
			return nil, false, nil
		}
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
		l.fillLogFields(ctx, "LOCK OBTAIN", message, start, false, nil)
	}
	if err != nil {
		return nil, false, err
	}
	lock = &Lock{lock: redisLock, locker: l, ttl: ttl, key: key, has: true}
	return lock, true, nil
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	ttl    time.Duration
	locker *Locker
	has    bool
}

func (l *Lock) Release(ctx Context) {
	if !l.has {
		return
	}
	l.has = false
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	err := l.lock.Release(context.Background())
	ok := true
	if errors.Is(err, redislock.ErrLockNotHeld) {
		err = nil
		ok = false
	}
	if hasLogger {
		l.locker.fillLogFields(ctx, "LOCK RELEASE", "LOCK RELEASE "+l.key, start, !ok, err)
	}
}

func (l *Lock) TTL(ctx Context) (time.Duration, error) {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	t, err := l.lock.TTL(ctx.Context())
	if hasLogger {
		l.locker.fillLogFields(ctx, "LOCK TTL", "LOCK TTL "+l.key, start, false, err)
	}
	return t, err
}

func (l *Lock) Refresh(ctx Context, ttl time.Duration) (bool, error) {
	if !l.has {
		return false, nil
	}
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	err := l.lock.Refresh(ctx.Context(), ttl, nil)
	ok := true
	if errors.Is(err, redislock.ErrNotObtained) {
		ok = false
		err = nil
		l.has = false
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, l.ttl)
		l.locker.fillLogFields(ctx, "LOCK REFRESH", message, start, !ok, err)
	}
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (l *Locker) fillLogFields(ctx Context, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := ctx.getRedisLoggers()
	fillLogFields(ctx, loggers, l.r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
