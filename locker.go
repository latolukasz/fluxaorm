package orm

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

func (l *Locker) Obtain(ctx Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	if waitTimeout > ttl {
		panic(errors.New("waitTimeout can't be higher than ttl"))
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
		if err == redislock.ErrNotObtained {
			if hasLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields(ctx, "LOCK OBTAIN", message, start, true, nil)
			}
			return nil, false
		}
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
		l.fillLogFields(ctx, "LOCK OBTAIN", message, start, false, nil)
	}
	checkError(err)
	lock = &Lock{lock: redisLock, locker: l, ttl: ttl, key: key, has: true}
	return lock, true
}

func (l *Locker) MustObtain(ctx Context, key string, ttl time.Duration, waitTimeout time.Duration) *Lock {
	lock, obtained := l.Obtain(ctx, key, ttl, waitTimeout)
	if !obtained {
		panic(errors.New("can't obtain lock"))
	}
	return lock
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
	if err == redislock.ErrLockNotHeld {
		err = nil
		ok = false
	}
	if hasLogger {
		l.locker.fillLogFields(ctx, "LOCK RELEASE", "LOCK RELEASE "+l.key, start, !ok, err)
	}
	checkError(err)
}

func (l *Lock) TTL(ctx Context) time.Duration {
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	t, err := l.lock.TTL(ctx.Context())
	if hasLogger {
		l.locker.fillLogFields(ctx, "LOCK TTL", "LOCK TTL "+l.key, start, false, err)
	}
	checkError(err)
	return t
}

func (l *Lock) Refresh(ctx Context, ttl time.Duration) bool {
	if !l.has {
		return false
	}
	hasLogger, _ := ctx.getRedisLoggers()
	start := getNow(hasLogger)
	err := l.lock.Refresh(ctx.Context(), ttl, nil)
	ok := true
	if err == redislock.ErrNotObtained {
		ok = false
		err = nil
		l.has = false
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, l.ttl)
		l.locker.fillLogFields(ctx, "LOCK REFRESH", message, start, !ok, err)
	}
	checkError(err)
	return ok
}

func (l *Locker) fillLogFields(ctx Context, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := ctx.getRedisLoggers()
	fillLogFields(ctx, loggers, l.r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
