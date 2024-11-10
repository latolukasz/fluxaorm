package orm

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redislock"

	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
)

const asyncConsumerPage = 1000
const asyncConsumerBlockTime = time.Second * 3
const flushAsyncEventsList = "flush_async_events"
const flushAsyncEventsListErrorSuffix = ":err"

var mySQLErrorCodesToSkip = []uint16{
	1022, // Can't write; duplicate key in table '%s'
	1048, // Column '%s' cannot be null
	1049, // Unknown database '%s'
	1051, // Unknown table '%s'
	1054, // Unknown column '%s' in '%s'
	1062, // Duplicate entry '%s' for key %d
	1063, // Incorrect column specifier for column '%s'
	1064, // Syntax error
	1067, // Invalid default value for '%s'
	1109, // Message: Unknown table '%s' in %s
	1146, // Table '%s.%s' doesn't exist
	1149, // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
	2032, // Data truncated
}

const asyncConsumerLockName = "async_consumer"

func ConsumeAsyncFlushEvents(ctx Context, block bool) error {
	lock, lockObtained := ctx.Engine().Redis(DefaultPoolCode).GetLocker().Obtain(ctx, asyncConsumerLockName, time.Minute, 0)
	if !lockObtained {
		return redislock.ErrNotObtained
	}
	defer func() {
		lock.Release(ctx)
	}()
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				lockObtained = false
				time.Sleep(time.Second * 50)
			}
		}()
		time.Sleep(time.Second * 50)
		if !lock.Refresh(ctx, time.Minute) {
			lockObtained = false
		}
	}()
	errorMutex := sync.Mutex{}
	waitGroup := &sync.WaitGroup{}
	ctxNoCancel := ctx.CloneWithContext(context.Background())
	groups := make(map[DB]map[RedisCache]map[string]bool)
	var stop uint32
	var globalError error
	for _, schema := range ctx.Engine().Registry().Entities() {
		db := schema.GetDB()
		dbGroup, has := groups[db]
		asyncCacheKey := schema.(*entitySchema).asyncCacheKey
		if !has {
			dbGroup = make(map[RedisCache]map[string]bool)
			groups[db] = dbGroup
		}
		r := ctx.Engine().Redis(schema.getForcedRedisCode())
		redisGroup, has := dbGroup[r]
		if !has {
			redisGroup = make(map[string]bool)
			dbGroup[r] = redisGroup
		}
		_, has = redisGroup[asyncCacheKey]
		if has {
			continue
		}
		redisGroup[asyncCacheKey] = true
		waitGroup.Add(1)
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					atomic.AddUint32(&stop, 1)
					asError, isError := rec.(error)
					if !isError {
						asError = fmt.Errorf("%v", rec)
					}
					if globalError == nil {
						errorMutex.Lock()
						globalError = asError
						errorMutex.Unlock()
					}
				}
			}()
			consumeAsyncEvents(ctx.Context(), ctxNoCancel.Clone(), asyncCacheKey, db, r, block, waitGroup, &lockObtained, &stop)
		}()
	}
	waitGroup.Wait()
	return globalError
}

func consumeAsyncEvents(context context.Context, ctx Context, list string, db DB, r RedisCache,
	block bool, waitGroup *sync.WaitGroup, lockObtained *bool, stop *uint32) {
	defer waitGroup.Done()
	var values []string
	for {
		if context.Err() != nil || !*lockObtained || *stop > 0 {
			return
		}

		values = r.LRange(ctx, list, 0, asyncConsumerPage-1)
		if len(values) > 0 {
			handleAsyncEvents(context, ctx, list, db, r, values)
		}
		if len(values) < asyncConsumerPage {
			if !block || context.Err() != nil {
				return
			}
			time.Sleep(ctx.Engine().Registry().(*engineRegistryImplementation).asyncConsumerBlockTime)
		}
	}
}

func handleAsyncEvents(context context.Context, ctx Context, list string, db DB, r RedisCache, values []string) {
	operations := len(values)
	inTX := operations > 1
	func() {
		var d DBBase
		defer func() {
			if inTX && d != nil {
				d.(DBTransaction).Rollback(ctx)
			}
		}()
		dbPool := db
		if inTX {
			d = dbPool.Begin(ctx)
		} else {
			d = dbPool
		}
		for _, event := range values {
			if context.Err() != nil {
				return
			}
			err := handleAsyncEvent(ctx, d, event)
			if err != nil {
				if inTX {
					d.(DBTransaction).Rollback(ctx)
				}
				handleAsyncEventsOneByOne(context, ctx, list, db, r, values)
				return
			}
		}
		if inTX {
			d.(DBTransaction).Commit(ctx)
		}
		r.Ltrim(ctx, list, int64(len(values)), -1)
	}()
}

func handleAsyncEvent(ctx Context, db DBBase, value string) (err *mysql.MySQLError) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
				err = asMySQLError
				return
			}
			panic(rec)
		}
	}()
	var data []any
	_ = jsoniter.ConfigFastest.UnmarshalFromString(value, &data)
	if len(data) == 0 {
		return nil
	}
	sql, valid := data[0].(string)
	if !valid {
		return
	}
	if len(data) == 1 {
		db.Exec(ctx, sql)
		return nil
	}
	db.Exec(ctx, sql, data[1:]...)
	return nil
}

func handleAsyncEventsOneByOne(context context.Context, ctx Context, list string, db DB, r RedisCache, values []string) {
	for _, event := range values {
		if context.Err() != nil {
			return
		}
		err := handleAsyncEvent(ctx, db, event)
		if err != nil {
			r.RPush(ctx, list+flushAsyncEventsListErrorSuffix, event, err.Error())
		}
		r.Ltrim(ctx, list, 1, -1)
	}
}
