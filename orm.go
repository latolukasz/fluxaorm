package fluxaorm

import (
	"context"
	"hash/maphash"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v2"
)

const defaultContextCacheTTL int64 = 1000 // milliseconds

type ID interface {
	int | uint | uint8 | uint16 | uint32 | uint64 | int8 | int16 | int32 | int64
}

type Meta map[string]string

func (m Meta) Get(key string) string {
	return m[key]
}

type Context interface {
	Context() context.Context
	Clone() Context
	CloneWithContext(context context.Context) Context
	Engine() Engine
	DisableContextCache()
	SetContextCacheTTL(ttl time.Duration)
	Flush() error
	FlushAsync() error
	GetAsyncSQLConsumer() (AsyncSQLConsumer, error)
	ClearFlush()
	RedisPipeLine(pool string) *RedisPipeLine
	DatabasePipeLine(pool string) *DatabasePipeline
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	Track(e Entity, cacheIndex uint64)
	GetEventBroker() EventBroker
	getMetricsSourceTag() string
	GetFromContextCache(cacheIndex uint64, id uint64) Entity
	SetInContextCache(cacheIndex uint64, id uint64, entity Entity)
}

type ormImplementation struct {
	context                  context.Context
	engine                   *engineImplementation
	trackedEntities          *xsync.MapOf[uint64, *xsync.MapOf[uint64, Entity]]
	cachedEntities           *xsync.MapOf[uint64, *xsync.MapOf[uint64, Entity]]
	cachedEntitiesFirstAdded int64
	contextCacheTTL          int64
	queryLoggersDB           []LogHandler
	queryLoggersRedis        []LogHandler
	queryLoggersLocalCache   []LogHandler
	hasRedisLogger           bool
	hasDBLogger              bool
	hasLocalCacheLogger      bool
	disabledContextCache     bool
	meta                     Meta
	redisPipeLines           map[string]*RedisPipeLine
	dbPipeLines              map[string]*DatabasePipeline
	mutexFlush               sync.Mutex
	mutexData                sync.Mutex
	eventBroker              *eventBroker
}

func (orm *ormImplementation) Context() context.Context {
	return orm.context
}

func (orm *ormImplementation) CloneWithContext(context context.Context) Context {
	return &ormImplementation{
		context:                context,
		engine:                 orm.engine,
		queryLoggersDB:         orm.queryLoggersDB,
		queryLoggersRedis:      orm.queryLoggersRedis,
		queryLoggersLocalCache: orm.queryLoggersLocalCache,
		hasRedisLogger:         orm.hasRedisLogger,
		hasDBLogger:            orm.hasDBLogger,
		hasLocalCacheLogger:    orm.hasLocalCacheLogger,
		meta:                   orm.meta,
		disabledContextCache:   orm.disabledContextCache,
		contextCacheTTL:        orm.contextCacheTTL,
	}
}

func (orm *ormImplementation) Clone() Context {
	return orm.CloneWithContext(orm.context)
}

func (orm *ormImplementation) RedisPipeLine(pool string) *RedisPipeLine {
	if orm.redisPipeLines != nil {
		pipeline, has := orm.redisPipeLines[pool]
		if has {
			return pipeline
		}
	}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.redisPipeLines == nil {
		orm.redisPipeLines = make(map[string]*RedisPipeLine)
	}
	r := orm.engine.Redis(pool).(*redisCache)
	pipeline := &RedisPipeLine{ctx: orm, pool: pool, r: r, pipeLine: r.client.Pipeline()}
	orm.redisPipeLines[pool] = pipeline
	return pipeline
}

func (orm *ormImplementation) DatabasePipeLine(pool string) *DatabasePipeline {
	if orm.dbPipeLines != nil {
		pipeline, has := orm.dbPipeLines[pool]
		if has {
			return pipeline
		}
	}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.dbPipeLines == nil {
		orm.dbPipeLines = make(map[string]*DatabasePipeline)
	}
	db := orm.engine.DB(pool)
	pipeline := &DatabasePipeline{ctx: orm, pool: pool, db: db}
	orm.dbPipeLines[pool] = pipeline
	return pipeline
}

func (orm *ormImplementation) SetMetaData(key, value string) {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.meta == nil {
		orm.meta = Meta{key: value}
		return
	}
	orm.meta[key] = value
}

func (orm *ormImplementation) GetMetaData() Meta {
	return orm.meta
}

func (orm *ormImplementation) Engine() Engine {
	return orm.engine
}

func (orm *ormImplementation) getRedisLoggers() (bool, []LogHandler) {
	if orm.hasRedisLogger {
		return true, orm.queryLoggersRedis
	}
	return false, nil
}

func (orm *ormImplementation) getDBLoggers() (bool, []LogHandler) {
	if orm.hasDBLogger {
		return true, orm.queryLoggersDB
	}
	return false, nil
}

func (orm *ormImplementation) getLocalCacheLoggers() (bool, []LogHandler) {
	if orm.hasLocalCacheLogger {
		return true, orm.queryLoggersLocalCache
	}
	return false, nil
}

func (orm *ormImplementation) Track(f Entity, cacheIndex uint64) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil {
		orm.trackedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, Entity]](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	entities, loaded := orm.trackedEntities.LoadOrCompute(cacheIndex, func() *xsync.MapOf[uint64, Entity] {
		entities := xsync.NewTypedMapOf[uint64, Entity](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		entities.Store(f.GetID(), f)
		return entities
	})
	if loaded {
		entities.Store(f.GetID(), f)
	}
	if orm.cachedEntities != nil {
		if cached, ok := orm.cachedEntities.Load(cacheIndex); ok {
			cached.Delete(f.GetID())
		}
	}
}

func (orm *ormImplementation) getMetricsSourceTag() string {
	userTag, has := orm.meta[MetricsMetaKey]
	if has {
		return userTag
	}
	return "default"
}

func (orm *ormImplementation) DisableContextCache() {
	orm.disabledContextCache = true
}

func (orm *ormImplementation) SetContextCacheTTL(ttl time.Duration) {
	orm.contextCacheTTL = ttl.Milliseconds()
}

func (orm *ormImplementation) GetFromContextCache(cacheIndex uint64, id uint64) Entity {
	if orm.disabledContextCache || orm.cachedEntities == nil {
		return nil
	}
	if orm.cachedEntitiesFirstAdded > 0 && time.Now().UnixMilli()-orm.cachedEntitiesFirstAdded > orm.contextCacheTTL {
		orm.cachedEntities.Clear()
		orm.cachedEntitiesFirstAdded = 0
		return nil
	}
	entities, ok := orm.cachedEntities.Load(cacheIndex)
	if !ok {
		return nil
	}
	entity, ok := entities.Load(id)
	if !ok {
		return nil
	}
	return entity
}

func (orm *ormImplementation) SetInContextCache(cacheIndex uint64, id uint64, entity Entity) {
	if orm.disabledContextCache {
		return
	}
	if orm.cachedEntities == nil {
		orm.cachedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, Entity]](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	entities, _ := orm.cachedEntities.LoadOrCompute(cacheIndex, func() *xsync.MapOf[uint64, Entity] {
		return xsync.NewTypedMapOf[uint64, Entity](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	})
	entities.Store(id, entity)
	if orm.cachedEntitiesFirstAdded == 0 {
		orm.cachedEntitiesFirstAdded = time.Now().UnixMilli()
	}
}
