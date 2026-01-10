package fluxaorm

import (
	"context"
	"hash/maphash"
	"strings"
	"sync"

	"github.com/puzpuzpuz/xsync/v2"
)

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
	NewEntity(entity any) error
	EditEntity(entity any) (any, error)
	DeleteEntity(entity any) error
	ForceDeleteEntity(entity any) error
	PushDirty(entities ...any) error
	ClearCache()
	EnableContextCache()
	Flush() error
	FlushAsync() error
	ClearFlush()
	RedisPipeLine(pool string) *RedisPipeLine
	RegisterQueryLogger(handler LogHandler, mysql, redis, local bool)
	EnableQueryDebug()
	EnableQueryDebugCustom(mysql, redis, local bool)
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	trackEntity(e EntityFlush)
	GetEventBroker() EventBroker
	getEntityFromCache(schema *entitySchema, id uint64) (e any, found bool)
	cacheEntity(schema *entitySchema, id uint64, e any)
	getMetricsSourceTag() string
}

type ormImplementation struct {
	context                context.Context
	engine                 *engineImplementation
	trackedEntities        *xsync.MapOf[uint64, *xsync.MapOf[uint64, EntityFlush]]
	cachedEntities         *xsync.MapOf[uint64, *xsync.MapOf[uint64, any]]
	queryLoggersDB         []LogHandler
	queryLoggersRedis      []LogHandler
	queryLoggersLocalCache []LogHandler
	hasRedisLogger         bool
	hasDBLogger            bool
	hasLocalCacheLogger    bool
	disabledContextCache   bool
	meta                   Meta
	stringBuilder          *strings.Builder
	stringBuilder2         *strings.Builder
	redisPipeLines         map[string]*RedisPipeLine
	flushDBActions         map[string][]dbAction
	flushPostActions       []func(ctx Context)
	mutexFlush             sync.Mutex
	mutexData              sync.Mutex
	eventBroker            *eventBroker
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

func (orm *ormImplementation) trackEntity(e EntityFlush) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil {
		orm.trackedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, EntityFlush]](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	entities, loaded := orm.trackedEntities.LoadOrCompute(e.Schema().index, func() *xsync.MapOf[uint64, EntityFlush] {
		entities := xsync.NewTypedMapOf[uint64, EntityFlush](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		entities.Store(e.ID(), e)
		return entities
	})
	if loaded {
		entities.Store(e.ID(), e)
	}
}

func (orm *ormImplementation) cacheEntity(schema *entitySchema, id uint64, e any) {
	if orm.disabledContextCache {
		return
	}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.cachedEntities == nil {
		orm.cachedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, any]](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
	}
	entities, loaded := orm.cachedEntities.LoadOrCompute(schema.index, func() *xsync.MapOf[uint64, any] {
		entities := xsync.NewTypedMapOf[uint64, any](func(seed maphash.Seed, u uint64) uint64 {
			return u
		})
		entities.Store(id, e)
		return entities
	})
	if loaded {
		entities.Store(id, e)
	}
}

func (orm *ormImplementation) getMetricsSourceTag() string {
	userTag, has := orm.meta[MetricsMetaKey]
	if has {
		return userTag
	}
	return "default"
}

func (orm *ormImplementation) getEntityFromCache(schema *entitySchema, id uint64) (e any, found bool) {
	if orm.disabledContextCache {
		return nil, false
	}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.cachedEntities == nil {
		return nil, false
	}
	entities, found := orm.cachedEntities.Load(schema.index)
	if !found {
		return nil, false
	}
	e, found = entities.Load(id)
	if e == nil {
		return nil, false
	}
	return e, found
}

func (orm *ormImplementation) ClearCache() {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	orm.cachedEntities = nil
}

func (orm *ormImplementation) EnableContextCache() {
	orm.disabledContextCache = false
}
