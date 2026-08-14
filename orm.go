package fluxaorm

import (
	"context"
	"hash/maphash"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v2"
)

const defaultContextCacheTTL int64 = 1000 // milliseconds

type xsyncEntityMap = xsync.MapOf[uint64, Entity]

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
	ClearFlush()
	Save(entities ...Entity) error
	Delete(entities ...Entity) error
	ForceDelete(entities ...Entity) error
	Transaction(fn func(tx Context) error) error
	InTransaction() bool
	DB(pool string) DBBase
	InvalidateCacheKey(pool, key string)
	RedisPipeLine(pool string) *RedisPipeLine
	DatabasePipeLine(pool string) *DatabasePipeline
	RegisterQueryLogger(handler LogHandler, options QueryLoggerOptions)
	EnableQueryDebug()
	EnableQueryDebugCustom(options QueryLoggerOptions)
	SetMetaData(key, value string)
	GetMetaData() Meta
	getDBLoggers() (bool, []LogHandler)
	getLocalCacheLoggers() (bool, []LogHandler)
	getClickhouseLoggers() (bool, []LogHandler)
	getNatsLoggers() (bool, []LogHandler)
	getRedisLoggers() (bool, []LogHandler)
	Track(e Entity, cacheIndex string)
	getMetricsSourceTag() string
	GetFromContextCache(cacheIndex string, id uint64) Entity
	SetInContextCache(cacheIndex string, id uint64, entity Entity)
}

type ormImplementation struct {
	context                  context.Context
	engine                   *engineImplementation
	trackedEntities          *xsync.MapOf[string, *xsync.MapOf[uint64, Entity]]
	cachedEntities           *xsync.MapOf[string, *xsync.MapOf[uint64, Entity]]
	cachedEntitiesFirstAdded int64
	contextCacheTTL          int64
	queryLoggersDB           []LogHandler
	queryLoggersRedis        []LogHandler
	queryLoggersLocalCache   []LogHandler
	queryLoggersClickhouse   []LogHandler
	queryLoggersNats         []LogHandler
	hasRedisLogger           bool
	hasDBLogger              bool
	hasLocalCacheLogger      bool
	hasClickhouseLogger      bool
	hasNatsLogger            bool
	disabledContextCache     bool
	meta                     Meta
	redisPipeLines           []*RedisPipeLine
	dbPipeLines              map[string]*DatabasePipeline
	tx                       *txState
	pendingInvalidations     map[string]map[string]bool
	mutexFlush               sync.Mutex
	mutexData                sync.Mutex
}

func (orm *ormImplementation) Context() context.Context {
	return orm.context
}

func (orm *ormImplementation) CloneWithContext(context context.Context) Context {
	orm.mutexData.Lock()
	var meta Meta
	if orm.meta != nil {
		meta = make(Meta, len(orm.meta))
		for k, v := range orm.meta {
			meta[k] = v
		}
	}
	orm.mutexData.Unlock()
	return &ormImplementation{
		context:                context,
		engine:                 orm.engine,
		queryLoggersDB:         orm.queryLoggersDB,
		queryLoggersRedis:      orm.queryLoggersRedis,
		queryLoggersLocalCache: orm.queryLoggersLocalCache,
		queryLoggersClickhouse: orm.queryLoggersClickhouse,
		queryLoggersNats:       orm.queryLoggersNats,
		hasRedisLogger:         orm.hasRedisLogger,
		hasDBLogger:            orm.hasDBLogger,
		hasLocalCacheLogger:    orm.hasLocalCacheLogger,
		hasClickhouseLogger:    orm.hasClickhouseLogger,
		hasNatsLogger:          orm.hasNatsLogger,
		meta:                   meta,
		disabledContextCache:   orm.disabledContextCache,
		contextCacheTTL:        orm.contextCacheTTL,
	}
}

func (orm *ormImplementation) Clone() Context {
	return orm.CloneWithContext(orm.context)
}

func (orm *ormImplementation) RedisPipeLine(pool string) *RedisPipeLine {
	r := orm.engine.Redis(pool).(*redisCache)
	pipeline := &RedisPipeLine{ctx: orm, pool: pool, r: r, pipeLine: r.client.Pipeline()}
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	orm.redisPipeLines = append(orm.redisPipeLines, pipeline)
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

func (orm *ormImplementation) getClickhouseLoggers() (bool, []LogHandler) {
	if orm.hasClickhouseLogger {
		return true, orm.queryLoggersClickhouse
	}
	return false, nil
}

func (orm *ormImplementation) getNatsLoggers() (bool, []LogHandler) {
	if orm.hasNatsLogger {
		return true, orm.queryLoggersNats
	}
	return false, nil
}

func (orm *ormImplementation) getLocalCacheLoggers() (bool, []LogHandler) {
	if orm.hasLocalCacheLogger {
		return true, orm.queryLoggersLocalCache
	}
	return false, nil
}

func (orm *ormImplementation) Track(f Entity, cacheIndex string) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil {
		orm.trackedEntities = xsync.NewMapOf[*xsync.MapOf[uint64, Entity]]()
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
			cached.Store(f.GetID(), f)
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

func (orm *ormImplementation) GetFromContextCache(cacheIndex string, id uint64) Entity {
	if orm.disabledContextCache || orm.cachedEntities == nil {
		return nil
	}
	if orm.cachedEntitiesFirstAdded > 0 && time.Now().UnixMilli()-orm.cachedEntitiesFirstAdded > orm.contextCacheTTL {
		orm.evictContextCache()
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

func (orm *ormImplementation) removeFromContextCache(cacheIndex string, id uint64) {
	if orm.cachedEntities == nil {
		return
	}
	if entities, ok := orm.cachedEntities.Load(cacheIndex); ok {
		entities.Delete(id)
	}
}

// evictContextCache drops expired identity-map entries but keeps entities that
// still hold unsaved changes - dropping those would serve a stale row to code
// running later in the same request.
func (orm *ormImplementation) evictContextCache() {
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		orm.cachedEntities.Clear()
		orm.cachedEntitiesFirstAdded = 0
		return
	}
	kept := false
	orm.cachedEntities.Range(func(cacheIndex string, entities *xsync.MapOf[uint64, Entity]) bool {
		tracked, hasTracked := orm.trackedEntities.Load(cacheIndex)
		entities.Range(func(id uint64, _ Entity) bool {
			if hasTracked {
				if _, dirty := tracked.Load(id); dirty {
					kept = true
					return true
				}
			}
			entities.Delete(id)
			return true
		})
		return true
	})
	if kept {
		orm.cachedEntitiesFirstAdded = time.Now().UnixMilli()
	} else {
		orm.cachedEntitiesFirstAdded = 0
	}
}

func (orm *ormImplementation) SetInContextCache(cacheIndex string, id uint64, entity Entity) {
	if orm.disabledContextCache {
		return
	}
	if orm.cachedEntities == nil {
		orm.cachedEntities = xsync.NewMapOf[*xsync.MapOf[uint64, Entity]]()
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
