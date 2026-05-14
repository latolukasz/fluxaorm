package fluxaorm

import (
	"context"
	"reflect"
)

const DefaultPoolCode = "default"

type EngineRegistry interface {
	DBPools() map[string]DB
	ClickhousePools() map[string]Clickhouse
	NatsPools() map[string]Nats
	LocalCachePools() map[string]LocalCache
	RedisPools() map[string]RedisCache
	Option(key string) any
	getDefaultQueryLogger() LogHandler
	getDBTables() map[string]map[string]bool
	getMetricsRegistry() (*metricsRegistry, bool)
}

type EngineSetter interface {
	SetOption(key string, value any)
}

type Engine interface {
	NewContext(parent context.Context) Context
	DB(code string) DB
	Clickhouse(code string) Clickhouse
	Nats(code string) Nats
	LocalCache(code string) LocalCache
	Redis(code string) RedisCache
	Registry() EngineRegistry
	Option(key string) any
}

type engineRegistryImplementation struct {
	engine                  *engineImplementation
	entitySchemas           map[reflect.Type]*entitySchema
	entitySchemasByIndex    map[uint64]*entitySchema
	defaultQueryLogger      *defaultLogLogger
	dbTables                map[string]map[string]bool
	options                 map[string]any
	asyncFlushNatsPool      string
	asyncFlushOptions       *AsyncFlushOptions
	clickhouseTables        []*ClickhouseTableBuilder
	clickhouseIgnoredTables map[string]map[string]bool
	natsStreams             []*NatsStreamBuilder
	natsIgnoredSubjects     map[string]map[string]bool
	natsConsumers           []*NatsConsumerBuilder
	natsIgnoredConsumers    map[string]map[string]bool
	dirtyStreams            map[NatsStreamName]*resolvedDirtyStream
	streamRegistry          map[NatsStreamName]*streamRegistryEntry
	dirtyPublishers         map[reflect.Type]*dirtyPublisherEntry
	hasMetrics              bool
	metricsRegistry         *metricsRegistry
}

type engineImplementation struct {
	registry            *engineRegistryImplementation
	localCacheServers   map[string]LocalCache
	dbServers           map[string]DB
	clickhouseServers   map[string]Clickhouse
	natsServers         map[string]Nats
	redisServers        map[string]RedisCache
	options             map[string]any
	afterInsertHandlers map[uint64]func(Context, Entity) error
	afterUpdateHandlers map[uint64]func(Context, Entity, map[string]any) error
	afterDeleteHandlers map[uint64]func(Context, Entity) error
	entityLoaders       map[uint64]func(Context, uint64) (Entity, bool, error)
	entityDBPools       map[uint64]string
}

func (e *engineImplementation) NewContext(context context.Context) Context {
	return &ormImplementation{context: context, engine: e, contextCacheTTL: defaultContextCacheTTL}
}

func (e *engineImplementation) Registry() EngineRegistry {
	return e.registry
}

func (e *engineRegistryImplementation) getMetricsRegistry() (*metricsRegistry, bool) {
	return e.metricsRegistry, e.hasMetrics
}

func (e *engineImplementation) Option(key string) any {
	return e.options[key]
}

func (e *engineImplementation) SetOption(key string, value any) {
	e.options[key] = value
}

func (e *engineImplementation) Clickhouse(code string) Clickhouse {
	return e.clickhouseServers[code]
}

func (e *engineImplementation) Nats(code string) Nats {
	return e.natsServers[code]
}

func (e *engineImplementation) DB(code string) DB {
	return e.dbServers[code]
}

func (e *engineImplementation) LocalCache(code string) LocalCache {
	return e.localCacheServers[code]
}

func (e *engineImplementation) Redis(code string) RedisCache {
	return e.redisServers[code]
}

func (er *engineRegistryImplementation) ClickhousePools() map[string]Clickhouse {
	return er.engine.clickhouseServers
}

func (er *engineRegistryImplementation) NatsPools() map[string]Nats {
	return er.engine.natsServers
}

func (er *engineRegistryImplementation) RedisPools() map[string]RedisCache {
	return er.engine.redisServers
}

func (er *engineRegistryImplementation) LocalCachePools() map[string]LocalCache {
	return er.engine.localCacheServers
}

func (er *engineRegistryImplementation) DBPools() map[string]DB {
	return er.engine.dbServers
}

func (er *engineRegistryImplementation) getDBTables() map[string]map[string]bool {
	return er.dbTables
}

func (er *engineRegistryImplementation) Option(key string) any {
	return er.options[key]
}

func (er *engineRegistryImplementation) getDefaultQueryLogger() LogHandler {
	return er.defaultQueryLogger
}

func RegisterEntityLoader(engine Engine, cacheIndex uint64, dbPool string, loader func(Context, uint64) (Entity, bool, error)) {
	e := engine.(*engineImplementation)
	if e.entityLoaders == nil {
		e.entityLoaders = make(map[uint64]func(Context, uint64) (Entity, bool, error))
		e.entityDBPools = make(map[uint64]string)
	}
	e.entityLoaders[cacheIndex] = loader
	e.entityDBPools[cacheIndex] = dbPool
}

func RegisterAfterInsertHandler(engine Engine, cacheIndex uint64, handler func(Context, Entity) error) {
	e := engine.(*engineImplementation)
	if e.afterInsertHandlers == nil {
		e.afterInsertHandlers = make(map[uint64]func(Context, Entity) error)
	}
	e.afterInsertHandlers[cacheIndex] = handler
}

func RegisterAfterUpdateHandler(engine Engine, cacheIndex uint64, handler func(Context, Entity, map[string]any) error) {
	e := engine.(*engineImplementation)
	if e.afterUpdateHandlers == nil {
		e.afterUpdateHandlers = make(map[uint64]func(Context, Entity, map[string]any) error)
	}
	e.afterUpdateHandlers[cacheIndex] = handler
}

func RegisterAfterDeleteHandler(engine Engine, cacheIndex uint64, handler func(Context, Entity) error) {
	e := engine.(*engineImplementation)
	if e.afterDeleteHandlers == nil {
		e.afterDeleteHandlers = make(map[uint64]func(Context, Entity) error)
	}
	e.afterDeleteHandlers[cacheIndex] = handler
}
