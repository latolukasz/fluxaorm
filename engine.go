package fluxaorm

import (
	"context"
	"reflect"
)

const DefaultPoolCode = "default"

type EngineRegistry interface {
	DBPools() map[string]DB
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
	LocalCache(code string) LocalCache
	Redis(code string) RedisCache
	Registry() EngineRegistry
	Option(key string) any
	GetRedisStreams() map[string]map[string]string
}

type engineRegistryImplementation struct {
	engine             *engineImplementation
	entitySchemas      map[reflect.Type]*entitySchema
	defaultQueryLogger *defaultLogLogger
	dbTables           map[string]map[string]bool
	options            map[string]any
	redisStreamGroups  map[string]map[string]string
	redisStreamPools   map[string]string
	hasMetrics         bool
	metricsRegistry    *metricsRegistry
}

type engineImplementation struct {
	registry                     *engineRegistryImplementation
	localCacheServers            map[string]LocalCache
	dbServers                    map[string]DB
	redisServers                 map[string]RedisCache
	options                      map[string]any
	asyncTemporaryIsQueueRunning bool
}

func (e *engineImplementation) NewContext(context context.Context) Context {
	return &ormImplementation{context: context, engine: e, disabledContextCache: true}
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

func (e *engineImplementation) DB(code string) DB {
	return e.dbServers[code]
}

func (e *engineImplementation) LocalCache(code string) LocalCache {
	return e.localCacheServers[code]
}

func (e *engineImplementation) Redis(code string) RedisCache {
	return e.redisServers[code]
}

func (e *engineImplementation) GetRedisStreams() map[string]map[string]string {
	res := make(map[string]map[string]string)
	for redisPool, row := range e.registry.redisStreamGroups {
		res[redisPool] = make(map[string]string)
		for stream, group := range row {
			res[redisPool][stream] = group
		}
	}
	return res
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
