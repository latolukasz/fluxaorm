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
	Redis(code string) RedisCache
	Registry() EngineRegistry
	Option(key string) any
	NextID() uint64
	SetNodeID(node int64)
}

type engineRegistryImplementation struct {
	engine                  *engineImplementation
	entitySchemas           map[reflect.Type]*entitySchema
	entitySchemasByIndex    map[string]*entitySchema
	defaultQueryLogger      *defaultLogLogger
	dbTables                map[string]map[string]bool
	options                 map[string]any
	clickhouseTables        []*ClickhouseTableBuilder
	clickhouseIgnoredTables map[string]map[string]bool
	natsStreams             []*NatsStreamBuilder
	natsIgnoredSubjects     map[string]map[string]bool
	natsConsumers           []*NatsConsumerBuilder
	natsIgnoredConsumers    map[string]map[string]bool
	dirtyStreams            map[NatsStreamName]*resolvedDirtyStream
	streamRegistry          map[NatsStreamName]*streamRegistryEntry
	dirtyPublishers         map[reflect.Type]*dirtyPublisherEntry
	cdcOutbox               *resolvedCDCOutbox
	hasMetrics              bool
	metricsRegistry         *metricsRegistry
}

type engineImplementation struct {
	registry            *engineRegistryImplementation
	dbServers           map[string]DB
	clickhouseServers   map[string]Clickhouse
	natsServers         map[string]Nats
	redisServers        map[string]RedisCache
	options             map[string]any
	afterInsertHandlers map[string]func(Context, Entity) error
	afterUpdateHandlers map[string]func(Context, Entity, map[string]any) error
	afterDeleteHandlers map[string]func(Context, Entity) error
	idGenerator         *snowflakeGenerator
}

func (e *engineImplementation) NextID() uint64 {
	return e.idGenerator.next()
}

func (e *engineImplementation) SetNodeID(node int64) {
	e.idGenerator.setNode(node)
}

func (e *engineImplementation) NewContext(context context.Context) Context {
	return &ormImplementation{context: context, engine: e}
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

func RegisterAfterInsertHandler(engine Engine, cacheIndex string, handler func(Context, Entity) error) {
	e := engine.(*engineImplementation)
	if e.afterInsertHandlers == nil {
		e.afterInsertHandlers = make(map[string]func(Context, Entity) error)
	}
	e.afterInsertHandlers[cacheIndex] = handler
}

func RegisterAfterUpdateHandler(engine Engine, cacheIndex string, handler func(Context, Entity, map[string]any) error) {
	e := engine.(*engineImplementation)
	if e.afterUpdateHandlers == nil {
		e.afterUpdateHandlers = make(map[string]func(Context, Entity, map[string]any) error)
	}
	e.afterUpdateHandlers[cacheIndex] = handler
}

func RegisterAfterDeleteHandler(engine Engine, cacheIndex string, handler func(Context, Entity) error) {
	e := engine.(*engineImplementation)
	if e.afterDeleteHandlers == nil {
		e.afterDeleteHandlers = make(map[string]func(Context, Entity) error)
	}
	e.afterDeleteHandlers[cacheIndex] = handler
}
