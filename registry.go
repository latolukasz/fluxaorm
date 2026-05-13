package fluxaorm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/pkg/errors"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql" // force this mysql driver
)

type Registry interface {
	Validate() (Engine, error)
	ValidateForCodeGen() (Engine, error)
	RegisterEntity(entity ...any)
	RegisterMySQL(dataSourceName string, poolCode string, poolOptions *MySQLOptions)
	RegisterLocalCache(code string, limit int)
	RegisterRedis(address string, db int, poolCode string, options *RedisOptions)
	InitByYaml(yaml any) error
	InitByConfig(config *Config) error
	SetOption(key string, value any)
	RegisterClickhouse(dataSourceName string, poolCode string, poolOptions *ClickhouseOptions)
	RegisterClickhouseTable(table *ClickhouseTableBuilder)
	RegisterNats(urls []string, poolCode string, options *NatsPoolOptions)
	RegisterNatsStream(stream *NatsStreamBuilder)
	RegisterNatsConsumer(consumer *NatsConsumerBuilder)
	RegisterAsyncFlush(natsPool string, options *AsyncFlushOptions)
	RegisterDebeziumServer(natsPool string, options *DebeziumOptions)
	EnableMetrics(factory promauto.Factory)
}

type registry struct {
	mysqlPools         map[string]MySQLConfig
	localCaches        map[string]LocalCache
	redisPools         map[string]RedisPoolConfig
	clickhousePools    map[string]ClickhouseConfig
	clickhouseTables   []*ClickhouseTableBuilder
	natsPools          map[string]*natsPoolConfig
	natsStreams        []*NatsStreamBuilder
	natsConsumers      []*NatsConsumerBuilder
	entities           map[string]reflect.Type
	options            map[string]any
	asyncFlushNatsPool string
	asyncFlushOptions  *AsyncFlushOptions
	debeziumNatsPools  map[string]bool
	debeziumOptions    map[string]*DebeziumOptions
	metricsFactory     *promauto.Factory
}

func NewRegistry() Registry {
	return &registry{}
}

func (r *registry) Validate() (Engine, error) {
	maxPoolLen := 0
	e := &engineImplementation{}
	e.registry = &engineRegistryImplementation{engine: e}
	e.registry.hasMetrics = r.metricsFactory != nil
	e.registry.options = make(map[string]any)
	l := len(r.entities)
	e.registry.entitySchemas = make(map[reflect.Type]*entitySchema, l)
	e.options = make(map[string]any)
	if e.dbServers == nil {
		e.dbServers = make(map[string]DB)
	}
	e.registry.dbTables = make(map[string]map[string]bool)
	for k, v := range r.mysqlPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		sourceURI := v.GetDataSourceURI()
		if strings.Contains(sourceURI, "?") {
			sourceURI += "&"
		} else {
			sourceURI += "?"
		}
		sourceURI += "parseTime=true&loc=UTC"
		db, err := sql.Open("mysql", sourceURI)
		if err != nil {
			return nil, err
		}

		var maxConnections int
		var skip string
		q := db.QueryRow("SHOW VARIABLES LIKE 'max_connections'")
		err = q.Scan(&skip, &maxConnections)
		if err != nil {
			return nil, err
		}
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		if err != nil {
			return nil, err
		}

		maxLimit := 100
		if v.GetOptions().MaxOpenConnections > 0 {
			maxLimit = int(math.Min(float64(v.GetOptions().MaxOpenConnections), float64(maxConnections)))
		} else {
			maxLimit = int(math.Min(float64(maxLimit), float64(maxConnections)))
		}
		maxIdle := maxLimit
		if v.GetOptions().MaxIdleConnections > 0 {
			maxIdle = int(math.Min(float64(v.GetOptions().MaxIdleConnections), float64(maxLimit)))
		}
		maxDuration := 5 * time.Minute
		if v.GetOptions().ConnMaxLifetime > 0 {
			maxDuration = time.Duration(int(math.Min(v.GetOptions().ConnMaxLifetime.Seconds(), float64(waitTimeout)))) * time.Second
		} else {
			maxDuration = time.Duration(int(math.Min(maxDuration.Seconds(), float64(waitTimeout)))) * time.Second
		}
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(maxDuration)
		options := v.GetOptions()
		if options.DefaultEncoding == "" {
			options.DefaultEncoding = "utf8mb4"
		}
		if options.DefaultCollate == "" {
			options.DefaultCollate = "0900_ai_ci"
		}
		if len(options.IgnoredTables) > 0 {
			if e.registry.dbTables[v.GetCode()] == nil {
				e.registry.dbTables[v.GetCode()] = make(map[string]bool)
			}
			for _, ignoredTable := range options.IgnoredTables {
				e.registry.dbTables[v.GetCode()][ignoredTable] = true
			}
		}
		v.(*mySQLConfig).client = db
		e.dbServers[k] = &dbImplementation{config: v, client: &standardSQLClient{db: v.getClient()}}
	}
	if e.clickhouseServers == nil {
		e.clickhouseServers = make(map[string]Clickhouse)
	}
	for k, v := range r.clickhousePools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		db, err := sql.Open("clickhouse", v.GetDataSourceURI())
		if err != nil {
			return nil, err
		}
		maxLimit := 100
		if v.GetOptions().MaxOpenConnections > 0 {
			maxLimit = v.GetOptions().MaxOpenConnections
		}
		maxIdle := maxLimit
		if v.GetOptions().MaxIdleConnections > 0 {
			maxIdle = v.GetOptions().MaxIdleConnections
		}
		maxDuration := 5 * time.Minute
		if v.GetOptions().ConnMaxLifetime > 0 {
			maxDuration = v.GetOptions().ConnMaxLifetime
		}
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(maxDuration)
		v.(*clickhouseConfig).client = db
		e.clickhouseServers[k] = &clickhouseImplementation{config: v, client: &standardSQLClient{db: v.getClient()}}
	}
	// Validate and register ClickHouse table definitions
	if len(r.clickhouseTables) > 0 {
		seenTableNames := make(map[string]string) // tableName -> poolCode
		for _, table := range r.clickhouseTables {
			if err := table.validate(); err != nil {
				return nil, err
			}
			if _, exists := r.clickhousePools[table.poolCode]; !exists {
				return nil, fmt.Errorf("clickhouse pool '%s' not registered for table '%s'", table.poolCode, table.tableName)
			}
			key := table.poolCode + "." + table.tableName
			if existingPool, exists := seenTableNames[key]; exists {
				return nil, fmt.Errorf("duplicate clickhouse table '%s' in pool '%s' (already registered in pool '%s')", table.tableName, table.poolCode, existingPool)
			}
			seenTableNames[key] = table.poolCode
		}
		e.registry.clickhouseTables = r.clickhouseTables
		// Build ignored tables map from ClickhouseOptions
		e.registry.clickhouseIgnoredTables = make(map[string]map[string]bool)
		for poolCode, poolConfig := range r.clickhousePools {
			if len(poolConfig.GetOptions().IgnoredTables) > 0 {
				if e.registry.clickhouseIgnoredTables[poolCode] == nil {
					e.registry.clickhouseIgnoredTables[poolCode] = make(map[string]bool)
				}
				for _, ignoredTable := range poolConfig.GetOptions().IgnoredTables {
					e.registry.clickhouseIgnoredTables[poolCode][ignoredTable] = true
				}
			}
		}
	}
	// Validate and register NATS stream definitions
	if len(r.natsStreams) > 0 {
		seenStreamNames := make(map[string]string) // streamName -> poolCode
		for _, stream := range r.natsStreams {
			if err := stream.validate(); err != nil {
				return nil, err
			}
			if _, exists := r.natsPools[stream.poolCode]; !exists {
				return nil, fmt.Errorf("nats pool '%s' not registered for stream '%s'", stream.poolCode, stream.streamName)
			}
			key := stream.poolCode + "." + stream.streamName
			if existingPool, exists := seenStreamNames[key]; exists {
				return nil, fmt.Errorf("duplicate nats stream '%s' in pool '%s' (already registered in pool '%s')", stream.streamName, stream.poolCode, existingPool)
			}
			seenStreamNames[key] = stream.poolCode
		}
		e.registry.natsStreams = r.natsStreams
		// Build ignored subjects map from NatsPoolOptions
		e.registry.natsIgnoredSubjects = make(map[string]map[string]bool)
		for poolCode, poolConfig := range r.natsPools {
			if len(poolConfig.options.IgnoredSubjects) > 0 {
				if e.registry.natsIgnoredSubjects[poolCode] == nil {
					e.registry.natsIgnoredSubjects[poolCode] = make(map[string]bool)
				}
				for _, ignoredSubject := range poolConfig.options.IgnoredSubjects {
					e.registry.natsIgnoredSubjects[poolCode][ignoredSubject] = true
				}
			}
		}
	}
	// Async flush configuration. Auto-register the async-flush consumer so callers can
	// resolve it via `pool.Consumer(AsyncSQLStreamName)` without an extra explicit registration.
	// The underlying JetStream stream/consumer are created by `GetNatsAlters`.
	if r.asyncFlushNatsPool != "" {
		pool, exists := r.natsPools[r.asyncFlushNatsPool]
		if !exists {
			return nil, fmt.Errorf("nats pool '%s' not registered for async flush", r.asyncFlushNatsPool)
		}
		e.registry.asyncFlushNatsPool = r.asyncFlushNatsPool
		e.registry.asyncFlushOptions = r.asyncFlushOptions
		if _, has := pool.consumers[AsyncSQLStreamName]; !has {
			cb := NewNatsConsumer(AsyncSQLStreamName, r.asyncFlushNatsPool).
				FilterSubjects(AsyncSQLSubject)
			if r.asyncFlushOptions != nil {
				if r.asyncFlushOptions.MaxAckPending > 0 {
					cb.MaxAckPending(r.asyncFlushOptions.MaxAckPending)
				}
				if r.asyncFlushOptions.AckWait > 0 {
					cb.AckWait(r.asyncFlushOptions.AckWait)
				}
				if r.asyncFlushOptions.MaxDeliver != 0 {
					cb.MaxDeliver(r.asyncFlushOptions.MaxDeliver)
				}
			}
			pool.consumers[AsyncSQLStreamName] = cb.toSettings()
		}
	}
	// Validate and register NATS consumer definitions
	if len(r.natsConsumers) > 0 {
		seenConsumerNames := make(map[string]string) // poolCode.name -> poolCode
		for _, cons := range r.natsConsumers {
			if err := cons.validate(); err != nil {
				return nil, err
			}
			pool, exists := r.natsPools[cons.poolCode]
			if !exists {
				return nil, fmt.Errorf("nats pool '%s' not registered for consumer '%s'", cons.poolCode, cons.name)
			}
			key := cons.poolCode + "." + cons.name
			if existingPool, exists := seenConsumerNames[key]; exists {
				return nil, fmt.Errorf("duplicate nats consumer '%s' in pool '%s' (already registered in pool '%s')", cons.name, cons.poolCode, existingPool)
			}
			seenConsumerNames[key] = cons.poolCode
			pool.consumers[cons.name] = cons.toSettings()
		}
		e.registry.natsConsumers = r.natsConsumers
		// Build ignored consumers map from NatsPoolOptions
		e.registry.natsIgnoredConsumers = make(map[string]map[string]bool)
		for poolCode, poolConfig := range r.natsPools {
			if len(poolConfig.options.IgnoredConsumers) > 0 {
				if e.registry.natsIgnoredConsumers[poolCode] == nil {
					e.registry.natsIgnoredConsumers[poolCode] = make(map[string]bool)
				}
				for _, ignored := range poolConfig.options.IgnoredConsumers {
					e.registry.natsIgnoredConsumers[poolCode][ignored] = true
				}
			}
		}
	}
	if e.natsServers == nil {
		e.natsServers = make(map[string]Nats)
	}
	for k, v := range r.natsPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		e.natsServers[k] = &natsPoolImplementation{config: v}
	}
	if e.localCacheServers == nil {
		e.localCacheServers = make(map[string]LocalCache)
	}
	if e.redisServers == nil {
		e.redisServers = make(map[string]RedisCache)
	}
	for k, v := range r.redisPools {
		client := v.getClient()
		server := &redisCache{config: v, client: client}
		info, err := client.Info(context.Background(), "server").Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get Redis server info for pool '%s': %w", k, err)
		}
		if err := validateRedisVersion(info, k); err != nil {
			return nil, err
		}
		e.redisServers[k] = server
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	// Sort entity names for deterministic index assignment
	entityNames := make([]string, 0, len(r.entities))
	for name := range r.entities {
		entityNames = append(entityNames, name)
	}
	sort.Strings(entityNames)
	index := uint64(0)
	for _, entityName := range entityNames {
		entityType := r.entities[entityName]
		schema := &entitySchema{engine: e, index: index}
		index++
		err := schema.init(r, entityType)
		if err != nil {
			return nil, err
		}
		e.registry.entitySchemas[entityType] = schema
		if schema.hasLocalCache {
			if r.localCaches == nil {
				r.localCaches = make(map[string]LocalCache)
			}
			r.localCaches[schema.cacheKey] = newLocalCache(schema.cacheKey, schema.localCacheLimit, schema)
		}
	}
	err := resolveSharedEnumDefinitions(e.registry.entitySchemas)
	if err != nil {
		return nil, err
	}
	for k, v := range r.localCaches {
		e.localCacheServers[k] = v
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
	}
	for _, schema := range e.registry.entitySchemas {
		if schema.hasLocalCache {
			schema.localCache = e.localCacheServers[schema.cacheKey].(*localCache)
		}
		if schema.hasRedisCache {
			schema.redisCache = e.redisServers[schema.redisCacheName].(*redisCache)
		}
	}
	// Build entitySchemasByIndex lookup
	e.registry.entitySchemasByIndex = make(map[uint64]*entitySchema)
	for _, schema := range e.registry.entitySchemas {
		e.registry.entitySchemasByIndex[schema.index] = schema
	}

	e.registry.defaultQueryLogger = &defaultLogLogger{maxPoolLen: maxPoolLen, logger: log.New(os.Stderr, "", 0)}
	for _, schema := range e.registry.entitySchemas {
		_, err := checkStruct(e, schema, schema.t, make(map[string]*IndexSchemaDefinition), nil, "", -1)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
		schema.engine = e
	}
	for key, value := range r.options {
		e.registry.options[key] = value
	}
	// Resolve DebeziumEntities to subject filters for NATS consumers
	for _, cons := range r.natsConsumers {
		if len(cons.debeziumEntityTypes) == 0 {
			continue
		}
		for _, entity := range cons.debeziumEntityTypes {
			entityType := reflect.TypeOf(entity)
			if entityType.Kind() == reflect.Ptr {
				entityType = entityType.Elem()
			}
			schema, ok := e.registry.entitySchemas[entityType]
			if !ok {
				return nil, fmt.Errorf("entity '%s' not registered (used in debezium nats consumer '%s')", entityType.String(), cons.name)
			}
			if schema.debeziumNatsPool == "" {
				return nil, fmt.Errorf("entity '%s' does not have debezium enabled (used in nats consumer '%s')", entityType.String(), cons.name)
			}
			db := e.dbServers[schema.mysqlPoolCode]
			dbName := db.GetConfig().GetDatabaseName()
			subject := "fluxa_" + schema.mysqlPoolCode + "." + dbName + "." + schema.tableName
			cons.filterSubjects = append(cons.filterSubjects, subject)
		}
		pool := r.natsPools[cons.poolCode]
		pool.consumers[cons.name] = cons.toSettings()
	}
	// Auto-register ignored NATS subjects for Debezium CDC
	if len(r.debeziumNatsPools) > 0 {
		e.registry.debeziumNatsPools = r.debeziumNatsPools
		e.registry.debeziumOptions = r.debeziumOptions
		debeziumIgnoredSubjects := make(map[string][]string) // natsPool -> subjects
		mysqlPoolsSeen := make(map[string]bool)
		for _, schema := range e.registry.entitySchemas {
			if schema.debeziumNatsPool == "" {
				continue
			}
			natsPool := schema.debeziumNatsPool
			mysqlPool := schema.mysqlPoolCode
			db := e.dbServers[mysqlPool]
			dbName := db.GetConfig().GetDatabaseName()
			subjectPrefix := "fluxa_" + mysqlPool
			dataSubject := subjectPrefix + "." + dbName + "." + schema.tableName
			debeziumIgnoredSubjects[natsPool] = append(debeziumIgnoredSubjects[natsPool], dataSubject)
			if !mysqlPoolsSeen[mysqlPool] {
				mysqlPoolsSeen[mysqlPool] = true
			}
		}
		for natsPool, subjects := range debeziumIgnoredSubjects {
			if e.registry.natsIgnoredSubjects == nil {
				e.registry.natsIgnoredSubjects = make(map[string]map[string]bool)
			}
			if e.registry.natsIgnoredSubjects[natsPool] == nil {
				e.registry.natsIgnoredSubjects[natsPool] = make(map[string]bool)
			}
			for _, subject := range subjects {
				e.registry.natsIgnoredSubjects[natsPool][subject] = true
			}
		}
	}
	if e.registry.hasMetrics {
		e.registry.metricsRegistry = initMetricsRegistry(*r.metricsFactory)
	}
	return e, nil
}

func (r *registry) RegisterAsyncFlush(natsPool string, options *AsyncFlushOptions) {
	r.asyncFlushNatsPool = natsPool
	r.asyncFlushOptions = options
}

func (r *registry) RegisterDebeziumServer(natsPool string, options *DebeziumOptions) {
	if r.debeziumNatsPools == nil {
		r.debeziumNatsPools = make(map[string]bool)
	}
	r.debeziumNatsPools[natsPool] = true
	if options != nil {
		if r.debeziumOptions == nil {
			r.debeziumOptions = make(map[string]*DebeziumOptions)
		}
		r.debeziumOptions[natsPool] = options
	}
}

func (r *registry) EnableMetrics(factory promauto.Factory) {
	r.metricsFactory = &factory
}

func (r *registry) ValidateForCodeGen() (Engine, error) {
	e := &engineImplementation{}
	e.registry = &engineRegistryImplementation{engine: e}
	e.registry.options = make(map[string]any)
	e.registry.entitySchemas = make(map[reflect.Type]*entitySchema, len(r.entities))
	e.options = make(map[string]any)
	e.dbServers = make(map[string]DB)
	e.registry.dbTables = make(map[string]map[string]bool)
	e.natsServers = make(map[string]Nats)
	e.redisServers = make(map[string]RedisCache)
	e.localCacheServers = make(map[string]LocalCache)
	e.clickhouseServers = make(map[string]Clickhouse)

	for k, v := range r.mysqlPools {
		options := v.GetOptions()
		if options == nil {
			options = &MySQLOptions{}
			v.(*mySQLConfig).options = options
		}
		if options.DefaultEncoding == "" {
			options.DefaultEncoding = "utf8mb4"
		}
		if options.DefaultCollate == "" {
			options.DefaultCollate = "0900_ai_ci"
		}
		e.dbServers[k] = &dbImplementation{config: v}
	}

	entityNames := make([]string, 0, len(r.entities))
	for name := range r.entities {
		entityNames = append(entityNames, name)
	}
	sort.Strings(entityNames)
	index := uint64(0)
	for _, entityName := range entityNames {
		entityType := r.entities[entityName]
		schema := &entitySchema{engine: e, index: index}
		index++
		err := schema.init(r, entityType)
		if err != nil {
			return nil, err
		}
		e.registry.entitySchemas[entityType] = schema
	}
	err := resolveSharedEnumDefinitions(e.registry.entitySchemas)
	if err != nil {
		return nil, err
	}
	e.registry.entitySchemasByIndex = make(map[uint64]*entitySchema)
	for _, schema := range e.registry.entitySchemas {
		e.registry.entitySchemasByIndex[schema.index] = schema
	}
	e.registry.defaultQueryLogger = &defaultLogLogger{maxPoolLen: 10, logger: log.New(os.Stderr, "", 0)}
	for _, schema := range e.registry.entitySchemas {
		_, err := checkStruct(e, schema, schema.t, make(map[string]*IndexSchemaDefinition), nil, "", -1)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
		schema.engine = e
	}
	return e, nil
}

func (r *registry) SetOption(key string, value any) {
	if r.options == nil {
		r.options = map[string]any{key: value}
		return
	}
	r.options[key] = value
}

func (r *registry) RegisterEntity(entity ...any) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		r.entities[t.String()] = t
	}
}

type MySQLOptions struct {
	ConnMaxLifetime    time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
	DefaultEncoding    string
	DefaultCollate     string
	IgnoredTables      []string
}

func (r *registry) RegisterMySQL(dataSourceName string, poolCode string, poolOptions *MySQLOptions) {
	db := &mySQLConfig{code: poolCode, dataSourceName: dataSourceName, options: poolOptions}
	if r.mysqlPools == nil {
		r.mysqlPools = make(map[string]MySQLConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]
	db.databaseName = dbName
	r.mysqlPools[poolCode] = db
}

func (r *registry) RegisterClickhouse(dataSourceName string, poolCode string, poolOptions *ClickhouseOptions) {
	if poolOptions == nil {
		poolOptions = &ClickhouseOptions{}
	}
	ch := &clickhouseConfig{code: poolCode, dataSourceName: dataSourceName, options: poolOptions}
	// Parse database name from DSN: clickhouse://host:port/dbname?params
	if idx := strings.Index(dataSourceName, "://"); idx >= 0 {
		rest := dataSourceName[idx+3:]
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			dbPart := rest[slashIdx+1:]
			if qIdx := strings.Index(dbPart, "?"); qIdx >= 0 {
				dbPart = dbPart[:qIdx]
			}
			ch.databaseName = dbPart
		}
	}
	if r.clickhousePools == nil {
		r.clickhousePools = make(map[string]ClickhouseConfig)
	}
	r.clickhousePools[poolCode] = ch
}

func (r *registry) RegisterClickhouseTable(table *ClickhouseTableBuilder) {
	r.clickhouseTables = append(r.clickhouseTables, table)
}

func (r *registry) RegisterNatsStream(stream *NatsStreamBuilder) {
	r.natsStreams = append(r.natsStreams, stream)
}

func (r *registry) RegisterNats(urls []string, poolCode string, options *NatsPoolOptions) {
	if options == nil {
		options = &NatsPoolOptions{}
	}
	n := &natsPoolConfig{code: poolCode, urls: urls, options: options, consumers: make(map[string]*NatsConsumerSettings)}
	if r.natsPools == nil {
		r.natsPools = make(map[string]*natsPoolConfig)
	}
	r.natsPools[poolCode] = n
}

func (r *registry) RegisterNatsConsumer(consumer *NatsConsumerBuilder) {
	r.natsConsumers = append(r.natsConsumers, consumer)
}

func (r *registry) RegisterLocalCache(code string, limit int) {
	if r.localCaches == nil {
		r.localCaches = make(map[string]LocalCache)
	}
	r.localCaches[code] = newLocalCache(code, limit, nil)
}

type RedisOptions struct {
	User            string
	Password        string
	Master          string
	Sentinels       []string
	SentinelOptions *redis.FailoverOptions
}

func (r *registry) RegisterRedis(address string, db int, poolCode string, options *RedisOptions) {
	if options != nil && len(options.Sentinels) > 0 {
		sentinelOptions := options.SentinelOptions
		if sentinelOptions == nil {
			sentinelOptions = &redis.FailoverOptions{
				MasterName:      options.Master,
				SentinelAddrs:   options.Sentinels,
				DB:              db,
				ConnMaxIdleTime: time.Minute * 2,
				Username:        options.User,
				Password:        options.Password,
			}
		}
		client := redis.NewFailoverClient(sentinelOptions)
		r.registerRedis(client, poolCode, fmt.Sprintf("%v", options.Sentinels), db)
		return
	}
	redisOptions := &redis.Options{
		Addr:            address,
		DB:              db,
		ConnMaxIdleTime: time.Minute * 2,
		UnstableResp3:   true,
	}
	if options != nil {
		redisOptions.Username = options.User
		redisOptions.Password = options.Password
	}
	if strings.HasSuffix(address, ".sock") {
		redisOptions.Network = "unix"
	}
	redisOptions.MaintNotificationsConfig = &maintnotifications.Config{
		Mode: maintnotifications.ModeDisabled,
	}
	client := redis.NewClient(redisOptions)
	r.registerRedis(client, poolCode, address, db)
}

func (r *registry) registerRedis(client *redis.Client, code string, address string, db int) {
	redisPool := &redisCacheConfig{code: code, client: client, address: address, db: db}
	if r.redisPools == nil {
		r.redisPools = make(map[string]RedisPoolConfig)
	}
	r.redisPools[code] = redisPool
}

type RedisPoolConfig interface {
	GetCode() string
	GetDatabaseNumber() int
	GetAddress() string
	getClient() *redis.Client
}

type redisCacheConfig struct {
	code    string
	client  *redis.Client
	db      int
	address string
}

func (p *redisCacheConfig) GetCode() string {
	return p.code
}

func (p *redisCacheConfig) GetDatabaseNumber() int {
	return p.db
}

func (p *redisCacheConfig) GetAddress() string {
	return p.address
}

func (p *redisCacheConfig) getClient() *redis.Client {
	return p.client
}

func validateRedisVersion(info string, pool string) error {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			version := strings.TrimPrefix(line, "redis_version:")
			parts := strings.Split(version, ".")
			if len(parts) < 2 {
				return fmt.Errorf("redis pool '%s': unable to parse version '%s'", pool, version)
			}
			major, err := strconv.Atoi(parts[0])
			if err != nil {
				return fmt.Errorf("redis pool '%s': unable to parse version '%s'", pool, version)
			}
			minor, err := strconv.Atoi(parts[1])
			if err != nil {
				return fmt.Errorf("redis pool '%s': unable to parse version '%s'", pool, version)
			}
			if major < 8 || (major == 8 && minor < 2) {
				return fmt.Errorf("redis pool '%s' version %s is not supported, minimum required is 8.2", pool, version)
			}
			return nil
		}
	}
	return fmt.Errorf("redis pool '%s': unable to determine Redis version from INFO", pool)
}

type enumSource struct {
	def        *enumDefinition
	entityName string
}

func resolveSharedEnumDefinitions(schemas map[reflect.Type]*entitySchema) error {
	defs := map[string]enumSource{}

	// Phase 1: Collect all definitions with values (not references)
	for entityType, schema := range schemas {
		err := collectEnumDefs(schema.fields, entityType.String(), defs)
		if err != nil {
			return err
		}
	}

	// Phase 2: Resolve references
	for entityType, schema := range schemas {
		err := resolveEnumRefs(schema, schema.fields, entityType.String(), defs)
		if err != nil {
			return err
		}
	}
	return nil
}

func enumFieldsEqual(a, b *enumDefinition) bool {
	if len(a.fields) != len(b.fields) {
		return false
	}
	for i, v := range a.fields {
		if b.fields[i] != v {
			return false
		}
	}
	return true
}

func collectEnumDefs(fields *tableFields, entityName string, defs map[string]enumSource) error {
	for _, def := range fields.enums {
		if def.isReference() {
			continue
		}
		if existing, exists := defs[def.name]; exists {
			if existing.entityName != entityName && !enumFieldsEqual(existing.def, def) {
				return fmt.Errorf("enum/set '%s' has conflicting values defined in both '%s' and '%s', definition must be in only one entity",
					def.name, existing.entityName, entityName)
			}
		} else {
			defs[def.name] = enumSource{def: def, entityName: entityName}
		}
	}
	for _, def := range fields.sets {
		if def.isReference() {
			continue
		}
		if existing, exists := defs[def.name]; exists {
			if existing.entityName != entityName && !enumFieldsEqual(existing.def, def) {
				return fmt.Errorf("enum/set '%s' has conflicting values defined in both '%s' and '%s', definition must be in only one entity",
					def.name, existing.entityName, entityName)
			}
		} else {
			defs[def.name] = enumSource{def: def, entityName: entityName}
		}
	}
	for _, subFields := range fields.structsFields {
		err := collectEnumDefs(subFields, entityName, defs)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveEnumRefs(schema *entitySchema, fields *tableFields, entityName string, defs map[string]enumSource) error {
	for _, def := range fields.enums {
		if !def.isReference() {
			continue
		}
		source, exists := defs[def.name]
		if !exists {
			return fmt.Errorf("enum/set '%s' referenced in '%s' but no entity defines its values", def.name, entityName)
		}
		def.fields = source.def.fields
		def.fieldNames = source.def.fieldNames
		def.mapping = source.def.mapping
		def.defaultValue = source.def.defaultValue
		// Update schema.tags so checkColumn reads correct values for SQL generation
		for fieldName, fieldTags := range schema.tags {
			if fieldTags["enumName"] != def.name {
				continue
			}
			if _, hasEnum := fieldTags["enum"]; !hasEnum || fieldTags["enum"] == "true" {
				fieldTags["enum"] = strings.Join(source.def.fields, ",")
				schema.tags[fieldName] = fieldTags
			}
		}
	}
	for _, def := range fields.sets {
		if !def.isReference() {
			continue
		}
		source, exists := defs[def.name]
		if !exists {
			return fmt.Errorf("enum/set '%s' referenced in '%s' but no entity defines its values", def.name, entityName)
		}
		def.fields = source.def.fields
		def.fieldNames = source.def.fieldNames
		def.mapping = source.def.mapping
		def.defaultValue = source.def.defaultValue
		// Update schema.tags so checkColumn reads correct values for SQL generation
		for fieldName, fieldTags := range schema.tags {
			if fieldTags["enumName"] != def.name {
				continue
			}
			if _, hasSet := fieldTags["set"]; !hasSet || fieldTags["set"] == "true" {
				fieldTags["set"] = strings.Join(source.def.fields, ",")
				schema.tags[fieldName] = fieldTags
			}
		}
	}
	for _, subFields := range fields.structsFields {
		err := resolveEnumRefs(schema, subFields, entityName, defs)
		if err != nil {
			return err
		}
	}
	return nil
}
