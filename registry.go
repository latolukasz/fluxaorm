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
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql" // force this mysql driver
)

type Registry interface {
	Validate() (Engine, error)
	RegisterEntity(entity ...any)
	RegisterMySQL(dataSourceName string, poolCode string, poolOptions *MySQLOptions)
	RegisterLocalCache(code string, limit int)
	RegisterRedis(address string, db int, poolCode string, options *RedisOptions)
	InitByYaml(yaml any) error
	InitByConfig(config *Config) error
	SetOption(key string, value any)
	RegisterClickhouse(dataSourceName string, poolCode string, poolOptions *ClickhouseOptions)
	RegisterClickhouseTable(table *ClickhouseTableBuilder)
	RegisterKafka(brokers []string, poolCode string, options *KafkaPoolOptions)
	RegisterKafkaTopic(topic *KafkaTopicBuilder)
	RegisterKafkaConsumerGroup(consumerGroup *KafkaConsumerGroupBuilder)
	RegisterRedisStream(name string, redisPool string)
	RegisterAsyncSQLStream(redisPool string)
	EnableMetrics(factory promauto.Factory)
}

type registry struct {
	mysqlPools          map[string]MySQLConfig
	localCaches         map[string]LocalCache
	redisPools          map[string]RedisPoolConfig
	clickhousePools     map[string]ClickhouseConfig
	clickhouseTables    []*ClickhouseTableBuilder
	kafkaPools          map[string]*kafkaPoolConfig
	kafkaTopics         []*KafkaTopicBuilder
	kafkaConsumerGroups []*KafkaConsumerGroupBuilder
	entities            map[string]reflect.Type
	options             map[string]any
	redisStreamGroups   map[string]map[string]string
	redisStreamPools    map[string]string
	metricsFactory      *promauto.Factory
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
	// Validate and register Kafka topic definitions
	if len(r.kafkaTopics) > 0 {
		seenTopicNames := make(map[string]string) // topicName -> poolCode
		for _, topic := range r.kafkaTopics {
			if err := topic.validate(); err != nil {
				return nil, err
			}
			if _, exists := r.kafkaPools[topic.poolCode]; !exists {
				return nil, fmt.Errorf("kafka pool '%s' not registered for topic '%s'", topic.poolCode, topic.topicName)
			}
			key := topic.poolCode + "." + topic.topicName
			if existingPool, exists := seenTopicNames[key]; exists {
				return nil, fmt.Errorf("duplicate kafka topic '%s' in pool '%s' (already registered in pool '%s')", topic.topicName, topic.poolCode, existingPool)
			}
			seenTopicNames[key] = topic.poolCode
		}
		e.registry.kafkaTopics = r.kafkaTopics
		// Build ignored topics map from KafkaPoolOptions
		e.registry.kafkaIgnoredTopics = make(map[string]map[string]bool)
		for poolCode, poolConfig := range r.kafkaPools {
			if len(poolConfig.options.IgnoredTopics) > 0 {
				if e.registry.kafkaIgnoredTopics[poolCode] == nil {
					e.registry.kafkaIgnoredTopics[poolCode] = make(map[string]bool)
				}
				for _, ignoredTopic := range poolConfig.options.IgnoredTopics {
					e.registry.kafkaIgnoredTopics[poolCode][ignoredTopic] = true
				}
			}
		}
	}
	// Validate and register Kafka consumer group definitions
	if len(r.kafkaConsumerGroups) > 0 {
		seenCGNames := make(map[string]string) // poolCode.name -> poolCode
		for _, cg := range r.kafkaConsumerGroups {
			if err := cg.validate(); err != nil {
				return nil, err
			}
			pool, exists := r.kafkaPools[cg.poolCode]
			if !exists {
				return nil, fmt.Errorf("kafka pool '%s' not registered for consumer group '%s'", cg.poolCode, cg.name)
			}
			key := cg.poolCode + "." + cg.name
			if existingPool, exists := seenCGNames[key]; exists {
				return nil, fmt.Errorf("duplicate kafka consumer group '%s' in pool '%s' (already registered in pool '%s')", cg.name, cg.poolCode, existingPool)
			}
			seenCGNames[key] = cg.poolCode
			pool.consumerGroups[cg.name] = cg.toSettings()
		}
		e.registry.kafkaConsumerGroups = r.kafkaConsumerGroups
		// Build ignored consumer groups map from KafkaPoolOptions
		e.registry.kafkaIgnoredConsumerGroups = make(map[string]map[string]bool)
		for poolCode, poolConfig := range r.kafkaPools {
			if len(poolConfig.options.IgnoredConsumerGroups) > 0 {
				if e.registry.kafkaIgnoredConsumerGroups[poolCode] == nil {
					e.registry.kafkaIgnoredConsumerGroups[poolCode] = make(map[string]bool)
				}
				for _, ignoredCG := range poolConfig.options.IgnoredConsumerGroups {
					e.registry.kafkaIgnoredConsumerGroups[poolCode][ignoredCG] = true
				}
			}
		}
	}
	// Determine which pools have registered topics (for disabling auto-creation)
	poolsWithTopics := make(map[string]bool)
	for _, topic := range r.kafkaTopics {
		poolsWithTopics[topic.poolCode] = true
	}
	if e.kafkaServers == nil {
		e.kafkaServers = make(map[string]Kafka)
	}
	for k, v := range r.kafkaPools {
		if len(k) > maxPoolLen {
			maxPoolLen = len(k)
		}
		// Validate SASL mechanism
		if v.options.SASL != nil {
			switch v.options.SASL.Mechanism {
			case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
			default:
				return nil, fmt.Errorf("kafka pool '%s': unsupported SASL mechanism '%s'", k, v.options.SASL.Mechanism)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		opts := buildProducerKgoOpts(v, poolsWithTopics[k])
		opts = append(opts, kgo.WithContext(ctx))
		producerClient, err := kgo.NewClient(opts...)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("kafka pool '%s': failed to create producer client: %w", k, err)
		}
		if err := producerClient.Ping(context.Background()); err != nil {
			producerClient.Close()
			cancel()
			return nil, fmt.Errorf("kafka pool '%s': failed to connect producer: %w", k, err)
		}
		e.kafkaServers[k] = &kafkaPoolImplementation{
			config:              v,
			producerClient:      producerClient,
			producerCancel:      cancel,
			hasRegisteredTopics: poolsWithTopics[k],
		}
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

	// Auto-register dirty streams
	for _, schema := range e.registry.entitySchemas {
		if schema.hasDirtyStreams {
			for _, ds := range schema.dirtyStreams {
				if existingPool, already := r.redisStreamPools[ds.streamName]; already {
					if existingPool != ds.redisPoolCode {
						return nil, fmt.Errorf("dirty stream '%s' uses conflicting Redis pools: '%s' vs '%s'", ds.streamName, existingPool, ds.redisPoolCode)
					}
					continue
				}
				r.RegisterRedisStream(ds.streamName, ds.redisPoolCode)
			}
		}
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
	// Auto-register async SQL streams on the default Redis pool if not already configured.
	if _, hasAsyncSQL := r.redisStreamPools[AsyncSQLStreamName]; !hasAsyncSQL {
		if _, hasDefault := r.redisPools[DefaultPoolCode]; hasDefault {
			r.RegisterRedisStream(AsyncSQLStreamName, DefaultPoolCode)
			r.RegisterRedisStream(AsyncSQLDeadLetterStreamName, DefaultPoolCode)
		}
	}
	e.registry.redisStreamGroups = r.redisStreamGroups
	e.registry.redisStreamPools = r.redisStreamPools
	if e.registry.hasMetrics {
		e.registry.metricsRegistry = initMetricsRegistry(*r.metricsFactory)
	}
	return e, nil
}

func (r *registry) RegisterRedisStream(name string, redisPool string) {
	if r.redisStreamGroups == nil {
		r.redisStreamGroups = make(map[string]map[string]string)
		r.redisStreamPools = make(map[string]string)
	}
	r.redisStreamPools[name] = redisPool
	if r.redisStreamGroups[redisPool] == nil {
		r.redisStreamGroups[redisPool] = make(map[string]string)
	}
	r.redisStreamGroups[redisPool][name] = consumerGroupName
}

func (r *registry) RegisterAsyncSQLStream(redisPool string) {
	r.RegisterRedisStream(AsyncSQLStreamName, redisPool)
	r.RegisterRedisStream(AsyncSQLDeadLetterStreamName, redisPool)
}

func (r *registry) EnableMetrics(factory promauto.Factory) {
	r.metricsFactory = &factory
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

func (r *registry) RegisterKafkaTopic(topic *KafkaTopicBuilder) {
	r.kafkaTopics = append(r.kafkaTopics, topic)
}

func (r *registry) RegisterKafka(brokers []string, poolCode string, options *KafkaPoolOptions) {
	if options == nil {
		options = &KafkaPoolOptions{}
	}
	k := &kafkaPoolConfig{code: poolCode, brokers: brokers, options: options, consumerGroups: make(map[string]*KafkaConsumerGroupSettings)}
	if r.kafkaPools == nil {
		r.kafkaPools = make(map[string]*kafkaPoolConfig)
	}
	r.kafkaPools[poolCode] = k
}

func (r *registry) RegisterKafkaConsumerGroup(consumerGroup *KafkaConsumerGroupBuilder) {
	r.kafkaConsumerGroups = append(r.kafkaConsumerGroups, consumerGroup)
}

func buildProducerKgoOpts(pool *kafkaPoolConfig, hasRegisteredTopics bool) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(pool.brokers...)}
	if !hasRegisteredTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	poolOpts := pool.options
	if poolOpts.ClientID != "" {
		opts = append(opts, kgo.ClientID(poolOpts.ClientID))
	}
	if poolOpts.RequiredAcks != 0 {
		switch poolOpts.RequiredAcks {
		case 1:
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		case -1:
			opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		case 0:
			opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
		}
	}
	if poolOpts.ProducerLinger > 0 {
		opts = append(opts, kgo.ProducerLinger(poolOpts.ProducerLinger))
	}
	if poolOpts.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(poolOpts.MaxBufferedRecords))
	}
	if poolOpts.SASL != nil {
		switch poolOpts.SASL.Mechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: poolOpts.SASL.User, Pass: poolOpts.SASL.Password}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: poolOpts.SASL.User, Pass: poolOpts.SASL.Password}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: poolOpts.SASL.User, Pass: poolOpts.SASL.Password}.AsSha512Mechanism()))
		}
	}
	return opts
}

func buildConsumerKgoOpts(pool *kafkaPoolConfig, settings *KafkaConsumerGroupSettings, hasRegisteredTopics bool) []kgo.Opt {
	opts := buildProducerKgoOpts(pool, hasRegisteredTopics)
	if settings.Name != "" {
		opts = append(opts, kgo.ConsumerGroup(settings.Name))
	}
	if len(settings.Topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(settings.Topics...))
	}
	if settings.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(settings.SessionTimeout))
	}
	if settings.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(settings.RebalanceTimeout))
	}
	if settings.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(settings.FetchMaxBytes))
	}
	if settings.AutoCommitInterval > 0 {
		opts = append(opts, kgo.AutoCommitInterval(settings.AutoCommitInterval))
	} else if settings.Name != "" {
		opts = append(opts, kgo.DisableAutoCommit())
	}
	opts = append(opts, kgo.MetadataMinAge(time.Second))
	return opts
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
