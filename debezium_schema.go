package fluxaorm

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// DebeziumOptions holds optional overrides for Debezium Server configuration.
// Useful when Debezium Server runs in Docker and needs different addresses than the Go application.
type DebeziumOptions struct {
	// MySQLHost overrides the MySQL hostname used in generated source config.
	MySQLHost string
	// MySQLPort overrides the MySQL port used in generated source config.
	MySQLPort string
	// NatsURLs overrides the NATS server URLs used in the sink config (e.g. when Debezium Server
	// runs in a container with a different network address than the Go application).
	NatsURLs []string
	// RedisPool overrides the fluxaorm Redis pool used for Debezium Server offset and schema-history
	// storage (defaults to DefaultPoolCode).
	RedisPool string
	// StreamConfig optionally tunes the JetStream stream that receives CDC events for this Debezium pool.
	// If nil, defaults are used (FileStorage, LimitsPolicy, 1 replica, single subject `fluxa_<mysqlPool>.>`).
	StreamConfig *NatsStreamBuilder
}

// GenerateDebeziumServerProperties returns one set of Debezium Server `application.properties`
// per registered MySQL pool that has at least one entity marked `orm:"debezium=<natsPool>"`.
// The user mounts the resulting file(s) into a `debezium/server` container.
//
// Source side configuration uses the Debezium MySQL connector.
// Sink side configuration uses `nats-jetstream` with `create-stream=false` because fluxaorm
// manages stream creation/tuning via `GetNatsAlters` (so retention/replicas/storage stay tunable).
// Offset and schema-history storage default to Redis-backed (reusing fluxaorm's Redis pool)
// for HA and restart durability.
func GenerateDebeziumServerProperties(ctx Context) (map[string]map[string]string, error) {
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)

	if len(reg.debeziumNatsPools) == 0 {
		return nil, nil
	}

	type connectorKey struct {
		natsPool  string
		mysqlPool string
	}
	tablesByConnector := make(map[connectorKey][]string)
	for _, schema := range reg.entitySchemas {
		if schema.debeziumNatsPool == "" {
			continue
		}
		db := ctx.Engine().DB(schema.mysqlPoolCode)
		dbName := db.GetConfig().GetDatabaseName()
		key := connectorKey{natsPool: schema.debeziumNatsPool, mysqlPool: schema.mysqlPoolCode}
		tablesByConnector[key] = append(tablesByConnector[key], dbName+"."+schema.tableName)
	}

	out := make(map[string]map[string]string)
	for key, tables := range tablesByConnector {
		sort.Strings(tables)
		propsKey := key.mysqlPool
		props := make(map[string]string)

		db := ctx.Engine().DB(key.mysqlPool)
		mysqlConfig := db.GetConfig()
		host, port, user, pass := parseMySQLDSN(mysqlConfig.GetDataSourceURI())
		opts := reg.debeziumOptions[key.natsPool]
		if opts != nil {
			if opts.MySQLHost != "" {
				host = opts.MySQLHost
			}
			if opts.MySQLPort != "" {
				port = opts.MySQLPort
			}
		}

		natsURLs := ctx.Engine().Nats(key.natsPool).GetURLs()
		if opts != nil && len(opts.NatsURLs) > 0 {
			natsURLs = opts.NatsURLs
		}

		redisPool := DefaultPoolCode
		if opts != nil && opts.RedisPool != "" {
			redisPool = opts.RedisPool
		}
		redisCache := ctx.Engine().Redis(redisPool)
		if redisCache == nil {
			return nil, fmt.Errorf("debezium: redis pool '%s' not registered (used for offset/history storage)", redisPool)
		}
		redisCfg := redisCache.GetConfig()
		redisAddress := redisCfg.GetAddress()

		props["debezium.source.connector.class"] = "io.debezium.connector.mysql.MySqlConnector"
		props["debezium.source.tasks.max"] = "1"
		props["debezium.source.database.hostname"] = host
		props["debezium.source.database.port"] = port
		props["debezium.source.database.user"] = user
		props["debezium.source.database.password"] = pass
		props["debezium.source.database.server.id"] = generateServerID(key.mysqlPool)
		props["debezium.source.topic.prefix"] = "fluxa_" + key.mysqlPool
		props["debezium.source.database.include.list"] = mysqlConfig.GetDatabaseName()
		props["debezium.source.table.include.list"] = strings.Join(tables, ",")
		props["debezium.source.include.schema.changes"] = "false"

		props["debezium.source.offset.storage"] = "io.debezium.storage.redis.offset.RedisOffsetBackingStore"
		props["debezium.source.offset.storage.redis.address"] = redisAddress
		props["debezium.source.offset.storage.redis.key"] = "fluxa:dbz:" + key.mysqlPool + ":offsets"

		props["debezium.source.schema.history.internal"] = "io.debezium.storage.redis.history.RedisSchemaHistory"
		props["debezium.source.schema.history.internal.redis.address"] = redisAddress
		props["debezium.source.schema.history.internal.redis.key"] = "fluxa:dbz:" + key.mysqlPool + ":history"

		props["debezium.format.key"] = "json"
		props["debezium.format.key.schemas.enable"] = "false"
		props["debezium.format.value"] = "json"
		props["debezium.format.value.schemas.enable"] = "false"

		props["debezium.sink.type"] = "nats-jetstream"
		props["debezium.sink.nats-jetstream.url"] = strings.Join(natsURLs, ",")
		// fluxaorm owns stream creation via GetNatsAlters — keep tuning under user control.
		props["debezium.sink.nats-jetstream.create-stream"] = "false"

		out[propsKey] = props
	}
	return out, nil
}

// parseMySQLDSN parses a Go MySQL DSN like "user:pass@tcp(host:port)/dbname" into components.
func parseMySQLDSN(dsn string) (host, port, user, pass string) {
	atIdx := strings.LastIndex(dsn, "@tcp(")
	if atIdx == -1 {
		return
	}
	userPass := dsn[:atIdx]
	colonIdx := strings.Index(userPass, ":")
	if colonIdx >= 0 {
		user = userPass[:colonIdx]
		pass = userPass[colonIdx+1:]
	} else {
		user = userPass
	}

	rest := dsn[atIdx+5:]
	parenIdx := strings.Index(rest, ")")
	if parenIdx == -1 {
		return
	}
	hostPort := rest[:parenIdx]
	colonIdx = strings.LastIndex(hostPort, ":")
	if colonIdx >= 0 {
		host = hostPort[:colonIdx]
		port = hostPort[colonIdx+1:]
	} else {
		host = hostPort
		port = "3306"
	}
	return
}

// generateServerID produces a deterministic database.server.id from a pool code string.
func generateServerID(poolCode string) string {
	h := fnv.New32a()
	h.Write([]byte(poolCode))
	return fmt.Sprintf("%d", h.Sum32()%100000+1000)
}
