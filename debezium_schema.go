package fluxaorm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strings"
)

// DebeziumOptions holds optional overrides for Debezium connector configuration.
// Useful when Debezium Connect runs in Docker and needs different addresses than the Go application.
type DebeziumOptions struct {
	// MySQLHost overrides the MySQL hostname used in connector configs.
	MySQLHost string
	// MySQLPort overrides the MySQL port used in connector configs.
	MySQLPort string
	// KafkaBrokers overrides the Kafka bootstrap servers used for schema history.
	KafkaBrokers []string
}

// DebeziumAlter holds a pending Debezium connector operation.
type DebeziumAlter struct {
	Description string
	KafkaPool   string
	execFunc    func(ctx Context) error
}

// Exec executes the Debezium connector operation.
func (a DebeziumAlter) Exec(ctx Context) error {
	return a.execFunc(ctx)
}

// GetDebeziumAlters compares registered Debezium connector definitions with actual
// Kafka Connect state and returns the operations needed to synchronize them.
func GetDebeziumAlters(ctx Context) ([]DebeziumAlter, error) {
	registry := ctx.Engine().Registry().(*engineRegistryImplementation)

	if len(registry.debeziumConnectURLs) == 0 {
		return nil, nil
	}

	type connectorKey struct {
		kafkaPool string
		mysqlPool string
	}

	// Group entities by {kafkaPool, mysqlPool}
	tablesByConnector := make(map[connectorKey][]string)
	for _, schema := range registry.entitySchemas {
		if schema.debeziumKafkaPool == "" {
			continue
		}
		key := connectorKey{kafkaPool: schema.debeziumKafkaPool, mysqlPool: schema.mysqlPoolCode}
		db := ctx.Engine().DB(schema.mysqlPoolCode)
		dbName := db.GetConfig().GetDatabaseName()
		tablesByConnector[key] = append(tablesByConnector[key], dbName+"."+schema.tableName)
	}

	if len(tablesByConnector) == 0 {
		return nil, nil
	}

	// Build desired connector configs (one per MySQL pool)
	type desiredConnector struct {
		name   string
		config map[string]string
	}
	desiredByPool := make(map[string][]desiredConnector) // kafkaPool -> connectors

	for key, tables := range tablesByConnector {
		sort.Strings(tables)
		connectorName := "fluxa_" + key.mysqlPool

		db := ctx.Engine().DB(key.mysqlPool)
		mysqlConfig := db.GetConfig()
		host, port, user, pass := parseMySQLDSN(mysqlConfig.GetDataSourceURI())

		topicPrefix := "fluxa_" + key.mysqlPool
		brokers := ctx.Engine().Kafka(key.kafkaPool).GetBrokers()

		if opts, ok := registry.debeziumOptions[key.kafkaPool]; ok && opts != nil {
			if opts.MySQLHost != "" {
				host = opts.MySQLHost
			}
			if opts.MySQLPort != "" {
				port = opts.MySQLPort
			}
			if len(opts.KafkaBrokers) > 0 {
				brokers = opts.KafkaBrokers
			}
		}

		config := map[string]string{
			"connector.class":                "io.debezium.connector.mysql.MySqlConnector",
			"database.hostname":              host,
			"database.port":                  port,
			"database.user":                  user,
			"database.password":              pass,
			"database.server.id":             generateServerID(key.mysqlPool),
			"topic.prefix":                   topicPrefix,
			"database.include.list":          mysqlConfig.GetDatabaseName(),
			"table.include.list":             strings.Join(tables, ","),
			"include.schema.changes":         "false",
			"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
			"key.converter.schemas.enable":   "false",
			"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "false",
			"schema.history.internal.kafka.bootstrap.servers": strings.Join(brokers, ","),
			"schema.history.internal.kafka.topic":             topicPrefix + "_schema_history",
		}

		desiredByPool[key.kafkaPool] = append(desiredByPool[key.kafkaPool], desiredConnector{
			name:   connectorName,
			config: config,
		})
	}

	var alters []DebeziumAlter

	// Process each Kafka pool
	for kafkaPool, desiredConnectors := range desiredByPool {
		connectURL, ok := registry.debeziumConnectURLs[kafkaPool]
		if !ok {
			return nil, fmt.Errorf("debezium connect URL not registered for kafka pool '%s'", kafkaPool)
		}

		// List existing connectors
		existing, err := listDebeziumConnectors(ctx, connectURL)
		if err != nil {
			return nil, fmt.Errorf("failed to list debezium connectors from '%s': %w", connectURL, err)
		}

		// Build desired names map
		desiredNames := make(map[string]desiredConnector)
		for _, d := range desiredConnectors {
			desiredNames[d.name] = d
		}

		// Check existing connectors
		for name, existingConfig := range existing {
			if desired, wantExists := desiredNames[name]; wantExists {
				// Compare configs
				if !debeziumConfigsEqual(desired.config, existingConfig) {
					alters = append(alters, buildUpdateConnectorAlter(connectURL, name, desired.config, kafkaPool))
				}
				delete(desiredNames, name)
			} else if strings.HasPrefix(name, "fluxa_") {
				// Delete ORM-managed connectors that are no longer needed
				alters = append(alters, buildDeleteConnectorAlter(connectURL, name, kafkaPool))
			}
		}

		// Create missing connectors
		for name, desired := range desiredNames {
			alters = append(alters, buildCreateConnectorAlter(connectURL, name, desired.config, kafkaPool))
		}
	}

	sort.Slice(alters, func(i, j int) bool {
		return alters[i].Description < alters[j].Description
	})

	return alters, nil
}

func listDebeziumConnectors(ctx Context, connectURL string) (map[string]map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx.Context(), http.MethodGet, connectURL+"/connectors?expand=info&expand=status", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	// Response format: {"connectorName": {"info": {"config": {...}}, "status": {...}}}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	result := make(map[string]map[string]string)
	for name, data := range raw {
		var info struct {
			Info struct {
				Config map[string]string `json:"config"`
			} `json:"info"`
		}
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("failed to parse connector '%s': %w", name, err)
		}
		result[name] = info.Info.Config
	}
	return result, nil
}

func buildCreateConnectorAlter(connectURL, name string, config map[string]string, kafkaPool string) DebeziumAlter {
	return DebeziumAlter{
		Description: fmt.Sprintf("CREATE debezium connector '%s'", name),
		KafkaPool:   kafkaPool,
		execFunc: func(ctx Context) error {
			payload := map[string]any{
				"name":   name,
				"config": config,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal connector config: %w", err)
			}
			req, err := http.NewRequestWithContext(ctx.Context(), http.MethodPost, connectURL+"/connectors", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to create connector '%s': %w", name, err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusCreated {
				return fmt.Errorf("failed to create connector '%s': status %d: %s", name, resp.StatusCode, string(respBody))
			}
			return nil
		},
	}
}

func buildUpdateConnectorAlter(connectURL, name string, config map[string]string, kafkaPool string) DebeziumAlter {
	return DebeziumAlter{
		Description: fmt.Sprintf("UPDATE debezium connector '%s'", name),
		KafkaPool:   kafkaPool,
		execFunc: func(ctx Context) error {
			body, err := json.Marshal(config)
			if err != nil {
				return fmt.Errorf("failed to marshal connector config: %w", err)
			}
			req, err := http.NewRequestWithContext(ctx.Context(), http.MethodPut, connectURL+"/connectors/"+name+"/config", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to update connector '%s': %w", name, err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to update connector '%s': status %d: %s", name, resp.StatusCode, string(respBody))
			}
			return nil
		},
	}
}

func buildDeleteConnectorAlter(connectURL, name string, kafkaPool string) DebeziumAlter {
	return DebeziumAlter{
		Description: fmt.Sprintf("DELETE debezium connector '%s'", name),
		KafkaPool:   kafkaPool,
		execFunc: func(ctx Context) error {
			req, err := http.NewRequestWithContext(ctx.Context(), http.MethodDelete, connectURL+"/connectors/"+name, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to delete connector '%s': %w", name, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				respBody, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("failed to delete connector '%s': status %d: %s", name, resp.StatusCode, string(respBody))
			}
			return nil
		},
	}
}

// debeziumConfigsEqual compares desired config keys against the actual connector config.
// Only keys present in desired are compared.
func debeziumConfigsEqual(desired, actual map[string]string) bool {
	for key, desiredValue := range desired {
		if actualValue, ok := actual[key]; !ok || actualValue != desiredValue {
			return false
		}
	}
	return true
}

// parseMySQLDSN parses a Go MySQL DSN like "user:pass@tcp(host:port)/dbname" into components.
func parseMySQLDSN(dsn string) (host, port, user, pass string) {
	// Format: user:pass@tcp(host:port)/dbname or user:pass@tcp(host:port)/dbname?params
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

	rest := dsn[atIdx+5:] // skip "@tcp("
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
