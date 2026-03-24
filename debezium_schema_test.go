package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type debeziumTestEntity struct {
	ID   uint64 `orm:"debezium=kafka"`
	Name string `orm:"length=100"`
	Age  uint16
}

func TestDebeziumTagRegistration(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, "kafka", nil)
	registry.RegisterDebeziumConnectURL("http://localhost:8183", "kafka")
	ctx := PrepareTables(t, registry, &debeziumTestEntity{})

	// Verify entity schema has debezium enabled
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)
	for _, schema := range reg.entitySchemas {
		if schema.tableName == "debeziumTestEntity" {
			assert.Equal(t, "kafka", schema.debeziumKafkaPool)
		}
	}

	// Verify Debezium topics are in the ignored list
	assert.True(t, reg.kafkaIgnoredTopics["kafka"]["fluxa_connect_configs"])
	assert.True(t, reg.kafkaIgnoredTopics["kafka"]["fluxa_connect_offsets"])
	assert.True(t, reg.kafkaIgnoredTopics["kafka"]["fluxa_connect_status"])
	assert.True(t, reg.kafkaIgnoredTopics["kafka"]["fluxa_default_schema_history"])
	assert.True(t, reg.kafkaIgnoredTopics["kafka"]["fluxa_default.test.debeziumTestEntity"])
}

func TestDebeziumTagInvalidKafkaPool(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)
	// No Kafka pool registered
	registry.RegisterEntity(&debeziumTestEntity{})

	_, err := registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka pool 'kafka' not found for debezium")
}

func TestGetDebeziumAltersNoDebeziumURLs(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, "kafka", nil)
	ctx := PrepareTables(t, registry, &debeziumTestEntity{})

	alters, err := GetDebeziumAlters(ctx)
	assert.NoError(t, err)
	assert.Nil(t, alters)
}

func TestDebeziumAlterDescription(t *testing.T) {
	alter := DebeziumAlter{
		Description: "CREATE debezium connector 'fluxa_default'",
		KafkaPool:   "kafka",
		execFunc:    func(ctx Context) error { return nil },
	}
	assert.Equal(t, "CREATE debezium connector 'fluxa_default'", alter.Description)
	assert.Equal(t, "kafka", alter.KafkaPool)
	assert.NoError(t, alter.Exec(nil))
}

func TestDebeziumTopicName(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterKafka([]string{"localhost:9944"}, "kafka", nil)
	registry.RegisterDebeziumConnectURL("http://localhost:8183", "kafka")
	ctx := PrepareTables(t, registry, &debeziumTestEntity{})

	topicName := DebeziumTopicName(ctx, &debeziumTestEntity{})
	assert.Equal(t, "fluxa_default.test.debeziumTestEntity", topicName)
}

func TestParseMySQLDSN(t *testing.T) {
	host, port, user, pass := parseMySQLDSN("root:root@tcp(localhost:3397)/test")
	assert.Equal(t, "localhost", host)
	assert.Equal(t, "3397", port)
	assert.Equal(t, "root", user)
	assert.Equal(t, "root", pass)

	host, port, user, pass = parseMySQLDSN("admin:secret123@tcp(db.example.com:3306)/mydb?parseTime=true")
	assert.Equal(t, "db.example.com", host)
	assert.Equal(t, "3306", port)
	assert.Equal(t, "admin", user)
	assert.Equal(t, "secret123", pass)

	host, port, user, pass = parseMySQLDSN("user@tcp(127.0.0.1:3306)/db")
	assert.Equal(t, "127.0.0.1", host)
	assert.Equal(t, "3306", port)
	assert.Equal(t, "user", user)
	assert.Equal(t, "", pass)
}

func TestDebeziumConfigsEqual(t *testing.T) {
	desired := map[string]string{
		"connector.class":   "io.debezium.connector.mysql.MySqlConnector",
		"database.hostname": "localhost",
	}
	actual := map[string]string{
		"connector.class":   "io.debezium.connector.mysql.MySqlConnector",
		"database.hostname": "localhost",
		"name":              "fluxa_default", // extra key in actual is OK
	}
	assert.True(t, debeziumConfigsEqual(desired, actual))

	actualChanged := map[string]string{
		"connector.class":   "io.debezium.connector.mysql.MySqlConnector",
		"database.hostname": "otherhost",
	}
	assert.False(t, debeziumConfigsEqual(desired, actualChanged))
}

func TestGenerateServerID(t *testing.T) {
	id1 := generateServerID("default")
	id2 := generateServerID("default")
	assert.Equal(t, id1, id2) // deterministic

	id3 := generateServerID("other")
	assert.NotEqual(t, id1, id3) // different pools get different IDs
}
