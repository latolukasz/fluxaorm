package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type debeziumTestEntity struct {
	ID   uint64 `orm:"debezium=nats"`
	Name string `orm:"length=100"`
	Age  uint16
}

func TestDebeziumTagRegistration(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterDebeziumServer("nats", nil)
	ctx := PrepareTables(t, registry, &debeziumTestEntity{})

	// Verify entity schema has debezium enabled.
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)
	for _, schema := range reg.entitySchemas {
		if schema.tableName == "debeziumTestEntity" {
			assert.Equal(t, "nats", schema.debeziumNatsPool)
		}
	}

	// Verify Debezium-emitted subjects are auto-ignored for the NATS reconciler.
	assert.True(t, reg.natsIgnoredSubjects["nats"]["fluxa_default.test.debeziumTestEntity"])
}

func TestDebeziumTagInvalidNatsPool(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)
	// No NATS pool registered
	registry.RegisterEntity(&debeziumTestEntity{})

	_, err := registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nats pool 'nats' not found for debezium")
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

func TestGenerateServerID(t *testing.T) {
	id1 := generateServerID("default")
	id2 := generateServerID("default")
	assert.Equal(t, id1, id2)

	id3 := generateServerID("other")
	assert.NotEqual(t, id1, id3)
}

func TestDebeziumEntitiesBuilder(t *testing.T) {
	cons := NewNatsConsumer("test_cons", "nats").
		DebeziumEntities(&debeziumTestEntity{})

	assert.Len(t, cons.debeziumEntityTypes, 1)
	assert.NoError(t, cons.validate())
}

func TestDebeziumEntitiesSubjectResolution(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterDebeziumServer("nats", nil)
	registry.RegisterNatsConsumer(
		NewNatsConsumer("debezium_cons", "nats").
			DebeziumEntities(&debeziumTestEntity{}),
	)
	ctx := PrepareTables(t, registry, &debeziumTestEntity{})

	natsPool := ctx.Engine().Nats("nats")
	cons, err := natsPool.Consumer("debezium_cons")
	// Consumer() initializes a JS connection — without a live broker this errors.
	// Just verify the registry resolved the subject into the settings.
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)
	var found bool
	for _, c := range reg.natsConsumers {
		if c.name == "debezium_cons" {
			found = true
			assert.Contains(t, c.filterSubjects, "fluxa_default.test.debeziumTestEntity")
		}
	}
	assert.True(t, found)
	// Skip broker-dependent assertion paths
	_ = cons
	_ = err
}

type nonDebeziumEntity struct {
	ID   uint64
	Name string `orm:"length=100"`
}

func TestDebeziumEntitiesEntityWithoutDebeziumTag(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterNatsConsumer(
		NewNatsConsumer("test_cons", "nats").
			DebeziumEntities(&nonDebeziumEntity{}),
	)
	registry.RegisterEntity(&nonDebeziumEntity{})
	_, err := registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not have debezium enabled")
}

type unregisteredDebeziumEntity struct {
	ID   uint64 `orm:"debezium=nats"`
	Name string `orm:"length=100"`
}

func TestDebeziumEntitiesUnregisteredEntity(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterNatsConsumer(
		NewNatsConsumer("test_cons", "nats").
			DebeziumEntities(&unregisteredDebeziumEntity{}),
	)
	// Note: entity NOT registered via RegisterEntity
	_, err := registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}
