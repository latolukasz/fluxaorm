package fluxaorm

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockLogHandler struct {
	Logs []map[string]any
}

func (h *MockLogHandler) Handle(_ Context, log map[string]any) {
	h.Logs = append(h.Logs, log)
}

func (h *MockLogHandler) Clear() {
	h.Logs = nil
}

func PrepareTables(t *testing.T, registry Registry, entities ...any) (orm Context) {
	return prepareTables(t, registry, &MySQLOptions{}, entities...)
}

func PrepareTablesWithNats(t *testing.T, registry Registry, entities ...any) (orm Context) {
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	ctx := prepareTables(t, registry, &MySQLOptions{}, entities...)
	applyNatsAlters(t, ctx)
	return ctx
}

// PrepareTablesWithCDC sets up MySQL/Redis/NATS plus auto-registers each CDC
// stream referenced by the tagged entities with default options. Production
// callers should call registry.RegisterCDCStream explicitly; this helper
// exists so tests don't have to mirror that boilerplate.
//
// The `cdcStreams` slice is the list of typed CDC stream refs the test will
// publish to / consume from. Each is registered with defaults before Validate().
func PrepareTablesWithCDC(t *testing.T, registry Registry, cdcStreams []CDCStream, entities ...any) (orm Context) {
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	for _, ref := range cdcStreams {
		registry.RegisterCDCStream(ref, CDCStreamOptions{NatsPool: "nats"})
	}
	ctx := prepareTables(t, registry, &MySQLOptions{}, entities...)
	applyNatsAlters(t, ctx)
	// Purge every CDC stream so test runs are independent.
	pool := ctx.Engine().Nats("nats")
	if pool != nil {
		if js, err := pool.GetJetStream(); err == nil {
			for _, ref := range cdcStreams {
				stream, sErr := js.Stream(ctx.Context(), dirtyStreamPrefix+string(ref.Name()))
				if sErr == nil && stream != nil {
					_ = stream.Purge(ctx.Context())
				}
			}
		}
	}
	return ctx
}

// applyNatsAlters reconciles JetStream state for tests so user-registered streams
// exist (and their durable consumers are created) before tests publish.
func applyNatsAlters(t *testing.T, ctx Context) {
	alters, err := GetNatsAlters(ctx)
	assert.NoError(t, err)
	for _, alter := range alters {
		assert.NoError(t, alter.Exec(ctx))
	}
}

func prepareTables(t *testing.T, registry Registry, mysqlOptions *MySQLOptions, entities ...any) (orm Context) {
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, mysqlOptions)
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 1, "second", nil)

	registry.RegisterEntity(entities...)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	orm = engine.NewContext(context.Background())
	cacheRedis := engine.Redis(DefaultPoolCode)
	err = cacheRedis.FlushDB(orm)
	assert.NoError(t, err)
	_ = engine.Redis("second").FlushDB(orm)

	alters, err := GetAlters(orm)
	assert.NoError(t, err)
	for _, alter := range alters {
		err = alter.Exec(orm)
		assert.NoError(t, err)
	}

	for _, schema := range orm.Engine().Registry().(*engineRegistryImplementation).entitySchemas {
		assert.NotNil(t, schema)
		err = schema.TruncateTable(orm)
		assert.NoError(t, err)
		err = schema.UpdateSchema(orm)
		assert.NoError(t, err)
	}
	return orm
}

type MockDBClient struct {
	OriginDB            DBClient
	PrepareMock         func(query string) (*sql.Stmt, error)
	ExecMock            func(query string, args ...any) (sql.Result, error)
	ExecContextMock     func(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRowMock        func(query string, args ...any) *sql.Row
	QueryRowContextMock func(context context.Context, query string, args ...any) *sql.Row
	QueryMock           func(query string, args ...any) (*sql.Rows, error)
	QueryContextMock    func(context context.Context, query string, args ...any) (*sql.Rows, error)
	BeginMock           func() (*sql.Tx, error)
	CommitMock          func() error
	RollbackMock        func() error
}

func (m *MockDBClient) Exec(query string, args ...any) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecMock(query, args...)
	}
	return m.OriginDB.Exec(query, args...)
}

func (m *MockDBClient) ExecContext(context context.Context, query string, args ...any) (sql.Result, error) {
	if m.ExecContextMock != nil {
		return m.ExecContextMock(context, query, args...)
	}
	return m.OriginDB.ExecContext(context, query, args...)
}

func (m *MockDBClient) QueryRow(query string, args ...any) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowMock(query, args...)
	}
	return m.OriginDB.QueryRow(query, args...)
}

func (m *MockDBClient) QueryRowContext(context context.Context, query string, args ...any) *sql.Row {
	if m.QueryRowContextMock != nil {
		return m.QueryRowContextMock(context, query, args...)
	}
	return m.OriginDB.QueryRowContext(context, query, args...)
}

func (m *MockDBClient) Query(query string, args ...any) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryMock(query, args...)
	}
	return m.OriginDB.Query(query, args...)
}

func (m *MockDBClient) QueryContext(context context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.QueryContextMock != nil {
		return m.QueryContextMock(context, query, args...)
	}
	return m.OriginDB.QueryContext(context, query, args...)
}

func (m *MockDBClient) Begin() (*sql.Tx, error) {
	if m.BeginMock != nil {
		return m.BeginMock()
	}
	return m.OriginDB.(DBClientNoTX).Begin()
}
