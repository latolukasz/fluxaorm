package fluxaorm

import (
	"context"
	"database/sql"
	"testing"
	"time"

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

func PrepareTablesBeta(t *testing.T, registry Registry, entities ...any) (orm Context) {
	return prepareTables(t, registry, &MySQLOptions{Beta: true}, entities...)
}

func prepareTables(t *testing.T, registry Registry, mysqlOptions *MySQLOptions, entities ...any) (orm Context) {
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, mysqlOptions)
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 1, "second", nil)
	registry.RegisterLocalCache(DefaultPoolCode, 0)

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

	for _, entity := range entities {
		schema := orm.Engine().Registry().EntitySchema(entity)
		assert.NotNil(t, schema)
		err = schema.TruncateTable(orm)
		assert.NoError(t, err)
		err = schema.UpdateSchema(orm)
		assert.NoError(t, err)
		cacheLocal, has := schema.GetLocalCache()
		if has {
			cacheLocal.Clear(orm)
		}
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
	if m.ExecMock != nil {
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
	if m.QueryRowMock != nil {
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
	if m.QueryMock != nil {
		return m.QueryContextMock(context, query, args...)
	}
	return m.OriginDB.QueryContext(context, query, args...)
}

func runAsyncConsumer(ctx Context) error {
	lazyFlashConsumer := NewLazyFlashConsumer(ctx)
	return lazyFlashConsumer.Consume(time.Millisecond)
}

func runLogTablesConsumer(ctx Context) error {
	lazyFlashConsumer := NewLogTablesConsumerSingle(ctx)
	return lazyFlashConsumer.Consume(100, time.Millisecond)
}
