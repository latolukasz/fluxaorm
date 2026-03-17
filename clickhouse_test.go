package fluxaorm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClickhouse(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	ctx := engine.NewContext(context.Background())

	ch := engine.Clickhouse(DefaultPoolCode)
	assert.NotNil(t, ch)

	config := ch.GetConfig()
	assert.Equal(t, DefaultPoolCode, config.GetCode())
	assert.Equal(t, "clickhouse://localhost:9942/default", config.GetDataSourceURI())

	// Test ClickhousePools
	pools := engine.Registry().ClickhousePools()
	assert.Len(t, pools, 1)
	assert.NotNil(t, pools[DefaultPoolCode])

	// Test Exec - create test table
	_, err = ch.Exec(ctx, "DROP TABLE IF EXISTS test_clickhouse")
	assert.NoError(t, err)

	_, err = ch.Exec(ctx, "CREATE TABLE test_clickhouse (id UInt64, name String) ENGINE = MergeTree() ORDER BY id")
	assert.NoError(t, err)

	// Test Exec - insert data
	_, err = ch.Exec(ctx, "INSERT INTO test_clickhouse (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	assert.NoError(t, err)

	// Test QueryRow
	var name string
	found, err := ch.QueryRow(ctx, NewWhere("SELECT name FROM test_clickhouse WHERE id = 1"), &name)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "Alice", name)

	// Test QueryRow - not found
	found, err = ch.QueryRow(ctx, NewWhere("SELECT name FROM test_clickhouse WHERE id = 999"), &name)
	assert.NoError(t, err)
	assert.False(t, found)

	// Test Query
	rows, close, err := ch.Query(ctx, "SELECT id, name FROM test_clickhouse ORDER BY id")
	assert.NoError(t, err)
	defer close()

	var ids []uint64
	var names []string
	for rows.Next() {
		var id uint64
		var n string
		err = rows.Scan(&id, &n)
		assert.NoError(t, err)
		ids = append(ids, id)
		names = append(names, n)
	}
	assert.Equal(t, []uint64{1, 2, 3}, ids)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, names)

	// Test Columns
	rows2, close2, err := ch.Query(ctx, "SELECT id, name FROM test_clickhouse LIMIT 1")
	assert.NoError(t, err)
	defer close2()
	cols, err := rows2.Columns()
	assert.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, cols)

	// Test debug logging
	testLogger := &MockLogHandler{}
	ctx.RegisterQueryLogger(testLogger, QueryLoggerOptions{Clickhouse: true})

	_, err = ch.Exec(ctx, "SELECT 1")
	assert.NoError(t, err)
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "clickhouse", testLogger.Logs[0]["source"])
	assert.Equal(t, DefaultPoolCode, testLogger.Logs[0]["pool"])
	assert.Equal(t, "EXEC", testLogger.Logs[0]["operation"])
	assert.Equal(t, "SELECT 1", testLogger.Logs[0]["query"])

	testLogger.Clear()

	found, err = ch.QueryRow(ctx, NewWhere("SELECT name FROM test_clickhouse WHERE id = 1"), &name)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "clickhouse", testLogger.Logs[0]["source"])
	assert.Equal(t, "SELECT", testLogger.Logs[0]["operation"])

	testLogger.Clear()

	rows3, close3, err := ch.Query(ctx, "SELECT id FROM test_clickhouse ORDER BY id")
	assert.NoError(t, err)
	defer close3()
	for rows3.Next() {
		var id uint64
		_ = rows3.Scan(&id)
	}
	assert.Len(t, testLogger.Logs, 1)
	assert.Equal(t, "clickhouse", testLogger.Logs[0]["source"])
	assert.Equal(t, "SELECT", testLogger.Logs[0]["operation"])

	// Cleanup
	_, _ = ch.Exec(ctx, "DROP TABLE IF EXISTS test_clickhouse")
}

func TestClickhouseGetDBClient(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	ch := engine.Clickhouse(DefaultPoolCode)
	client := ch.GetDBClient()
	assert.NotNil(t, client)
}

func TestClickhouseOptions(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, &ClickhouseOptions{
		MaxOpenConnections: 10,
		MaxIdleConnections: 5,
	})
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	ch := engine.Clickhouse(DefaultPoolCode)
	assert.NotNil(t, ch)
	opts := ch.GetConfig().GetOptions()
	assert.Equal(t, 10, opts.MaxOpenConnections)
	assert.Equal(t, 5, opts.MaxIdleConnections)
}
