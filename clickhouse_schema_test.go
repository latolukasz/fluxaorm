package fluxaorm

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClickhouseTableBuilderCreateSQL(t *testing.T) {
	table := NewClickhouseTable("events", DefaultPoolCode).
		Column("id", "UInt64").
		Column("event_time", "DateTime").
		Column("user_id", "UInt32").
		Column("event_type", "String").
		Engine("MergeTree").
		OrderBy("id").
		PartitionBy("toYYYYMM(event_time)").
		Setting("index_granularity", "8192")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "CREATE TABLE events")
	assert.Contains(t, sql, "id UInt64")
	assert.Contains(t, sql, "event_time DateTime")
	assert.Contains(t, sql, "ENGINE = MergeTree()")
	assert.Contains(t, sql, "ORDER BY (id)")
	assert.Contains(t, sql, "PARTITION BY toYYYYMM(event_time)")
	assert.Contains(t, sql, "SETTINGS index_granularity = 8192")
}

func TestClickhouseTableBuilderColumnsWithOptions(t *testing.T) {
	table := NewClickhouseTable("test_opts", DefaultPoolCode).
		ColumnDefault("status", "UInt8", "0").
		ColumnMaterialized("year", "UInt16", "toYear(event_time)").
		ColumnAlias("full_name", "String", "concat(first_name, ' ', last_name)").
		ColumnCodec("data", "String", "ZSTD(1)").
		ColumnTTL("expires", "DateTime", "expires + INTERVAL 1 DAY").
		ColumnComment("description", "String", "Human readable description").
		Column("event_time", "DateTime").
		Column("first_name", "String").
		Column("last_name", "String").
		Engine("MergeTree").
		OrderBy("event_time")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "status UInt8 DEFAULT 0")
	assert.Contains(t, sql, "year UInt16 MATERIALIZED toYear(event_time)")
	assert.Contains(t, sql, "full_name String ALIAS concat(first_name, ' ', last_name)")
	assert.Contains(t, sql, "data String CODEC(ZSTD(1))")
	assert.Contains(t, sql, "expires DateTime TTL expires + INTERVAL 1 DAY")
	assert.Contains(t, sql, "description String COMMENT 'Human readable description'")
}

func TestClickhouseTableBuilderColumnFull(t *testing.T) {
	table := NewClickhouseTable("test_full", DefaultPoolCode).
		ColumnFull("data", "String", ClickhouseColumnOptions{
			Default: "'hello'",
			Codec:   "ZSTD(1)",
			Comment: "test column",
		}).
		Column("id", "UInt64").
		Engine("MergeTree").
		OrderBy("id")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "data String DEFAULT 'hello' CODEC(ZSTD(1)) COMMENT 'test column'")
}

func TestClickhouseTableBuilderPrimaryKey(t *testing.T) {
	table := NewClickhouseTable("test_pk", DefaultPoolCode).
		Column("id", "UInt64").
		Column("ts", "DateTime").
		Engine("MergeTree").
		OrderBy("id", "ts").
		PrimaryKey("id")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "ORDER BY (id, ts)")
	assert.Contains(t, sql, "PRIMARY KEY (id)")
}

func TestClickhouseTableBuilderTTL(t *testing.T) {
	table := NewClickhouseTable("test_ttl", DefaultPoolCode).
		Column("id", "UInt64").
		Column("ts", "DateTime").
		Engine("MergeTree").
		OrderBy("id").
		TTL("ts + INTERVAL 30 DAY")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "TTL ts + INTERVAL 30 DAY")
}

func TestClickhouseTableBuilderComment(t *testing.T) {
	table := NewClickhouseTable("test_comment", DefaultPoolCode).
		Column("id", "UInt64").
		Engine("MergeTree").
		OrderBy("id").
		Comment("This is a test table")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "COMMENT 'This is a test table'")
}

func TestClickhouseTableBuilderEngineWithParams(t *testing.T) {
	table := NewClickhouseTable("test_replacing", DefaultPoolCode).
		Column("id", "UInt64").
		Column("version", "UInt32").
		Engine("ReplacingMergeTree(version)").
		OrderBy("id")

	sql := table.createTableSQL()
	assert.Contains(t, sql, "ENGINE = ReplacingMergeTree(version)")
	// Should NOT append extra "()" since it already has parens
	assert.NotContains(t, sql, "ReplacingMergeTree(version)()")
}

func TestClickhouseTableBuilderValidation(t *testing.T) {
	// Missing table name
	table := NewClickhouseTable("", DefaultPoolCode)
	err := table.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")

	// Missing pool code
	table = NewClickhouseTable("test", "")
	err = table.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool code is required")

	// Missing columns
	table = NewClickhouseTable("test", DefaultPoolCode).
		Engine("MergeTree").
		OrderBy("id")
	err = table.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one column")

	// Missing engine
	table = NewClickhouseTable("test", DefaultPoolCode).
		Column("id", "UInt64").
		OrderBy("id")
	err = table.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have an engine")

	// Missing ORDER BY
	table = NewClickhouseTable("test", DefaultPoolCode).
		Column("id", "UInt64").
		Engine("MergeTree")
	err = table.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have ORDER BY")

	// Valid table
	table = NewClickhouseTable("test", DefaultPoolCode).
		Column("id", "UInt64").
		Engine("MergeTree").
		OrderBy("id")
	err = table.validate()
	assert.NoError(t, err)
}

func TestRegisterClickhouseTableValidation(t *testing.T) {
	// Pool not registered
	registry := NewRegistry()
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	registry.RegisterClickhouseTable(
		NewClickhouseTable("test", "nonexistent").
			Column("id", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)
	_, err := registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool 'nonexistent' not registered")

	// Duplicate table
	registry = NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	registry.RegisterClickhouseTable(
		NewClickhouseTable("test_dup", DefaultPoolCode).
			Column("id", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)
	registry.RegisterClickhouseTable(
		NewClickhouseTable("test_dup", DefaultPoolCode).
			Column("id", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)
	_, err = registry.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate clickhouse table")
}

func TestClickhouseDatabaseNameParsing(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)

	ch := engine.Clickhouse(DefaultPoolCode)
	assert.Equal(t, "default", ch.GetConfig().GetDatabaseName())

	// Test with query params
	registry2 := NewRegistry()
	registry2.RegisterClickhouse("clickhouse://localhost:9942/mydb?debug=true", "ch2", nil)
	registry2.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine2, err := registry2.Validate()
	assert.NoError(t, err)

	ch2 := engine2.Clickhouse("ch2")
	assert.Equal(t, "mydb", ch2.GetConfig().GetDatabaseName())
}

func getClickhouseTestEngine(t *testing.T, tables ...*ClickhouseTableBuilder) (Engine, Context) {
	t.Helper()
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	for _, table := range tables {
		registry.RegisterClickhouseTable(table)
	}
	engine, err := registry.Validate()
	assert.NoError(t, err)
	ctx := engine.NewContext(context.Background())
	return engine, ctx
}

func cleanupClickhouseTable(t *testing.T, ctx Context, engine Engine, tableName string) {
	t.Helper()
	ch := engine.Clickhouse(DefaultPoolCode)
	_, err := ch.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	assert.NoError(t, err)
}

func TestGetClickhouseAltersCreateTable(t *testing.T) {
	tableName := "test_ch_schema_create"
	table := NewClickhouseTable(tableName, DefaultPoolCode).
		Column("id", "UInt64").
		Column("name", "String").
		Column("ts", "DateTime").
		Engine("MergeTree").
		OrderBy("id")

	engine, ctx := getClickhouseTestEngine(t, table)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)

	// Ensure table doesn't exist
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	assert.Contains(t, alters[0].SQL, "CREATE TABLE "+tableName)
	assert.Contains(t, alters[0].SQL, "id UInt64")
	assert.Contains(t, alters[0].SQL, "name String")
	assert.Equal(t, DefaultPoolCode, alters[0].Pool)

	// Execute the alter
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Verify no more alters needed
	alters, err = GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	// Filter out non-comment alters (there may be codec-related alters from system defaults)
	var realAlters []ClickhouseAlter
	for _, a := range alters {
		if !strings.HasPrefix(a.SQL, "--") {
			realAlters = append(realAlters, a)
		}
	}
	assert.Len(t, realAlters, 0)
}

func TestGetClickhouseAltersAddColumn(t *testing.T) {
	tableName := "test_ch_schema_addcol"

	// First create the table with initial columns
	engine, ctx := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("name", "String").
			Engine("MergeTree").
			OrderBy("id"),
	)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Now register with an extra column
	engine2, ctx2 := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("name", "String").
			Column("age", "UInt32").
			Engine("MergeTree").
			OrderBy("id"),
	)

	alters, err = GetClickhouseAlters(ctx2)
	assert.NoError(t, err)

	hasAddColumn := false
	for _, a := range alters {
		if strings.Contains(a.SQL, "ADD COLUMN age UInt32") {
			hasAddColumn = true
			err = a.Exec(ctx2)
			assert.NoError(t, err)
		}
	}
	assert.True(t, hasAddColumn, "expected ADD COLUMN alter")
	_ = engine2
}

func TestGetClickhouseAltersDropColumn(t *testing.T) {
	tableName := "test_ch_schema_dropcol"

	// Create table with extra column
	engine, ctx := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("name", "String").
			Column("extra", "String").
			Engine("MergeTree").
			OrderBy("id"),
	)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Now register without the extra column
	engine2, ctx2 := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("name", "String").
			Engine("MergeTree").
			OrderBy("id"),
	)

	alters, err = GetClickhouseAlters(ctx2)
	assert.NoError(t, err)

	hasDropColumn := false
	for _, a := range alters {
		if strings.Contains(a.SQL, "DROP COLUMN extra") {
			hasDropColumn = true
			err = a.Exec(ctx2)
			assert.NoError(t, err)
		}
	}
	assert.True(t, hasDropColumn, "expected DROP COLUMN alter")
	_ = engine2
}

func TestGetClickhouseAltersModifyColumn(t *testing.T) {
	tableName := "test_ch_schema_modcol"

	// Create table with UInt32 column
	engine, ctx := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("count", "UInt32").
			Engine("MergeTree").
			OrderBy("id"),
	)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Now register with UInt64 column type
	engine2, ctx2 := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("count", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)

	alters, err = GetClickhouseAlters(ctx2)
	assert.NoError(t, err)

	hasModifyColumn := false
	for _, a := range alters {
		if strings.Contains(a.SQL, "MODIFY COLUMN count UInt64") {
			hasModifyColumn = true
			err = a.Exec(ctx2)
			assert.NoError(t, err)
		}
	}
	assert.True(t, hasModifyColumn, "expected MODIFY COLUMN alter")
	_ = engine2
}

func TestGetClickhouseAltersDropTable(t *testing.T) {
	tableName := "test_ch_schema_droptbl"

	// Create a table directly
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	ctx := engine.NewContext(context.Background())

	ch := engine.Clickhouse(DefaultPoolCode)
	_, err = ch.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	assert.NoError(t, err)
	_, err = ch.Exec(ctx, "CREATE TABLE "+tableName+" (id UInt64) ENGINE = MergeTree() ORDER BY id")
	assert.NoError(t, err)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)

	// Register a different table so the existing one is unregistered
	otherTable := "test_ch_schema_other"
	engine2, ctx2 := getClickhouseTestEngine(t,
		NewClickhouseTable(otherTable, DefaultPoolCode).
			Column("id", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)
	defer cleanupClickhouseTable(t, ctx2, engine2, otherTable)

	alters, err := GetClickhouseAlters(ctx2)
	assert.NoError(t, err)

	hasDropTable := false
	for _, a := range alters {
		if strings.Contains(a.SQL, "DROP TABLE IF EXISTS "+tableName) {
			hasDropTable = true
		}
	}
	assert.True(t, hasDropTable, "expected DROP TABLE alter for unregistered table")
}

func TestGetClickhouseAltersIgnoredTables(t *testing.T) {
	tableName := "test_ch_schema_ignored"

	// Create a table directly
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode,
		&ClickhouseOptions{IgnoredTables: []string{tableName}})
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)

	otherTable := "test_ch_schema_kept"
	registry.RegisterClickhouseTable(
		NewClickhouseTable(otherTable, DefaultPoolCode).
			Column("id", "UInt64").
			Engine("MergeTree").
			OrderBy("id"),
	)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	ctx := engine.NewContext(context.Background())

	ch := engine.Clickhouse(DefaultPoolCode)
	_, err = ch.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	assert.NoError(t, err)
	_, err = ch.Exec(ctx, "CREATE TABLE "+tableName+" (id UInt64) ENGINE = MergeTree() ORDER BY id")
	assert.NoError(t, err)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	defer cleanupClickhouseTable(t, ctx, engine, otherTable)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)

	for _, a := range alters {
		assert.NotContains(t, a.SQL, "DROP TABLE IF EXISTS "+tableName,
			"ignored table should not be dropped")
	}
}

func TestGetClickhouseAltersNoTablesRegistered(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterClickhouse("clickhouse://localhost:9942/default", DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 15, "redis", nil)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	ctx := engine.NewContext(context.Background())

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Nil(t, alters)
}

func TestGetClickhouseAltersOrderByMismatch(t *testing.T) {
	tableName := "test_ch_schema_orderby"

	// Create table with ORDER BY id
	engine, ctx := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("ts", "DateTime").
			Engine("MergeTree").
			OrderBy("id"),
	)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Now register with different ORDER BY
	engine2, ctx2 := getClickhouseTestEngine(t,
		NewClickhouseTable(tableName, DefaultPoolCode).
			Column("id", "UInt64").
			Column("ts", "DateTime").
			Engine("MergeTree").
			OrderBy("id", "ts"),
	)

	alters, err = GetClickhouseAlters(ctx2)
	assert.NoError(t, err)

	hasOrderByWarning := false
	for _, a := range alters {
		if strings.Contains(a.SQL, "ORDER BY mismatch") {
			hasOrderByWarning = true
		}
	}
	assert.True(t, hasOrderByWarning, "expected ORDER BY mismatch warning")
	_ = engine2
}

func TestClickhouseAlterExec(t *testing.T) {
	tableName := "test_ch_schema_exec"
	engine, ctx := getClickhouseTestEngine(t)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	alter := ClickhouseAlter{
		SQL:  "CREATE TABLE " + tableName + " (id UInt64) ENGINE = MergeTree() ORDER BY id",
		Pool: DefaultPoolCode,
	}
	err := alter.Exec(ctx)
	assert.NoError(t, err)

	// Verify it was created
	ch := engine.Clickhouse(DefaultPoolCode)
	var count uint64
	found, err := ch.QueryRow(ctx, NewWhere("SELECT count() FROM "+tableName), &count)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(0), count)
}

func TestGetClickhouseAltersNoChanges(t *testing.T) {
	tableName := "test_ch_schema_nochange"
	table := NewClickhouseTable(tableName, DefaultPoolCode).
		Column("id", "UInt64").
		Column("name", "String").
		Engine("MergeTree").
		OrderBy("id")

	engine, ctx := getClickhouseTestEngine(t, table)
	defer cleanupClickhouseTable(t, ctx, engine, tableName)
	cleanupClickhouseTable(t, ctx, engine, tableName)

	// Create the table
	alters, err := GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	err = alters[0].Exec(ctx)
	assert.NoError(t, err)

	// Check again - should have no real alters
	alters, err = GetClickhouseAlters(ctx)
	assert.NoError(t, err)
	var realAlters []ClickhouseAlter
	for _, a := range alters {
		if !strings.HasPrefix(a.SQL, "--") {
			realAlters = append(realAlters, a)
		}
	}
	assert.Len(t, realAlters, 0)
}
