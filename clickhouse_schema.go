package fluxaorm

import (
	"fmt"
	"sort"
	"strings"
)

// ClickhouseColumnOptions holds optional settings for a ClickHouse column.
type ClickhouseColumnOptions struct {
	Default      string // DEFAULT expression
	Materialized string // MATERIALIZED expression
	Alias        string // ALIAS expression
	Codec        string // e.g. "ZSTD(1)"
	TTL          string // column-level TTL expression
	Comment      string
}

type clickhouseColumnDef struct {
	name        string
	columnType  string
	defaultKind string // "", "DEFAULT", "MATERIALIZED", "ALIAS"
	defaultExpr string
	codec       string
	ttl         string
	comment     string
}

// ClickhouseTableBuilder defines a ClickHouse table schema using a fluent API.
type ClickhouseTableBuilder struct {
	tableName   string
	poolCode    string
	columns     []clickhouseColumnDef
	engine      string
	orderBy     []string
	partitionBy string
	primaryKey  []string
	ttl         string
	settings    [][2]string
	comment     string
}

// NewClickhouseTable creates a new ClickHouse table builder.
func NewClickhouseTable(tableName, poolCode string) *ClickhouseTableBuilder {
	return &ClickhouseTableBuilder{
		tableName: tableName,
		poolCode:  poolCode,
	}
}

// Column adds a simple column.
func (b *ClickhouseTableBuilder) Column(name, typeName string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName})
	return b
}

// ColumnDefault adds a column with a DEFAULT expression.
func (b *ClickhouseTableBuilder) ColumnDefault(name, typeName, defaultExpr string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, defaultKind: "DEFAULT", defaultExpr: defaultExpr})
	return b
}

// ColumnMaterialized adds a column with a MATERIALIZED expression.
func (b *ClickhouseTableBuilder) ColumnMaterialized(name, typeName, expr string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, defaultKind: "MATERIALIZED", defaultExpr: expr})
	return b
}

// ColumnAlias adds a column with an ALIAS expression.
func (b *ClickhouseTableBuilder) ColumnAlias(name, typeName, expr string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, defaultKind: "ALIAS", defaultExpr: expr})
	return b
}

// ColumnCodec adds a column with a compression CODEC.
func (b *ClickhouseTableBuilder) ColumnCodec(name, typeName, codec string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, codec: codec})
	return b
}

// ColumnTTL adds a column with a column-level TTL expression.
func (b *ClickhouseTableBuilder) ColumnTTL(name, typeName, ttl string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, ttl: ttl})
	return b
}

// ColumnComment adds a column with a comment.
func (b *ClickhouseTableBuilder) ColumnComment(name, typeName, comment string) *ClickhouseTableBuilder {
	b.columns = append(b.columns, clickhouseColumnDef{name: name, columnType: typeName, comment: comment})
	return b
}

// ColumnFull adds a column with all possible options.
func (b *ClickhouseTableBuilder) ColumnFull(name, typeName string, opts ClickhouseColumnOptions) *ClickhouseTableBuilder {
	col := clickhouseColumnDef{name: name, columnType: typeName, codec: opts.Codec, ttl: opts.TTL, comment: opts.Comment}
	if opts.Materialized != "" {
		col.defaultKind = "MATERIALIZED"
		col.defaultExpr = opts.Materialized
	} else if opts.Alias != "" {
		col.defaultKind = "ALIAS"
		col.defaultExpr = opts.Alias
	} else if opts.Default != "" {
		col.defaultKind = "DEFAULT"
		col.defaultExpr = opts.Default
	}
	b.columns = append(b.columns, col)
	return b
}

// Engine sets the table engine (e.g. "MergeTree", "ReplacingMergeTree(Version)").
func (b *ClickhouseTableBuilder) Engine(engine string) *ClickhouseTableBuilder {
	b.engine = engine
	return b
}

// OrderBy sets the ORDER BY columns.
func (b *ClickhouseTableBuilder) OrderBy(columns ...string) *ClickhouseTableBuilder {
	b.orderBy = columns
	return b
}

// PartitionBy sets the PARTITION BY expression.
func (b *ClickhouseTableBuilder) PartitionBy(expr string) *ClickhouseTableBuilder {
	b.partitionBy = expr
	return b
}

// PrimaryKey sets the PRIMARY KEY columns. If not set, defaults to ORDER BY.
func (b *ClickhouseTableBuilder) PrimaryKey(columns ...string) *ClickhouseTableBuilder {
	b.primaryKey = columns
	return b
}

// TTL sets the table-level TTL expression.
func (b *ClickhouseTableBuilder) TTL(expr string) *ClickhouseTableBuilder {
	b.ttl = expr
	return b
}

// Setting adds a table-level SETTINGS key=value pair.
func (b *ClickhouseTableBuilder) Setting(key, value string) *ClickhouseTableBuilder {
	b.settings = append(b.settings, [2]string{key, value})
	return b
}

// Comment sets the table comment.
func (b *ClickhouseTableBuilder) Comment(comment string) *ClickhouseTableBuilder {
	b.comment = comment
	return b
}

func (c *clickhouseColumnDef) buildColumnSQL() string {
	s := c.name + " " + c.columnType
	if c.defaultKind != "" && c.defaultExpr != "" {
		s += " " + c.defaultKind + " " + c.defaultExpr
	}
	if c.codec != "" {
		s += " CODEC(" + c.codec + ")"
	}
	if c.ttl != "" {
		s += " TTL " + c.ttl
	}
	if c.comment != "" {
		s += " COMMENT '" + strings.ReplaceAll(c.comment, "'", "\\'") + "'"
	}
	return s
}

func (b *ClickhouseTableBuilder) createTableSQL() string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(b.tableName)
	sb.WriteString(" (\n")
	for i, col := range b.columns {
		sb.WriteString("  ")
		sb.WriteString(col.buildColumnSQL())
		if i < len(b.columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString(") ENGINE = ")
	engine := b.engine
	if !strings.Contains(engine, "(") {
		engine += "()"
	}
	sb.WriteString(engine)
	sb.WriteString("\n")
	if len(b.orderBy) > 0 {
		sb.WriteString("ORDER BY (")
		sb.WriteString(strings.Join(b.orderBy, ", "))
		sb.WriteString(")\n")
	}
	if b.partitionBy != "" {
		sb.WriteString("PARTITION BY ")
		sb.WriteString(b.partitionBy)
		sb.WriteString("\n")
	}
	if len(b.primaryKey) > 0 {
		sb.WriteString("PRIMARY KEY (")
		sb.WriteString(strings.Join(b.primaryKey, ", "))
		sb.WriteString(")\n")
	}
	if b.ttl != "" {
		sb.WriteString("TTL ")
		sb.WriteString(b.ttl)
		sb.WriteString("\n")
	}
	if len(b.settings) > 0 {
		sb.WriteString("SETTINGS ")
		for i, s := range b.settings {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s[0])
			sb.WriteString(" = ")
			sb.WriteString(s[1])
		}
		sb.WriteString("\n")
	}
	if b.comment != "" {
		sb.WriteString("COMMENT '")
		sb.WriteString(strings.ReplaceAll(b.comment, "'", "\\'"))
		sb.WriteString("'\n")
	}
	return strings.TrimRight(sb.String(), "\n") + ";"
}

func (b *ClickhouseTableBuilder) validate() error {
	if b.tableName == "" {
		return fmt.Errorf("clickhouse table name is required")
	}
	if b.poolCode == "" {
		return fmt.Errorf("clickhouse pool code is required for table '%s'", b.tableName)
	}
	if len(b.columns) == 0 {
		return fmt.Errorf("clickhouse table '%s' must have at least one column", b.tableName)
	}
	if b.engine == "" {
		return fmt.Errorf("clickhouse table '%s' must have an engine", b.tableName)
	}
	if len(b.orderBy) == 0 {
		return fmt.Errorf("clickhouse table '%s' must have ORDER BY", b.tableName)
	}
	return nil
}

// ClickhouseAlter holds a pending ClickHouse DDL operation.
type ClickhouseAlter struct {
	SQL  string
	Pool string
}

// Exec executes the ClickHouse DDL statement.
func (a ClickhouseAlter) Exec(ctx Context) error {
	_, err := ctx.Engine().Clickhouse(a.Pool).Exec(ctx, a.SQL)
	return err
}

// GetClickhouseAlters compares registered ClickHouse table definitions with actual
// database schemas and returns the DDL statements needed to synchronize them.
func GetClickhouseAlters(ctx Context) ([]ClickhouseAlter, error) {
	registry := ctx.Engine().Registry().(*engineRegistryImplementation)
	if len(registry.clickhouseTables) == 0 {
		return nil, nil
	}

	// Group registered tables by pool
	tablesByPool := make(map[string][]*ClickhouseTableBuilder)
	for _, t := range registry.clickhouseTables {
		tablesByPool[t.poolCode] = append(tablesByPool[t.poolCode], t)
	}

	var alters []ClickhouseAlter

	for poolCode, tables := range tablesByPool {
		ch := ctx.Engine().Clickhouse(poolCode)
		if ch == nil {
			return nil, fmt.Errorf("clickhouse pool '%s' not found", poolCode)
		}

		// Get existing tables
		existingTables := make(map[string]bool)
		rows, closeRows, err := ch.Query(ctx, "SELECT name FROM system.tables WHERE database = currentDatabase() AND engine != 'View'")
		if err != nil {
			return nil, fmt.Errorf("failed to query system.tables for pool '%s': %w", poolCode, err)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				closeRows()
				return nil, err
			}
			existingTables[name] = true
		}
		closeRows()

		registeredNames := make(map[string]bool)
		for _, table := range tables {
			registeredNames[table.tableName] = true

			if !existingTables[table.tableName] {
				// CREATE TABLE
				alters = append(alters, ClickhouseAlter{
					SQL:  table.createTableSQL(),
					Pool: poolCode,
				})
				continue
			}

			// Compare columns
			columnAlters, err := compareClickhouseColumns(ctx, ch, table)
			if err != nil {
				return nil, err
			}
			alters = append(alters, columnAlters...)

			// Compare table properties
			propAlters, err := compareClickhouseTableProperties(ctx, ch, table)
			if err != nil {
				return nil, err
			}
			alters = append(alters, propAlters...)
		}

		// DROP unregistered tables
		ignoredTables := registry.clickhouseIgnoredTables[poolCode]
		for tableName := range existingTables {
			if registeredNames[tableName] {
				continue
			}
			if ignoredTables != nil && ignoredTables[tableName] {
				continue
			}
			alters = append(alters, ClickhouseAlter{
				SQL:  fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName),
				Pool: poolCode,
			})
		}
	}

	sort.Slice(alters, func(i, j int) bool {
		return alters[i].SQL < alters[j].SQL
	})

	return alters, nil
}

type clickhouseExistingColumn struct {
	name             string
	columnType       string
	defaultKind      string
	defaultExpr      string
	comment          string
	compressionCodec string
}

func compareClickhouseColumns(ctx Context, ch Clickhouse, table *ClickhouseTableBuilder) ([]ClickhouseAlter, error) {
	rows, closeRows, err := ch.Query(ctx,
		"SELECT name, type, default_kind, default_expression, comment, compression_codec "+
			"FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table.tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query system.columns for table '%s': %w", table.tableName, err)
	}

	var existingCols []clickhouseExistingColumn
	existingByName := make(map[string]clickhouseExistingColumn)
	for rows.Next() {
		var col clickhouseExistingColumn
		if err := rows.Scan(&col.name, &col.columnType, &col.defaultKind, &col.defaultExpr, &col.comment, &col.compressionCodec); err != nil {
			closeRows()
			return nil, err
		}
		existingCols = append(existingCols, col)
		existingByName[col.name] = col
	}
	closeRows()

	var alters []ClickhouseAlter

	// Check for new or modified columns
	definedNames := make(map[string]bool)
	for _, col := range table.columns {
		definedNames[col.name] = true
		existing, exists := existingByName[col.name]
		if !exists {
			// ADD COLUMN
			alters = append(alters, ClickhouseAlter{
				SQL:  fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", table.tableName, col.buildColumnSQL()),
				Pool: table.poolCode,
			})
			continue
		}

		// Compare column properties
		if columnNeedsModify(col, existing) {
			alters = append(alters, ClickhouseAlter{
				SQL:  fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", table.tableName, col.buildColumnSQL()),
				Pool: table.poolCode,
			})
		}
	}

	// Check for columns to drop
	for _, existing := range existingCols {
		if !definedNames[existing.name] {
			alters = append(alters, ClickhouseAlter{
				SQL:  fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", table.tableName, existing.name),
				Pool: table.poolCode,
			})
		}
	}

	return alters, nil
}

func columnNeedsModify(defined clickhouseColumnDef, existing clickhouseExistingColumn) bool {
	if defined.columnType != existing.columnType {
		return true
	}
	if !strings.EqualFold(defined.defaultKind, existing.defaultKind) {
		return true
	}
	if defined.defaultExpr != existing.defaultExpr {
		return true
	}
	if defined.comment != existing.comment {
		return true
	}
	// Compare codec - existing codec comes from system.columns as full codec string
	if defined.codec != "" {
		expectedCodec := "CODEC(" + defined.codec + ")"
		if existing.compressionCodec != expectedCodec {
			return true
		}
	} else if existing.compressionCodec != "" {
		return true
	}
	return false
}

func compareClickhouseTableProperties(ctx Context, ch Clickhouse, table *ClickhouseTableBuilder) ([]ClickhouseAlter, error) {
	rows, closeRows, err := ch.Query(ctx,
		"SELECT engine, sorting_key, partition_key, primary_key, comment "+
			"FROM system.tables WHERE database = currentDatabase() AND name = ?", table.tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query system.tables for table '%s': %w", table.tableName, err)
	}

	var engine, sortingKey, partitionKey, primaryKey, comment string
	hasRow := false
	for rows.Next() {
		if err := rows.Scan(&engine, &sortingKey, &partitionKey, &primaryKey, &comment); err != nil {
			closeRows()
			return nil, err
		}
		hasRow = true
		break
	}
	closeRows()
	if !hasRow {
		return nil, nil
	}

	var alters []ClickhouseAlter

	// ENGINE, ORDER BY, PARTITION BY cannot be altered - emit warnings
	expectedOrderBy := strings.Join(table.orderBy, ", ")
	if sortingKey != expectedOrderBy {
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("-- TABLE %s: ORDER BY mismatch. Current: (%s), Expected: (%s). Manual recreation required.", table.tableName, sortingKey, expectedOrderBy),
			Pool: table.poolCode,
		})
	}

	// Check engine - compare just the engine name (before parenthesis)
	expectedEngine := table.engine
	if !strings.Contains(expectedEngine, "(") {
		expectedEngine += "()"
	}
	currentEngineName := engine
	expectedEngineName := strings.Split(expectedEngine, "(")[0]
	if currentEngineName != expectedEngineName {
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("-- TABLE %s: ENGINE mismatch. Current: %s, Expected: %s. Manual recreation required.", table.tableName, engine, expectedEngineName),
			Pool: table.poolCode,
		})
	}

	if table.partitionBy != "" && partitionKey != table.partitionBy {
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("-- TABLE %s: PARTITION BY mismatch. Current: %s, Expected: %s. Manual recreation required.", table.tableName, partitionKey, table.partitionBy),
			Pool: table.poolCode,
		})
	}

	// TTL can be altered
	if table.ttl != "" {
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s;", table.tableName, table.ttl),
			Pool: table.poolCode,
		})
	}

	// SETTINGS can be altered
	if len(table.settings) > 0 {
		var parts []string
		for _, s := range table.settings {
			parts = append(parts, s[0]+" = "+s[1])
		}
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("ALTER TABLE %s MODIFY SETTING %s;", table.tableName, strings.Join(parts, ", ")),
			Pool: table.poolCode,
		})
	}

	// COMMENT can be altered
	if table.comment != "" && comment != table.comment {
		alters = append(alters, ClickhouseAlter{
			SQL:  fmt.Sprintf("ALTER TABLE %s MODIFY COMMENT '%s';", table.tableName, strings.ReplaceAll(table.comment, "'", "\\'")),
			Pool: table.poolCode,
		})
	}

	return alters, nil
}
