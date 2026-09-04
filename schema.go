package fluxaorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type Alter struct {
	SQL    string
	Pool   string
	Kind   AlterKind
	Table  string
	Entity string // empty for a table with no registered entity
	Safety AlterSafety
	Reason string
}

type tableSQLSchemaDefinition struct {
	ctx            Context
	EntitySchema   *entitySchema
	EntityColumns  []*ColumnSchemaDefinition
	EntityIndexes  []*IndexSchemaDefinition
	DBTableColumns []*ColumnSchemaDefinition
	DBIndexes      []*IndexSchemaDefinition
	DBCreateSchema string
	DBEncoding     string
	Engine         string
}

func GetAlters(ctx Context) (alters []Alter, err error) {
	return getAlters(ctx)
}

func (td *tableSQLSchemaDefinition) CreateTableSQL() string {
	pool := td.EntitySchema.GetDB()
	createTableSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", pool.GetConfig().GetDatabaseName(), td.EntitySchema.GetTableName())
	for _, value := range td.EntityColumns {
		createTableSQL += fmt.Sprintf("  %s,\n", value.Definition)
	}
	var indexDefinitions []string
	for _, indexEntity := range td.EntityIndexes {
		if !indexEntity.Duplicated {
			indexDefinitions = append(indexDefinitions, buildCreateIndexSQL(indexEntity))
		}
	}
	sort.Strings(indexDefinitions)
	for _, value := range indexDefinitions {
		createTableSQL += fmt.Sprintf("  %s,\n", value[4:])
	}

	createTableSQL += " PRIMARY KEY (`ID`)\n"
	collate := " COLLATE=" + pool.GetConfig().GetOptions().DefaultEncoding + "_" +
		pool.GetConfig().GetOptions().DefaultCollate
	engine := "InnoDB"
	createTableSQL += fmt.Sprintf(") ENGINE=%s DEFAULT CHARSET=%s%s", engine, pool.GetConfig().GetOptions().DefaultEncoding, collate)
	createTableSQL += ";"
	return createTableSQL
}

type IndexSchemaDefinition struct {
	Name       string
	Unique     bool
	Duplicated bool
	columnsMap map[int]string
}

type indexDB struct {
	Skip      sql.NullString
	NonUnique uint8
	KeyName   string
	Seq       int
	Column    string
}

func (ti *IndexSchemaDefinition) GetColumns() []string {
	columns := make([]string, len(ti.columnsMap))
	for i := 1; i <= len(columns); i++ {
		columns[i-1] = ti.columnsMap[i]
	}
	return columns
}

func (ti *IndexSchemaDefinition) SetColumns(columns []string) {
	ti.columnsMap = make(map[int]string)
	for i, column := range columns {
		ti.columnsMap[i+1] = column
	}
}

func (a Alter) Exec(ctx Context) error {
	_, err := ctx.Engine().DB(a.Pool).Exec(ctx, a.SQL)
	return err
}

func getAlters(ctx Context) (alters []Alter, err error) {
	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	for poolName, pool := range ctx.Engine().Registry().DBPools() {
		tablesInDB[poolName] = make(map[string]bool)
		tablesInEntities[poolName] = make(map[string]bool)
		tables, err := getAllTables(pool.GetDBClient())
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			tablesInDB[poolName][table] = true
		}
	}
	alters = make([]Alter, 0)
	for _, schema := range ctx.Engine().Registry().(*engineRegistryImplementation).entitySchemas {
		db := schema.GetDB()
		tablesInEntities[db.GetConfig().GetCode()][schema.GetTableName()] = true
		schemaAlters, err := getSchemaChanges(ctx, schema)
		if err != nil {
			return nil, err
		}
		alters = append(alters, schemaAlters...)
	}
	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				_, has = ctx.Engine().Registry().getDBTables()[poolName][tableName]
				if !has {
					pool := ctx.Engine().DB(poolName)
					dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetConfig().GetDatabaseName(), tableName)
					alters = append(alters, Alter{
						SQL:    dropSQL,
						Pool:   poolName,
						Kind:   AlterKindDropTable,
						Table:  tableName,
						Safety: AlterDestructive,
						Reason: "table has no registered entity",
					})
				}
			}
		}
	}
	sortAlters(alters)

	return alters, nil
}

func getAllTables(db DBClient) ([]string, error) {
	tables := make([]string, 0)
	results, err := db.Query("SHOW FULL TABLES WHERE Table_Type = 'BASE TABLE'")
	if err != nil {
		return tables, err
	}
	defer func() {
		_ = results.Close()
	}()
	var skip string
	for results.Next() {
		var row string
		err = results.Scan(&row, &skip)
		if err != nil {
			return tables, err
		}
		tables = append(tables, row)
	}
	err = results.Err()
	return tables, err
}

func getSchemaChanges(ctx Context, entitySchema *entitySchema) (alters []Alter, err error) {
	indexes := make(map[string]*IndexSchemaDefinition)
	columns, err := checkStruct(ctx.Engine(), entitySchema, entitySchema.GetType(), indexes, nil, "", -1)
	if err != nil {
		return nil, err
	}

	if entitySchema.hasFakeDelete {
		hasFakeDeleteIndex := false
		for _, indexDef := range indexes {
			if indexDef.columnsMap[1] == "FakeDelete" {
				hasFakeDeleteIndex = true
			}
			hasAnyFakeDeleteColumn := false
			for _, column := range indexDef.columnsMap {
				if column == "FakeDelete" {
					hasAnyFakeDeleteColumn = true
					break
				}
			}
			if !hasAnyFakeDeleteColumn {
				indexDef.columnsMap[len(indexDef.columnsMap)+1] = "FakeDelete"
			}
		}
		if !hasFakeDeleteIndex {
			indexes["FakeDelete"] = &IndexSchemaDefinition{
				Name:       "FakeDelete",
				columnsMap: map[int]string{1: "FakeDelete"},
			}
		}
	}
	indexesSlice := make([]*IndexSchemaDefinition, 0)
	for _, index := range indexes {
		indexesSlice = append(indexesSlice, index)
	}
	pool := entitySchema.GetDB()
	var skip string
	hasTable, err := pool.QueryRow(ctx, NewWhere(fmt.Sprintf("SHOW TABLES LIKE '%s'", entitySchema.GetTableName())), &skip)
	if err != nil {
		return nil, err
	}
	sqlSchema := &tableSQLSchemaDefinition{
		ctx:           ctx,
		EntitySchema:  entitySchema,
		EntityIndexes: indexesSlice,
		DBEncoding:    pool.GetConfig().GetOptions().DefaultEncoding,
		EntityColumns: columns}

	if !hasTable {
		return []Alter{newAlter(entitySchema, AlterKindCreateTable, AlterSafe, "", sqlSchema.CreateTableSQL())}, nil
	}

	sqlSchema.DBTableColumns = make([]*ColumnSchemaDefinition, 0)
	_, err = pool.QueryRow(ctx, NewWhere(fmt.Sprintf("SHOW CREATE TABLE `%s`", entitySchema.GetTableName())), &skip, &sqlSchema.DBCreateSchema)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(sqlSchema.DBCreateSchema, "\n")
	for x := 1; x < len(lines); x++ {
		l := strings.Trim(lines[x], " ")
		if strings.HasPrefix(l, "CONSTRAINT ") {
			// Reports drift rather than converging: nothing re-adds the constraint, so it returns
			// on every call. Dormant while no table carries a foreign key.
			parts := strings.Split(l, " ")
			alters = append(alters, newAlter(entitySchema, AlterKindDropForeignKey, AlterDestructive,
				"foreign keys are not part of the entity schema",
				fmt.Sprintf("ALTER TABLE `%s`.`%s`\n DROP FOREIGN KEY %s;",
					pool.GetConfig().GetDatabaseName(), entitySchema.GetTableName(), parts[1])))
			continue
		}
		if lines[x][2] != 96 {
			for _, field := range strings.Split(lines[x], " ") {
				if strings.HasPrefix(field, "CHARSET=") {
					sqlSchema.DBEncoding = field[8:]
				} else if strings.HasPrefix(field, "ENGINE=") {
					sqlSchema.Engine = field[7:]
				}
			}
			continue
		}
		var line = strings.TrimRight(lines[x], ",")
		line = strings.TrimLeft(line, " ")
		var columnName = strings.Split(line, "`")[1]
		sqlSchema.DBTableColumns = append(sqlSchema.DBTableColumns, &ColumnSchemaDefinition{columnName, line})
	}

	var rows []indexDB
	/* #nosec */
	results, def, err := pool.Query(ctx, fmt.Sprintf("SHOW INDEXES FROM `%s`", entitySchema.GetTableName()))
	if err != nil {
		return nil, err
	}
	defer def()
	for results.Next() {
		var row indexDB
		err = results.Scan(&row.Skip, &row.NonUnique, &row.KeyName, &row.Seq, &row.Column, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	def()
	for _, value := range rows {
		hasCurrent := false
		for _, current := range sqlSchema.DBIndexes {
			if current.Name == value.KeyName {
				hasCurrent = true
				current.columnsMap[value.Seq] = value.Column
				break
			}
		}
		if !hasCurrent {
			current := &IndexSchemaDefinition{Name: value.KeyName, Unique: value.NonUnique == 0, columnsMap: map[int]string{value.Seq: value.Column}}
			sqlSchema.DBIndexes = append(sqlSchema.DBIndexes, current)
		}
	}

	alters = append(alters, diffColumnsAndIndexes(entitySchema, sqlSchema, columns)...)

	return alters, nil
}

// diffColumnsAndIndexes emits one classified Alter per unit of work. Keyed on column name, not
// position: physical order is unobservable, so reordering a struct field must not rebuild a table.
func diffColumnsAndIndexes(entitySchema *entitySchema, sqlSchema *tableSQLSchemaDefinition, columns []*ColumnSchemaDefinition) []Alter {
	pool := entitySchema.GetDB()
	dbByName := make(map[string]*ColumnSchemaDefinition, len(sqlSchema.DBTableColumns))
	for _, c := range sqlSchema.DBTableColumns {
		dbByName[c.ColumnName] = c
	}
	entityByName := make(map[string]*ColumnSchemaDefinition, len(columns))
	for _, c := range columns {
		entityByName[c.ColumnName] = c
	}

	var alters []Alter
	// Added without a usable default, so an index over one has to wait for the same uniform fleet.
	deferredColumns := make(map[string]bool)

	for i, value := range columns {
		live, inDB := dbByName[value.ColumnName]
		if inDB {
			if isDefinitionEquivalent(live.Definition, value.Definition) {
				continue
			}
			if clause, ok := defaultOnlyDifference(live.Definition, value.Definition); ok {
				alters = append(alters, newAlter(entitySchema, AlterKindSetDefault, AlterSafe,
					"column default changed", alterTableSQL(entitySchema, instant(clause))))
				continue
			}
			alters = append(alters, newAlter(entitySchema, AlterKindChangeColumn, AlterDestructive,
				fmt.Sprintf("CHANGED FROM %s", live.Definition),
				alterTableSQL(entitySchema, fmt.Sprintf("CHANGE COLUMN `%s` %s", value.ColumnName, value.Definition))))
			continue
		}
		// An ADD COLUMN carrying an expression default cannot be INSTANT (1845), so the column is
		// added bare and the default follows as a set_default, ranked to run straight after. In
		// between the column is NOT NULL with no default and an INSERT omitting it fails 1364 —
		// a window on one locked connection, and the next boot re-emits the MODIFY if it is lost.
		addDefinition, deferredDefault := splitExpressionDefault(value.Definition)

		// AFTER only when the predecessor is already there, so it never names a missing column.
		clause := fmt.Sprintf("ADD COLUMN %s", addDefinition)
		if i > 0 {
			if _, predecessorLive := dbByName[columns[i-1].ColumnName]; predecessorLive {
				clause += fmt.Sprintf(" AFTER `%s`", columns[i-1].ColumnName)
			}
		}
		safety, reason := AlterSafe, ""
		if !isForwardCompatibleAdd(value.Definition) {
			safety = AlterDestructive
			reason = "NOT NULL without DEFAULT"
			deferredColumns[value.ColumnName] = true
		}
		alters = append(alters, newAlter(entitySchema, AlterKindAddColumn, safety, reason,
			alterTableSQL(entitySchema, instant(clause))))
		if deferredDefault {
			alters = append(alters, newAlter(entitySchema, AlterKindSetDefault, AlterSafe,
				"adding the column default the ADD could not carry",
				alterTableSQL(entitySchema, instant("MODIFY "+value.Definition))))
		}
	}

	for _, live := range sqlSchema.DBTableColumns {
		if _, stillWanted := entityByName[live.ColumnName]; stillWanted {
			continue
		}
		alters = append(alters, newAlter(entitySchema, AlterKindDropColumn, AlterDestructive,
			"column is no longer part of the entity",
			alterTableSQL(entitySchema, fmt.Sprintf("DROP COLUMN `%s`", live.ColumnName))))
	}

	alters = append(alters, diffIndexes(entitySchema, sqlSchema, deferredColumns)...)

	if sqlSchema.DBEncoding != pool.GetConfig().GetOptions().DefaultEncoding || sqlSchema.Engine != "InnoDB" {
		collate := " COLLATE=" + pool.GetConfig().GetOptions().DefaultEncoding + "_" + pool.GetConfig().GetOptions().DefaultCollate
		alters = append(alters, newAlter(entitySchema, AlterKindConvertTable, AlterDestructive,
			"table rebuild: engine or charset conversion",
			fmt.Sprintf("ALTER TABLE `%s`.`%s`\n ENGINE=InnoDB DEFAULT CHARSET=%s%s;",
				pool.GetConfig().GetDatabaseName(), entitySchema.GetTableName(),
				pool.GetConfig().GetOptions().DefaultEncoding, collate)))
	}

	return alters
}

func diffIndexes(entitySchema *entitySchema, sqlSchema *tableSQLSchemaDefinition, deferredColumns map[string]bool) []Alter {
	var alters []Alter

	sortedEntityIndexes := make([]*IndexSchemaDefinition, len(sqlSchema.EntityIndexes))
	copy(sortedEntityIndexes, sqlSchema.EntityIndexes)
	sort.Slice(sortedEntityIndexes, func(i, j int) bool { return sortedEntityIndexes[i].Name < sortedEntityIndexes[j].Name })

	for _, indexEntity := range sortedEntityIndexes {
		var live *IndexSchemaDefinition
		for _, index := range sqlSchema.DBIndexes {
			if index.Name == indexEntity.Name {
				live = index
				break
			}
		}
		addSQL := buildCreateIndexSQL(indexEntity)
		if live != nil {
			if addSQL == buildCreateIndexSQL(live) {
				continue
			}
			// The DROP and the ADD share a name, so splitting them makes the ADD fail with 1061.
			alters = append(alters, newAlter(entitySchema, AlterKindRebuildIndex, AlterDestructive,
				"index definition changed",
				alterTableSQL(entitySchema, fmt.Sprintf("DROP INDEX `%s`", indexEntity.Name), addSQL)))
			continue
		}
		if indexEntity.Duplicated {
			continue
		}
		kind, safety, reason := AlterKindAddIndex, AlterSafe, ""
		if indexEntity.Unique {
			// Neither ordering works: applied mid-rollout it fails the old version's inserts,
			// deferred the new pods create the duplicates that make it fail forever.
			kind, safety, reason = AlterKindAddUniqueIndex, AlterDestructive, "unique index cannot be added during a rollout"
		}
		if safety == AlterSafe && indexDependsOnDeferredColumn(indexEntity, deferredColumns) {
			safety = AlterDestructive
			reason = "indexes a column that is itself deferred"
		}
		alters = append(alters, newAlter(entitySchema, kind, safety, reason, alterTableSQL(entitySchema, addSQL)))
	}

	sortedDBIndexes := make([]*IndexSchemaDefinition, len(sqlSchema.DBIndexes))
	copy(sortedDBIndexes, sqlSchema.DBIndexes)
	sort.Slice(sortedDBIndexes, func(i, j int) bool { return sortedDBIndexes[i].Name < sortedDBIndexes[j].Name })

	for _, live := range sortedDBIndexes {
		if live.Name == "PRIMARY" {
			continue
		}
		wanted := false
		for _, index := range sqlSchema.EntityIndexes {
			if index.Name == live.Name && !index.Duplicated {
				wanted = true
				break
			}
		}
		if wanted {
			continue
		}
		alters = append(alters, newAlter(entitySchema, AlterKindDropIndex, AlterDestructive,
			"index is no longer part of the entity",
			alterTableSQL(entitySchema, fmt.Sprintf("DROP INDEX `%s`", live.Name))))
	}

	return alters
}

func indexDependsOnDeferredColumn(index *IndexSchemaDefinition, deferredColumns map[string]bool) bool {
	if len(deferredColumns) == 0 {
		return false
	}
	for _, col := range index.GetColumns() {
		if deferredColumns[col] {
			return true
		}
	}

	return false
}

func alterTableSQL(entitySchema *entitySchema, clauses ...string) string {
	pool := entitySchema.GetDB()
	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", pool.GetConfig().GetDatabaseName(), entitySchema.GetTableName())
	for i, clause := range clauses {
		if i > 0 {
			sql += ",\n"
		}
		sql += "    " + clause
	}

	return sql + ";"
}

func newAlter(entitySchema *entitySchema, kind AlterKind, safety AlterSafety, reason, sql string) Alter {
	return Alter{
		SQL:    sql,
		Pool:   entitySchema.GetDB().GetConfig().GetCode(),
		Kind:   kind,
		Table:  entitySchema.GetTableName(),
		Entity: entitySchema.GetType().String(),
		Safety: safety,
		Reason: reason,
	}
}

// isForwardCompatibleAdd reports whether the previous version can keep inserting once this column
// exists: it must accept NULL or supply its own default.
func isForwardCompatibleAdd(definition string) bool {
	if !strings.Contains(definition, "NOT NULL") {
		return true
	}

	return strings.Contains(definition, " DEFAULT ")
}

func checkColumn(engine Engine, schema *entitySchema, field *reflect.StructField, indexes map[string]*IndexSchemaDefinition, prefix string) ([]*ColumnSchemaDefinition, error) {
	var definition string
	var addNotNullIfNotSet bool
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	columnName := prefix + field.Name
	attributes := schema.tags[columnName]
	_, has := attributes["ignore"]
	if has {
		return nil, nil
	}
	var columns []*ColumnSchemaDefinition
	isArray := false
	arrayLen := 0
	fieldType := field.Type
	if field.Type.Kind().String() == "array" {
		fieldType = fieldType.Elem()
		isArray = true
		arrayLen = field.Type.Len()
		if arrayLen > 100 {
			return nil, fmt.Errorf("array len for column %s exceeded limit of 100", columnName)
		}
	}

	for i := 0; i <= arrayLen; i++ {
		columnName = prefix + field.Name
		if isArray {
			if i == arrayLen {
				break
			}
			columnName += "_" + strconv.Itoa(i+1)
		}
		for indexName, indexDef := range schema.uniqueIndexes {
			_, hasIndex := indexes[indexName]
			if !hasIndex {
				current := &IndexSchemaDefinition{Name: indexName, Unique: true, columnsMap: map[int]string{}}
				for k, v := range indexDef.Columns {
					current.columnsMap[k+1] = v
				}
				indexes[indexName] = current
			}
		}
		for indexName, indexDef := range schema.indexes {
			_, hasIndex := indexes[indexName]
			if !hasIndex {
				current := &IndexSchemaDefinition{Name: indexName, Unique: false, columnsMap: map[int]string{}}
				for k, v := range indexDef.Columns {
					current.columnsMap[k+1] = v
				}
				indexes[indexName] = current
			}
		}
		required, hasRequired := attributes["required"]
		isRequired := hasRequired && required == "true"

		var err error
		typeAsString := fieldType.String()
		switch typeAsString {
		case "uint",
			"uint8",
			"uint32",
			"uint64",
			"int8",
			"int16",
			"int32",
			"int64",
			"int":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, false)
		case "*uint",
			"*uint8",
			"*uint32",
			"*uint64",
			"*int8",
			"*int16",
			"*int32",
			"*int64",
			"*int":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, true)
		case "uint16":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, false)
		case "*uint16":
			definition, addNotNullIfNotSet, defaultValue = handleInt(typeAsString, attributes, true)
		case "bool":
			if columnName == "FakeDelete" && prefix == "" {
				idField, _ := schema.t.FieldByName("ID")
				definition, addNotNullIfNotSet, defaultValue = handleInt(idField.Type.String(), schema.tags["ID"], false)
			} else {
				definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", true, "'0'"
			}
		case "*bool":
			definition, addNotNullIfNotSet, defaultValue = "tinyint(1)", false, "nil"
		case "string":
			if enumVals, hasEnum := attributes["enum"]; hasEnum {
				definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleSetEnum("enum", schema, strings.Split(enumVals, ","), !isRequired)
			} else if setVals, hasSet := attributes["set"]; hasSet {
				definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleSetEnum("set", schema, strings.Split(setVals, ","), !isRequired)
			} else {
				definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = handleString(schema, attributes, !isRequired)
			}
			if err != nil {
				return nil, err
			}
		case "float32":
			definition, addNotNullIfNotSet, defaultValue = handleFloat("float", attributes, false)
		case "float64":
			definition, addNotNullIfNotSet, defaultValue = handleFloat("double", attributes, false)
		case "*float32":
			definition, addNotNullIfNotSet, defaultValue = handleFloat("float", attributes, true)
		case "*float64":
			definition, addNotNullIfNotSet, defaultValue = handleFloat("double", attributes, true)
		case "time.Time":
			if columnName == "CreatedAt" || columnName == "UpdatedAt" {
				if attributes == nil {
					attributes = make(map[string]string)
				}
				attributes["time"] = "true"
			}
			definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue = handleTime(attributes, false)
		case "*time.Time":
			if columnName == "CreatedAt" || columnName == "UpdatedAt" {
				if attributes == nil {
					attributes = make(map[string]string)
				}
				attributes["time"] = "true"
			}
			definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue = handleTime(attributes, true)
		case "[]uint8":
			definition, addDefaultNullIfNullable = handleBlob(attributes)
		default:
			kind := fieldType.Kind().String()
			if fieldType.Implements(reflect.TypeOf((*referencesInterface)(nil)).Elem()) {
				definition = "text"
				addNotNullIfNotSet = false
				defaultValue = "nil"
			} else if fieldType.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				refIDType := reflect.New(reflect.New(fieldType).Interface().(referenceInterface).getType()).Elem().FieldByName("ID").Type().String()
				definition, addNotNullIfNotSet, defaultValue = handleInt(refIDType, attributes, !isRequired)
			} else if kind == "struct" {
				subFieldPrefix := prefix
				arrayIndex := -1
				if isArray {
					arrayIndex = i + 1
				}
				structFields, err := checkStruct(engine, schema, fieldType, indexes, field, subFieldPrefix, arrayIndex)
				if err != nil {
					return nil, err
				}
				columns = append(columns, structFields...)
				continue
			} else if kind == "ptr" && fieldType.Elem().Kind() == reflect.Struct {
				definition = "text"
				addNotNullIfNotSet = false
				defaultValue = "nil"
			} else {
				return nil, fmt.Errorf("field type %s is not supported, consider adding  tag `ignore`", field.Type.String())
			}
		}
		isNotNull := false
		if addNotNullIfNotSet || isRequired {
			definition += " NOT NULL"
			isNotNull = true
		}
		if defaultValue == "nil" && isNotNull && columnName != "ID" && isTextOrBlob(definition) {
			// Without a default the previous version's INSERT omits the column and fails with 1364
			// under strict mode. MySQL takes an expression default on TEXT/BLOB since 8.0.13.
			defaultValue = "('')"
		}
		if defaultValue != "nil" && columnName != "ID" {
			definition += " DEFAULT " + defaultValue
		} else if !isNotNull && addDefaultNullIfNullable {
			definition += " DEFAULT NULL"
		}
		columns = append(columns, &ColumnSchemaDefinition{columnName, fmt.Sprintf("`%s` %s", columnName, definition)})
	}
	return columns, nil
}

// emptyExprDefaultRE matches the expression default MySQL accepts on TEXT and BLOB. The introducer
// follows the connection charset, not the column's, so the same column reads back differently.
var emptyExprDefaultRE = regexp.MustCompile(` DEFAULT \((?:_[a-z0-9]+)?''\)$`)

// isDefinitionEquivalent is the single place deciding two definitions describe the same column.
// The diff compares SHOW CREATE TABLE text against generated text, so anything MySQL renders
// differently has to be reconciled here or the alter is re-emitted forever.
func isDefinitionEquivalent(dbDef, entityDef string) bool {
	if dbDef == entityDef {
		return true
	}
	if isNullableDefaultEquivalent(dbDef, entityDef) {
		return true
	}

	// Introducer only, never the presence of the clause: a column with no default must still
	// read as different, or it never gets one.
	return canonicalExprDefault(dbDef) == canonicalExprDefault(entityDef)
}

// canonicalExprDefault rewrites MySQL's echo of an empty expression default to the generated form.
func canonicalExprDefault(definition string) string {
	return emptyExprDefaultRE.ReplaceAllString(definition, " DEFAULT ('')")
}

// defaultOnlyDifference returns the narrow statement for two definitions differing by nothing but
// their DEFAULT clause. A MODIFY, because MySQL rejects ALTER COLUMN ... SET DEFAULT on TEXT (1101).
func defaultOnlyDifference(dbDef, entityDef string) (string, bool) {
	dbBase := trimDefaultClause(dbDef)
	entityBase := trimDefaultClause(entityDef)
	if dbBase == "" || dbBase != entityBase || dbDef == entityDef {
		return "", false
	}

	return "MODIFY " + entityDef, true
}

var defaultClauseRE = regexp.MustCompile(` DEFAULT (\([^)]*\)|'[^']*'|[A-Za-z0-9_.]+)$`)

func trimDefaultClause(definition string) string {
	return defaultClauseRE.ReplaceAllString(definition, "")
}

// isNullableDefaultEquivalent returns true when two column definitions differ only
// by a trailing " DEFAULT NULL" suffix. MySQL implicitly defaults nullable columns
// to NULL, so `text` and `text DEFAULT NULL` are functionally identical.
func isNullableDefaultEquivalent(a, b string) bool {
	if strings.HasSuffix(b, " DEFAULT NULL") && !strings.Contains(b, "NOT NULL") {
		return strings.TrimSuffix(b, " DEFAULT NULL") == strings.TrimSuffix(a, " DEFAULT NULL")
	}
	if strings.HasSuffix(a, " DEFAULT NULL") && !strings.Contains(a, "NOT NULL") {
		return strings.TrimSuffix(a, " DEFAULT NULL") == strings.TrimSuffix(b, " DEFAULT NULL")
	}
	return false
}

func handleInt(typeAsString string, attributes map[string]string, nullable bool) (string, bool, string) {
	if nullable {
		if strings.HasPrefix(typeAsString, "*") {
			typeAsString = typeAsString[1:]
		}
		return convertIntToSchema(typeAsString, attributes), false, "nil"
	}
	return convertIntToSchema(typeAsString, attributes), true, "'0'"
}

func handleFloat(floatDefinition string, attributes map[string]string, nullable bool) (string, bool, string) {
	decimal, hasDecimal := attributes["decimal"]
	var definition string
	defaultValue := "'0'"
	if hasDecimal {
		decimalArgs := strings.Split(decimal, ",")
		definition = fmt.Sprintf("decimal(%s,%s)", decimalArgs[0], decimalArgs[1])
		defaultValue = fmt.Sprintf("'%s'", fmt.Sprintf("%."+decimalArgs[1]+"f", float32(0)))
	} else {
		definition = floatDefinition
	}
	unsigned, hasUnsigned := attributes["unsigned"]
	if hasUnsigned && unsigned == "true" {
		definition += " unsigned"
	}
	if nullable {
		return definition, false, "nil"
	}
	return definition, true, defaultValue
}

func handleBlob(attributes map[string]string) (string, bool) {
	definition := "blob"
	if attributes["mediumblob"] == "true" {
		definition = "mediumblob"
	}
	if attributes["longblob"] == "true" {
		definition = "longblob"
	}

	return definition, false
}

func handleString(schema *entitySchema, attributes map[string]string, nullable bool) (string, bool, bool, string, error) {
	dbOptions := schema.GetDB().GetConfig().GetOptions()
	var definition string
	length, hasLength := attributes["length"]
	if !hasLength {
		length = "255"
	}
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	if !nullable {
		defaultValue = "''"
	}
	if length == "max" {
		definition = "mediumtext"
		encoding := dbOptions.DefaultEncoding
		definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_" + dbOptions.DefaultCollate
		addDefaultNullIfNullable = false
		defaultValue = "nil"
	} else {
		i, err := strconv.Atoi(length)
		if err != nil || i > 65535 {
			return "", false, false, "", fmt.Errorf("invalid max string: %s", length)
		}
		definition = fmt.Sprintf("varchar(%s) CHARACTER SET %s COLLATE %s_%s", strconv.Itoa(i),
			dbOptions.DefaultEncoding, dbOptions.DefaultEncoding, dbOptions.DefaultCollate)
	}
	return definition, !nullable, addDefaultNullIfNullable, defaultValue, nil
}
func handleSetEnum(sqlType string, schema *entitySchema, values []string, nullable bool) (string, bool, bool, string, error) {
	if len(values) == 0 {
		return "", false, false, "", errors.New("empty enum not allowed")
	}
	var definition = sqlType + "("
	for key, value := range values {
		if key > 0 {
			definition += ","
		}
		definition += fmt.Sprintf("'%s'", strings.TrimSpace(value))
	}
	definition += ")"
	encoding := schema.GetDB().GetConfig().GetOptions().DefaultEncoding
	definition += " CHARACTER SET " + encoding + " COLLATE " + encoding + "_0900_ai_ci"
	defaultValue := "nil"
	if !nullable {
		defaultValue = fmt.Sprintf("'%s'", strings.TrimSpace(values[0]))
	}
	return definition, !nullable, true, defaultValue, nil
}

func handleTime(attributes map[string]string, nullable bool) (string, bool, bool, string) {
	t := attributes["time"]
	defaultValue := "nil"
	if t == "true" {
		if !nullable {
			defaultValue = "'1000-01-01 00:00:00'"
		}
		return "datetime", !nullable, true, defaultValue
	}
	if !nullable {
		defaultValue = "'0001-01-01'"
	}
	return "date", !nullable, true, defaultValue
}

func convertIntToSchema(typeAsString string, attributes Meta) string {
	switch typeAsString {
	case "uint":
		return "int unsigned"
	case "uint8":
		return "tinyint unsigned"
	case "uint16":
		return "smallint unsigned"
	case "uint32":
		if attributes["mediumint"] == "true" {
			return "mediumint unsigned"
		}
		return "int unsigned"
	case "uint64":
		return "bigint unsigned"
	case "int8":
		return "tinyint"
	case "int16":
		return "smallint"
	case "int32":
		if attributes["mediumint"] == "true" {
			return "mediumint"
		}
		return "int"
	case "int64":
		return "bigint"
	default:
		return "int"
	}
}

type ColumnSchemaDefinition struct {
	ColumnName string
	Definition string
}

func checkStruct(engine Engine, entitySchema *entitySchema, t reflect.Type, indexes map[string]*IndexSchemaDefinition,
	subField *reflect.StructField, subFieldPrefix string, arrayIndex int) ([]*ColumnSchemaDefinition, error) {
	columns := make([]*ColumnSchemaDefinition, 0)
	if subField == nil {
		f, hasID := t.FieldByName("ID")
		if !hasID || len(f.Index) != 1 || f.Index[0] != 0 {
			return nil, errors.New("field ID on position 1 is missing")
		}
		// IDs come from the snowflake generator, which needs the full 64-bit
		// range. A narrower column silently truncates or fails with MySQL 1264.
		if f.Type.String() != "uint64" {
			return nil, fmt.Errorf("ID column must be uint64, got %s on %s", f.Type.String(), t.String())
		}
	}
	maxFields := t.NumField() - 1
	for i := 0; i <= maxFields; i++ {
		field := t.Field(i)
		prefix := subFieldPrefix
		if subField != nil && !subField.Anonymous {
			prefix += subField.Name
			if arrayIndex > 0 {
				prefix += "_" + strconv.Itoa(arrayIndex) + "_"
			}
		}
		fieldColumns, err := checkColumn(engine, entitySchema, &field, indexes, prefix)
		if err != nil {
			return nil, err
		}
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
	}
	return columns, nil
}

func buildCreateIndexSQL(index *IndexSchemaDefinition) string {
	var indexColumns []string
	for i := 1; i <= 100; i++ {
		value, has := index.columnsMap[i]
		if has {
			indexColumns = append(indexColumns, fmt.Sprintf("`%s`", value))
		} else {
			break
		}
	}
	indexType := "INDEX"
	if index.Unique {
		indexType = "UNIQUE " + indexType
	}
	return fmt.Sprintf("ADD %s `%s` (%s)", indexType, index.Name, strings.Join(indexColumns, ","))
}

// isTextOrBlob reports whether MySQL refuses a literal default on this column's type.
func isTextOrBlob(definition string) bool {
	for _, t := range []string{"text", "blob"} {
		if strings.HasPrefix(definition, t) || strings.HasPrefix(definition, "medium"+t) ||
			strings.HasPrefix(definition, "long"+t) || strings.HasPrefix(definition, "tiny"+t) {
			return true
		}
	}

	return false
}

// instant pins the algorithm rather than hoping for it: MySQL silently falls back to a full rebuild
// after 64 instant additions to a table, and at boot that is a stall with no explanation.
func instant(clause string) string {
	return clause + ", ALGORITHM=INSTANT"
}

// splitExpressionDefault separates a trailing expression default. MySQL refuses ALGORITHM=INSTANT
// for an ADD COLUMN carrying one, but accepts it for a MODIFY adding the same default afterwards.
func splitExpressionDefault(definition string) (string, bool) {
	trimmed := emptyExprDefaultRE.ReplaceAllString(definition, "")

	return trimmed, trimmed != definition
}
