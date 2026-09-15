package fluxaorm

import "fmt"

// Each write needs an immutable view for events and an independently advancing
// live baseline. SQL rows contain only scalar values, strings, time.Time and
// sql.Null* structs, so copying the row and maps also isolates JSON/byte fields.
func (g *codeGenerator) generatePrivateWriteState(schema *entitySchema, names *entityNames) {
	g.addImport("maps")
	fIdx := 0
	cols := buildColOriginInfos(schema.fields, &fIdx)

	g.addLine(fmt.Sprintf("func (e *%s) PrivateSnapshot() fluxaorm.Entity {", names.entityName))
	g.addLine("\tcopy := *e")
	g.addLine("\tcopy.snapshot = true")
	g.addLine("\tif e.originDatabaseValues != nil { row := *e.originDatabaseValues; copy.originDatabaseValues = &row }")
	if schema.hasRedisCache {
		g.addLine("\tcopy.originRedisValues = append([]string(nil), e.originRedisValues...)")
	}
	g.addLine("\tcopy.databaseBind = maps.Clone(e.databaseBind)")
	g.addLine("\tcopy.flushChanges = maps.Clone(e.flushChanges)")
	g.addLine("\treturn &copy")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) PrivateIsSnapshot() bool { return e.snapshot }", names.entityName))
	if schema.hasFakeDelete {
		g.addLine(fmt.Sprintf("func (e *%s) PrivateIsDeleted() bool { return e.removed || e.privateGetOriginalColumnValue(\"FakeDelete\").(bool) }", names.entityName))
	} else {
		g.addLine(fmt.Sprintf("func (e *%s) PrivateIsDeleted() bool { return e.removed }", names.entityName))
	}
	g.addLine(fmt.Sprintf("func (e *%s) PrivateDatabasePool() string { return %s.dbCode }", names.entityName, names.providerName))
	g.addLine("")

	// withChanges=false is the actual database baseline even when the first
	// transaction write already has edits in its bind.
	g.addLine(fmt.Sprintf("func (e *%s) privateSQLRow(withChanges bool) *%s {", names.entityName, names.sqlRowName))
	g.addLine(fmt.Sprintf("\trow := &%s{}", names.sqlRowName))
	g.addLine("\tif e.originDatabaseValues != nil { *row = *e.originDatabaseValues }")
	g.addLine("\trow.F0 = e.id")
	if schema.hasRedisCache {
		g.addLine("\tif e.originRedisValues != nil {")
		for _, c := range cols {
			g.generateWriteStateRedisColumn(c)
		}
		g.addLine("\t}")
	}
	g.addLine("\tif withChanges {")
	g.addLine("\t\tfor column, value := range e.databaseBind {")
	g.addLine("\t\t\tswitch column {")
	for _, c := range cols {
		g.addLine(fmt.Sprintf("\t\t\tcase %q: row.F%d = value.(%s)", c.colName, c.fIndex, writeStateColumnType(c.category)))
	}
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn row")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) privateRebase(current, baseline *%s) {", names.entityName, names.sqlRowName))
	g.addLine("\te.originDatabaseValues = baseline")
	if schema.hasRedisCache {
		g.addLine("\te.originRedisValues = nil")
	}
	g.addLine("\te.databaseBind = nil")
	g.addLine("\te.flushType = 0")
	g.addLine("\te.flushChanges = nil")
	g.addLine("\tif e.new { e.originDatabaseValues = current; return }")
	for _, c := range cols {
		current := fmt.Sprintf("current.F%d", c.fIndex)
		baseline := fmt.Sprintf("baseline.F%d", c.fIndex)
		equal := current + " == " + baseline
		if c.category == "time" {
			equal = current + ".Equal(" + baseline + ")"
		} else if c.category == "nullTime" {
			equal = current + ".Valid == " + baseline + ".Valid && (!" + current + ".Valid || " + current + ".Time.Equal(" + baseline + ".Time))"
		}
		g.addLine(fmt.Sprintf("\tif !(%s) { e.addToDatabaseBind(%q, %s) }", equal, c.colName, current))
	}
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateAdvance(saved fluxaorm.Entity) {", names.entityName))
	g.addLine(fmt.Sprintf("\twrite := saved.(*%s)", names.entityName))
	g.addLine("\tcurrent := e.privateSQLRow(true)")
	g.addLine("\te.new = false")
	g.addLine("\te.removed = e.removed || write.deleted")
	g.addLine("\te.deleted = e.deleted && !write.deleted")
	g.addLine("\te.privateRebase(current, write.privateSQLRow(true))")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateRollback(saved fluxaorm.Entity) {", names.entityName))
	g.addLine(fmt.Sprintf("\tbefore := saved.(*%s)", names.entityName))
	g.addLine("\tcurrent := e.privateSQLRow(true)")
	g.addLine("\te.new = before.new")
	g.addLine("\te.deleted = e.deleted || (e.removed && !before.removed)")
	g.addLine("\te.removed = before.removed")
	g.addLine("\te.privateRebase(current, before.privateSQLRow(false))")
	g.addLine("}")
}

func writeStateColumnType(category string) string {
	switch category {
	case "fakeDeleteBool":
		return "uint64"
	case "time":
		return "time.Time"
	case "nullUint64", "nullInt64":
		return "sql.NullInt64"
	case "nullBool":
		return "sql.NullBool"
	case "nullFloat64":
		return "sql.NullFloat64"
	case "nullTime":
		return "sql.NullTime"
	case "nullString":
		return "sql.NullString"
	default:
		return category
	}
}

func (g *codeGenerator) generateWriteStateRedisColumn(c colOriginInfo) {
	raw := fmt.Sprintf("e.originRedisValues[%d]", c.fIndex)
	target := fmt.Sprintf("row.F%d", c.fIndex)
	g.addLine("\t\t{")
	indent := "\t\t\t"
	category := c.category
	switch category {
	case "nullUint64", "nullInt64", "nullBool", "nullFloat64", "nullTime", "nullString":
		g.addLine(fmt.Sprintf("%sif %s != \"\" {", indent, raw))
		indent += "\t"
		g.addLine(fmt.Sprintf("%s%s.Valid = true", indent, target))
		switch category {
		case "nullUint64", "nullInt64":
			target += ".Int64"
		case "nullBool":
			target += ".Bool"
		case "nullFloat64":
			target += ".Float64"
		case "nullTime":
			target += ".Time"
		case "nullString":
			target += ".String"
		}
	}
	switch category {
	case "uint64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s%s, _ = strconv.ParseUint(%s, 10, 64)", indent, target, raw))
	case "int64", "nullInt64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s%s, _ = strconv.ParseInt(%s, 10, 64)", indent, target, raw))
	case "nullUint64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%svalue, _ := strconv.ParseUint(%s, 10, 64)", indent, raw))
		g.addLine(fmt.Sprintf("%s%s = int64(value)", indent, target))
	case "float64", "nullFloat64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s%s, _ = strconv.ParseFloat(%s, 64)", indent, target, raw))
	case "time", "nullTime":
		g.addImport("strconv")
		g.addImport("time")
		g.addLine(fmt.Sprintf("%svalue, _ := strconv.ParseInt(%s, 10, 64)", indent, raw))
		g.addLine(fmt.Sprintf("%s%s = time.Unix(value, 0).UTC()", indent, target))
	case "bool", "nullBool":
		g.addLine(fmt.Sprintf("%s%s = %s == \"1\"", indent, target, raw))
	case "fakeDeleteBool":
		g.addLine(fmt.Sprintf("%sif %s == \"1\" { %s = e.id }", indent, raw, target))
	case "string", "nullString":
		g.addLine(fmt.Sprintf("%s%s = %s", indent, target, raw))
	}
	if indent == "\t\t\t\t" {
		g.addLine("\t\t\t}")
	}
	g.addLine("\t\t}")
}
