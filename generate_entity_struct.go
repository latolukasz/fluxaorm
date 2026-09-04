package fluxaorm

import (
	"fmt"
	"sort"
	"strings"
)

type uniqueIndexColInfo struct {
	colName        string
	fIndex         int
	nullable       bool
	bindNullType   string // e.g. "sql.NullInt64" — only for nullable
	bindInnerField string // e.g. ".Int64" — only for nullable
}

func sortedCachedUniqueIndexNames(schema *entitySchema) []string {
	names := make([]string, 0, len(schema.cachedUniqueIndexes))
	for idxName, isCached := range schema.cachedUniqueIndexes {
		if isCached {
			names = append(names, idxName)
		}
	}
	sort.Strings(names)

	return names
}

func (g *codeGenerator) getUniqueIndexColInfo(schema *entitySchema, colName string, fIndex int) uniqueIndexColInfo {
	info := uniqueIndexColInfo{colName: colName, fIndex: fIndex}
	attr, ok := schema.fieldDefinitions[colName]
	if !ok {
		return info
	}
	_, isRef := schema.references[colName]
	tn := attr.TypeName
	_, hasEnum := attr.Tags["enum"]
	_, hasSet := attr.Tags["set"]
	isRequired := attr.Tags["required"] == "true"

	if strings.HasPrefix(tn, "*") {
		// Pointer types are always nullable
		info.nullable = true
	} else if isRef {
		info.nullable = !isRequired
	} else if tn == "string" || hasEnum || hasSet {
		// Non-pointer strings/enums/sets are nullable unless required
		info.nullable = !isRequired
	}
	// Numeric types, bool, time.Time without pointer are always non-nullable

	if !info.nullable {
		return info
	}
	if isRef || strings.Contains(tn, "uint") || strings.Contains(tn, "int") {
		info.bindNullType = "sql.NullInt64"
		info.bindInnerField = ".Int64"
	} else if strings.Contains(tn, "float") {
		info.bindNullType = "sql.NullFloat64"
		info.bindInnerField = ".Float64"
	} else if strings.Contains(tn, "bool") {
		info.bindNullType = "sql.NullBool"
		info.bindInnerField = ".Bool"
	} else if strings.Contains(tn, "time.Time") {
		info.bindNullType = "sql.NullTime"
		info.bindInnerField = ".Time"
	} else {
		// string, enum, set
		info.bindNullType = "sql.NullString"
		info.bindInnerField = ".String"
	}
	return info
}

func (g *codeGenerator) generateUniqueIndexKeyFromOrigin(schema *entitySchema, names *entityNames, indexName string, cols []uniqueIndexColInfo, indent string, keyVar string, isInsert bool) {
	g.addImport("strconv")

	hasNullable := false
	for _, c := range cols {
		if c.nullable {
			hasNullable = true
			break
		}
	}

	// For INSERT, originDatabaseValues is always available
	// For DELETE/UPDATE, entity might be loaded from Redis, so handle both sources
	if !isInsert && schema.hasRedisCache {
		if hasNullable {
			g.addLine(fmt.Sprintf("%s%s_valid := true", indent, keyVar))
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			// Nullable check from Redis
			for _, c := range cols {
				if c.nullable {
					g.addLine(fmt.Sprintf("%s\tif e.originRedisValues[%d] == \"\" { %s_valid = false }", indent, c.fIndex, keyVar))
				}
			}
			g.addLine(fmt.Sprintf("%s} else {", indent))
			for _, c := range cols {
				if c.nullable {
					g.addLine(fmt.Sprintf("%s\tif !e.originDatabaseValues.F%d.Valid { %s_valid = false }", indent, c.fIndex, keyVar))
				}
			}
			g.addLine(fmt.Sprintf("%s}", indent))
			g.addLine(fmt.Sprintf("%sif %s_valid {", indent, keyVar))
			g.generateUniqueIndexHashDualSource(cols, indent+"\t", keyVar, names, indexName)
			g.addLine(fmt.Sprintf("%s\te.ctx.InvalidateCacheKey(%s.redisCode, %s)", indent, names.providerName, keyVar))
			g.addLine(fmt.Sprintf("%s}", indent))
		} else {
			g.generateUniqueIndexHashDualSource(cols, indent, keyVar, names, indexName)
			g.addLine(fmt.Sprintf("%se.ctx.InvalidateCacheKey(%s.redisCode, %s)", indent, names.providerName, keyVar))
		}
	} else {
		// INSERT path or no Redis cache — originDatabaseValues is always available
		if hasNullable {
			validChecks := ""
			for _, c := range cols {
				if c.nullable {
					if validChecks != "" {
						validChecks += " && "
					}
					validChecks += fmt.Sprintf("e.originDatabaseValues.F%d.Valid", c.fIndex)
				}
			}
			g.addLine(fmt.Sprintf("%sif %s {", indent, validChecks))
			g.generateUniqueIndexHashFromDB(cols, indent+"\t", keyVar, names, indexName)
			g.addLine(fmt.Sprintf("%s\te.ctx.InvalidateCacheKey(%s.redisCode, %s)", indent, names.providerName, keyVar))
			g.addLine(fmt.Sprintf("%s}", indent))
		} else {
			g.generateUniqueIndexHashFromDB(cols, indent, keyVar, names, indexName)
			g.addLine(fmt.Sprintf("%se.ctx.InvalidateCacheKey(%s.redisCode, %s)", indent, names.providerName, keyVar))
		}
	}
}

func (g *codeGenerator) generateUniqueIndexHashDualSource(cols []uniqueIndexColInfo, indent string, keyVar string, names *entityNames, indexName string) {
	redisArgs := ""
	dbArgs := ""
	for i, c := range cols {
		if i > 0 {
			redisArgs += ", "
			dbArgs += ", "
		}
		redisArgs += fmt.Sprintf("e.originRedisValues[%d]", c.fIndex)
		if c.nullable {
			dbArgs += fmt.Sprintf("e.originDatabaseValues.F%d%s", c.fIndex, c.bindInnerField)
		} else {
			dbArgs += fmt.Sprintf("e.originDatabaseValues.F%d", c.fIndex)
		}
	}
	g.addLine(fmt.Sprintf("%s%s_v := \"\"", indent, keyVar))
	g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
	g.addLine(fmt.Sprintf("%s\t%s_v = fluxaorm.UniqueIndexKeyHash(%s)", indent, keyVar, redisArgs))
	g.addLine(fmt.Sprintf("%s} else {", indent))
	g.addLine(fmt.Sprintf("%s\t%s_v = fluxaorm.UniqueIndexKeyHash(%s)", indent, keyVar, dbArgs))
	g.addLine(fmt.Sprintf("%s}", indent))
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + %q + %s_v", indent, keyVar, names.providerName, indexSegment(indexName, cols), keyVar))
}

func (g *codeGenerator) generateUniqueIndexHashFromDB(cols []uniqueIndexColInfo, indent string, keyVar string, names *entityNames, indexName string) {
	args := ""
	for i, c := range cols {
		if i > 0 {
			args += ", "
		}
		if c.nullable {
			args += fmt.Sprintf("e.originDatabaseValues.F%d%s", c.fIndex, c.bindInnerField)
		} else {
			args += fmt.Sprintf("e.originDatabaseValues.F%d", c.fIndex)
		}
	}
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + %q + fluxaorm.UniqueIndexKeyHash(%s)", indent, keyVar, names.providerName, indexSegment(indexName, cols), args))
}

func (g *codeGenerator) generateUniqueIndexHashFromVars(cols []uniqueIndexColInfo, idxNum int, indent string, keyVar string, names *entityNames, indexName string) {
	args := ""
	for i := range cols {
		if i > 0 {
			args += ", "
		}
		args += fmt.Sprintf("_uNewV%d_%d", idxNum, i)
	}
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + %q + fluxaorm.UniqueIndexKeyHash(%s)", indent, keyVar, names.providerName, indexSegment(indexName, cols), args))
}

// searchHSetAppendFromOrigin returns generated code lines (with trailing \n) that append a
// searchable field's serialised value (read from originDatabaseValues.Fn) to the variable
// named varName.
func (g *codeGenerator) searchHSetAppendFromOrigin(f searchableFieldDef, indent, varName string) string {
	n := f.sqlRowIndex
	col := f.columnName
	var sb strings.Builder
	if !f.nullable {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatUint(e.originDatabaseValues.F%d, 10))\n", indent, varName, varName, col, n))
		case "int":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d, 10))\n", indent, varName, varName, col, n))
		case "float":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatFloat(e.originDatabaseValues.F%d, 'g', -1, 64))\n", indent, varName, varName, col, n))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Unix(), 10))\n", indent, varName, varName, col, n))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, e.originDatabaseValues.F%d)\n", indent, varName, varName, col, n))
		}
	} else {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(uint64(e.originDatabaseValues.F%d.Int64), 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "int":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Int64, 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(e.originDatabaseValues.F%d.Float64, 'g', -1, 64))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Time.Unix(), 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\tif e.originDatabaseValues.F%d.Bool {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set (sql.NullString)
			sb.WriteString(fmt.Sprintf("%sif e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, e.originDatabaseValues.F%d.String)\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	}
	return sb.String()
}

// searchHSetAppendFromDBBind returns generated code lines that append a searchable field's
// serialised value (checking databaseBind first, then originDatabaseValues) to varName.
func (g *codeGenerator) searchHSetAppendFromDBBind(f searchableFieldDef, indent, varName string) string {
	n := f.sqlRowIndex
	col := f.columnName
	var sb strings.Builder
	if !f.nullable {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(_sv.(uint64), 10))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(e.originDatabaseValues.F%d, 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "int":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(_sv.(int64), 10))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d, 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(_sv.(float64), 'g', -1, 64))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(e.originDatabaseValues.F%d, 'g', -1, 64))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(_sv.(time.Time).Unix(), 10))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Unix(), 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\tif _sv.(bool) {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif e.originDatabaseValues.F%d {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, _sv.(string))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, e.originDatabaseValues.F%d)\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	} else {
		// For nullable fields, databaseBind stores sql.NullXxx types
		switch f.goKind {
		case "uint", "ref", "int":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullInt64)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			if f.goKind == "int" {
				sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatInt(_nv.Int64, 10))\n", indent, varName, varName, col))
			} else {
				sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatUint(uint64(_nv.Int64), 10))\n", indent, varName, varName, col))
			}
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else if e.originDatabaseValues.F%d.Valid {\n", indent, n))
			if f.goKind == "int" {
				sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Int64, 10))\n", indent, varName, varName, col, n))
			} else {
				sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(uint64(e.originDatabaseValues.F%d.Int64), 10))\n", indent, varName, varName, col, n))
			}
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullFloat64)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatFloat(_nv.Float64, 'g', -1, 64))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else if e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(e.originDatabaseValues.F%d.Float64, 'g', -1, 64))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullTime)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatInt(_nv.Time.Unix(), 10))\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else if e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(e.originDatabaseValues.F%d.Time.Unix(), 10))\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullBool)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\tif _nv.Bool {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else if e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\tif e.originDatabaseValues.F%d.Bool {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set (sql.NullString)
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullString)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, _nv.String)\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s} else if e.originDatabaseValues.F%d.Valid {\n", indent, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, e.originDatabaseValues.F%d.String)\n", indent, varName, varName, col, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	}
	return sb.String()
}

// searchHSetAppendOnlyChanged returns generated code that appends a searchable field's
// serialised value to hsetVar only if the field is present in databaseBind. For nullable
// fields that become NULL, the column name is appended to hdelVar instead.
func (g *codeGenerator) searchHSetAppendOnlyChanged(f searchableFieldDef, indent, hsetVar, hdelVar string) string {
	col := f.columnName
	var sb strings.Builder
	if !f.nullable {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(_sv.(uint64), 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "int":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(_sv.(int64), 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(_sv.(float64), 'g', -1, 64))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(_sv.(time.Time).Unix(), 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\tif _sv.(bool) {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, _sv.(string))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	} else {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullInt64)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatUint(uint64(_nv.Int64), 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "int":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullInt64)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatInt(_nv.Int64, 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullFloat64)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatFloat(_nv.Float64, 'g', -1, 64))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullTime)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, strconv.FormatInt(_nv.Time.Unix(), 10))\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullBool)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\tif _nv.Bool {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t\t%s = append(%s, %q, \"1\")\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t\t%s = append(%s, %q, \"0\")\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set (sql.NullString)
			sb.WriteString(fmt.Sprintf("%sif _sv, _ok := e.databaseBind[%q]; _ok {\n", indent, col))
			sb.WriteString(fmt.Sprintf("%s\t_nv := _sv.(sql.NullString)\n", indent))
			sb.WriteString(fmt.Sprintf("%s\tif _nv.Valid {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, _nv.String)\n", indent, hsetVar, hsetVar, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q)\n", indent, hdelVar, hdelVar, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	}
	return sb.String()
}

// searchHSetAppendFromVar is like searchHSetAppendFromOrigin but reads from an arbitrary
// local variable (e.g. "_sqlRow") instead of "e.originDatabaseValues".
func (g *codeGenerator) searchHSetAppendFromVar(f searchableFieldDef, indent, varName, sourceVar string) string {
	n := f.sqlRowIndex
	col := f.columnName
	var sb strings.Builder
	if !f.nullable {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatUint(%s.F%d, 10))\n", indent, varName, varName, col, sourceVar, n))
		case "int":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatInt(%s.F%d, 10))\n", indent, varName, varName, col, sourceVar, n))
		case "float":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatFloat(%s.F%d, 'g', -1, 64))\n", indent, varName, varName, col, sourceVar, n))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, strconv.FormatInt(%s.F%d.Unix(), 10))\n", indent, varName, varName, col, sourceVar, n))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set
			sb.WriteString(fmt.Sprintf("%s%s = append(%s, %q, %s.F%d)\n", indent, varName, varName, col, sourceVar, n))
		}
	} else {
		switch f.goKind {
		case "uint", "ref":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatUint(uint64(%s.F%d.Int64), 10))\n", indent, varName, varName, col, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "int":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(%s.F%d.Int64, 10))\n", indent, varName, varName, col, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "float":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatFloat(%s.F%d.Float64, 'g', -1, 64))\n", indent, varName, varName, col, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "time", "date":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, strconv.FormatInt(%s.F%d.Time.Unix(), 10))\n", indent, varName, varName, col, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		case "bool":
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\tif %s.F%d.Bool {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"1\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t} else {\n", indent))
			sb.WriteString(fmt.Sprintf("%s\t\t%s = append(%s, %q, \"0\")\n", indent, varName, varName, col))
			sb.WriteString(fmt.Sprintf("%s\t}\n", indent))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		default: // string, enum, set (sql.NullString)
			sb.WriteString(fmt.Sprintf("%sif %s.F%d.Valid {\n", indent, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s\t%s = append(%s, %q, %s.F%d.String)\n", indent, varName, varName, col, sourceVar, n))
			sb.WriteString(fmt.Sprintf("%s}\n", indent))
		}
	}
	return sb.String()
}

func (g *codeGenerator) generateEntityStruct(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("type %s struct {", names.entityName))
	g.addLine("\tctx fluxaorm.Context")
	g.addLine("\tid uint64")
	g.addLine("\tnew bool")
	g.addLine("\tdeleted bool")
	g.addLine(fmt.Sprintf("\toriginDatabaseValues *%s", names.sqlRowName))
	g.addLine("\tdatabaseBind map[string]any")
	if schema.hasRedisCache {
		g.addLine("\toriginRedisValues []string")
	}
	g.addLine("\tflushType uint8")
	g.addLine("\tflushChanges map[string]any")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) GetID() uint64 {", names.entityName))
	g.addLine("\treturn e.id")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateDelete() {", names.entityName))
	if schema.hasFakeDelete {
		g.addLine("\te.SetFakeDelete(true)")
	} else {
		g.addLine("\te.deleted = true")
	}
	g.addLine("}")
	g.addLine("")

	if schema.hasFakeDelete {
		g.addLine(fmt.Sprintf("func (e *%s) PrivateForceDelete() {", names.entityName))
		g.addLine("\te.deleted = true")
		g.addLine("}")
		g.addLine("")
	}

	g.addLine(fmt.Sprintf("func (e *%s) addToDatabaseBind(column string, value any) {", names.entityName))
	g.addLine("\tif e.databaseBind == nil {")
	g.addLine("\t\te.databaseBind = map[string]any{}")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\te.databaseBind[column] = value"))
	g.addLine("}")
	g.addLine("")

	if schema.hasRedisSearch {
		g.addImport("strconv")
		for _, f := range schema.searchableFields {
			if f.nullable {
				g.addImport("database/sql")
			}
			if f.goKind == "time" || f.goKind == "date" {
				g.addImport("time")
			}
		}
	}

	// PrivateFlush
	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlush() error {", names.entityName))

	// INSERT block
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\tfor _, cb := range %sBeforeInsertCallbacks { cb(e) }", names.entityPrivate))
	if schema.hasCreatedAt {
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.IsZero() {", schema.createdAtFIndex))
		g.addLine(fmt.Sprintf("\t\t\te.originDatabaseValues.F%d = time.Now().UTC().Truncate(time.Second)", schema.createdAtFIndex))
		g.addLine("\t\t}")
	}
	if schema.hasUpdatedAt {
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.IsZero() {", schema.updatedAtFIndex))
		g.addLine(fmt.Sprintf("\t\t\te.originDatabaseValues.F%d = time.Now().UTC().Truncate(time.Second)", schema.updatedAtFIndex))
		g.addLine("\t\t}")
	}
	insertQueryLine := "\t\tsqlQuery := \"INSERT INTO `" + schema.tableName + "` (`ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		insertQueryLine += ",`" + columnName + "`"
	}
	insertQueryLine += fmt.Sprintf(") VALUES (?%s)\"\n", strings.Repeat(",?", len(schema.columnNames)-1))
	insertQueryLine += fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQueryForTable(%s.tableName, sqlQuery, e.originDatabaseValues.F0", names.providerName, names.providerName)
	for i := 1; i < len(schema.columnNames); i++ {
		insertQueryLine += fmt.Sprintf(", e.originDatabaseValues.F%d", i)
	}
	insertQueryLine += ")\n"
	if schema.hasRedisCache {
		insertQueryLine += fmt.Sprintf("\t\te.ctx.InvalidateCacheKey(%s.redisCode, %s.redisCachePrefix+strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName)
	}
	g.addLine(insertQueryLine)

	// Cached unique index INSERT
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")
		idxNum := 0
		for _, idxName := range sortedCachedUniqueIndexNames(schema) {
			fIndexes := schema.uniqueIndexFIndexes[idxName]
			index := schema.uniqueIndexes[idxName]
			cols := make([]uniqueIndexColInfo, len(index.Columns))
			for i, colName := range index.Columns {
				cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
			}
			keyVar := fmt.Sprintf("_uKey%d", idxNum)
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t", keyVar, true)
			idxNum++
		}
	}

	// Redis Search INSERT
	if schema.hasRedisSearch {
		g.addImport("strconv")
		fdIndex := -1
		if schema.hasFakeDelete {
			for i, cn := range schema.columnNames {
				if cn == "FakeDelete" {
					fdIndex = i
					break
				}
			}
		}
		cap := len(schema.searchableFields) * 2
		g.addLine(fmt.Sprintf("\t\t_searchKey := %s.redisSearchPrefix + strconv.FormatUint(e.GetID(), 10)", names.providerName))
		g.addLine(fmt.Sprintf("\t\t_sp := e.ctx.RedisPipeLine(%s.redisSearchCode)", names.providerName))
		if fdIndex >= 0 {
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d == 0 {", fdIndex))
			g.addLine(fmt.Sprintf("\t\t\t_sp.Del(_searchKey)"))
			g.addLine(fmt.Sprintf("\t\t\t_sa := make([]any, 0, %d)", cap))
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendFromOrigin(f, "\t\t\t", "_sa")
			}
			g.addLine(fmt.Sprintf("\t\t\tif len(_sa) > 0 {"))
			g.addLine(fmt.Sprintf("\t\t\t\t_sp.HSet(_searchKey, _sa...)"))
			g.addLine(fmt.Sprintf("\t\t\t}"))
			g.addLine(fmt.Sprintf("\t\t}"))
		} else {
			g.addLine(fmt.Sprintf("\t\t_sp.Del(_searchKey)"))
			g.addLine(fmt.Sprintf("\t\t_sa := make([]any, 0, %d)", cap))
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendFromOrigin(f, "\t\t", "_sa")
			}
			g.addLine(fmt.Sprintf("\t\tif len(_sa) > 0 {"))
			g.addLine(fmt.Sprintf("\t\t\t_sp.HSet(_searchKey, _sa...)"))
			g.addLine(fmt.Sprintf("\t\t}"))
		}
	}

	g.addLine("\t\te.flushType = 1")
	g.addLine("\t\treturn nil")
	g.addLine("\t}")

	// DELETE block
	g.addLine("\tif e.deleted {")
	g.addLine(fmt.Sprintf("\t\tfor _, cb := range %sBeforeDeleteCallbacks { cb(e) }", names.entityPrivate))
	g.addLine(fmt.Sprintf("\t\tsqlQuery := \"DELETE FROM `%s` WHERE `ID` = ?\"", schema.tableName))
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQueryForTable(%s.tableName, sqlQuery, e.GetID())", names.providerName, names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\te.ctx.InvalidateCacheKey(%s.redisCode, %s.redisCachePrefix+strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}
	if schema.hasRedisSearch {
		g.addLine(fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisSearchCode).Del(%s.redisSearchPrefix + strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}
	// Cached unique index DELETE
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")
		idxNum := 0
		for _, idxName := range sortedCachedUniqueIndexNames(schema) {
			fIndexes := schema.uniqueIndexFIndexes[idxName]
			index := schema.uniqueIndexes[idxName]
			cols := make([]uniqueIndexColInfo, len(index.Columns))
			for i, colName := range index.Columns {
				cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
			}
			keyVar := fmt.Sprintf("_udKey%d", idxNum)
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t", keyVar, false)
			idxNum++
		}
	}
	g.addLine("\t\te.flushType = 3")
	g.addLine("\t\treturn nil")
	g.addLine("\t}")

	// UPDATE block
	g.addLine("\tif len(e.databaseBind) > 0 {")
	g.addLine(fmt.Sprintf("\t\tfor _, cb := range %sBeforeUpdateCallbacks { cb(e) }", names.entityPrivate))

	// Lifecycle callback: determine flush event type and build old-values map
	if schema.hasFakeDelete {
		g.addLine("\t\tif _fdv, _fdok := e.databaseBind[\"FakeDelete\"]; _fdok && _fdv.(uint64) != 0 {")
		g.addLine("\t\t\te.flushType = 3")
		g.addLine("\t\t} else {")
		g.addLine("\t\t\te.flushType = 2")
		g.addLine("\t\t\te.flushChanges = make(map[string]any, len(e.databaseBind))")
		g.addLine("\t\t\tfor _col := range e.databaseBind {")
		// Exclude auto-set fields from changes map
		excludeCheck := ""
		if schema.hasUpdatedAt {
			excludeCheck += `_col == "UpdatedAt"`
		}
		if schema.hasCreatedAt {
			if excludeCheck != "" {
				excludeCheck += " || "
			}
			excludeCheck += `_col == "CreatedAt"`
		}
		// Always exclude FakeDelete from changes when it's not a delete event
		if excludeCheck != "" {
			excludeCheck += " || "
		}
		excludeCheck += `_col == "FakeDelete"`
		g.addLine(fmt.Sprintf("\t\t\t\tif %s {", excludeCheck))
		g.addLine("\t\t\t\t\tcontinue")
		g.addLine("\t\t\t\t}")
		g.addLine("\t\t\t\te.flushChanges[_col] = e.privateGetOriginalColumnValue(_col)")
		g.addLine("\t\t\t}")
		g.addLine("\t\t}")
	} else {
		g.addLine("\t\te.flushType = 2")
		g.addLine("\t\te.flushChanges = make(map[string]any, len(e.databaseBind))")
		g.addLine("\t\tfor _col := range e.databaseBind {")
		excludeCheck := ""
		if schema.hasUpdatedAt {
			excludeCheck += `_col == "UpdatedAt"`
		}
		if schema.hasCreatedAt {
			if excludeCheck != "" {
				excludeCheck += " || "
			}
			excludeCheck += `_col == "CreatedAt"`
		}
		if excludeCheck != "" {
			g.addLine(fmt.Sprintf("\t\t\tif %s {", excludeCheck))
			g.addLine("\t\t\t\tcontinue")
			g.addLine("\t\t\t}")
		}
		g.addLine("\t\t\te.flushChanges[_col] = e.privateGetOriginalColumnValue(_col)")
		g.addLine("\t\t}")
	}

	if schema.hasUpdatedAt {
		g.addImport("time")
		g.addLine("\t\te.databaseBind[\"UpdatedAt\"] = time.Now().UTC().Truncate(time.Second)")
	}
	g.addLine(fmt.Sprintf("\t\tsqlQuery := \"UPDATE `%s` SET\" ", schema.tableName))
	g.addLine("\t\ti := 0")
	g.addLine("\t\tupdateParams := make([]any, len(e.databaseBind))")
	g.addLine("\t\tfor column, value := range e.databaseBind {")
	g.addLine("\t\t\tif i > 0 {")
	g.addLine("\t\t\t\tsqlQuery += \",\"")
	g.addLine("\t\t\t}")
	g.addLine("\t\t\tupdateParams[i] = value")
	g.addLine("\t\t\ti++")
	g.addLine("\t\t\tsqlQuery += \"`\" + column + \"`=?\"")
	g.addLine("\t\t}")
	g.addImport("strconv")
	g.addLine("\t\tsqlQuery += \" WHERE `ID`=\" + strconv.FormatUint(e.id, 10)")
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQueryForTable(%s.tableName, sqlQuery, updateParams...)", names.providerName, names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\te.ctx.InvalidateCacheKey(%s.redisCode, %s.redisCachePrefix+strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}

	// Redis Search UPDATE
	if schema.hasRedisSearch {
		searchFieldNames := make([]string, len(schema.searchableFields))
		for i, f := range schema.searchableFields {
			searchFieldNames[i] = fmt.Sprintf("%q", f.columnName)
		}
		cap := len(schema.searchableFields) * 2
		g.addLine(fmt.Sprintf("\t\t_searchKey2 := %s.redisSearchPrefix + strconv.FormatUint(e.GetID(), 10)", names.providerName))
		g.addLine(fmt.Sprintf("\t\t_sp2 := e.ctx.RedisPipeLine(%s.redisSearchCode)", names.providerName))
		hasNullable := false
		nullableCount := 0
		for _, f := range schema.searchableFields {
			if f.nullable {
				hasNullable = true
				nullableCount++
			}
		}
		if schema.hasFakeDelete {
			g.addLine("\t\t_doSearch := false")
			g.addLine("\t\t_doSearchFull := false")
			g.addLine("\t\tif _fdv, _fdok := e.databaseBind[\"FakeDelete\"]; _fdok {")
			g.addLine("\t\t\tif _fdv.(uint64) != 0 {")
			g.addLine("\t\t\t\t_sp2.Del(_searchKey2)")
			g.addLine("\t\t\t} else {")
			g.addLine("\t\t\t\t_doSearch = true")
			g.addLine("\t\t\t\t_doSearchFull = true")
			g.addLine("\t\t\t}")
			g.addLine("\t\t} else {")
			g.addLine(fmt.Sprintf("\t\t\tfor _, _sf := range []string{%s} {", strings.Join(searchFieldNames, ", ")))
			g.addLine("\t\t\t\tif _, _ok := e.databaseBind[_sf]; _ok {")
			g.addLine("\t\t\t\t\t_doSearch = true")
			g.addLine("\t\t\t\t\tbreak")
			g.addLine("\t\t\t\t}")
			g.addLine("\t\t\t}")
			g.addLine("\t\t}")
			g.addLine("\t\tif _doSearch {")
			g.addLine("\t\t\tif _doSearchFull {")
			g.addLine("\t\t\t\t_sp2.Del(_searchKey2)")
			g.addLine(fmt.Sprintf("\t\t\t\t_sa2 := make([]any, 0, %d)", cap))
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendFromDBBind(f, "\t\t\t\t", "_sa2")
			}
			g.addLine("\t\t\t\tif len(_sa2) > 0 {")
			g.addLine("\t\t\t\t\t_sp2.HSet(_searchKey2, _sa2...)")
			g.addLine("\t\t\t\t}")
			g.addLine("\t\t\t} else {")
			g.addLine(fmt.Sprintf("\t\t\t\t_sa2 := make([]any, 0, %d)", cap))
			if hasNullable {
				g.addLine(fmt.Sprintf("\t\t\t\t_sd2 := make([]string, 0, %d)", nullableCount))
			}
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendOnlyChanged(f, "\t\t\t\t", "_sa2", "_sd2")
			}
			g.addLine("\t\t\t\tif len(_sa2) > 0 {")
			g.addLine("\t\t\t\t\t_sp2.HSet(_searchKey2, _sa2...)")
			g.addLine("\t\t\t\t}")
			if hasNullable {
				g.addLine("\t\t\t\tif len(_sd2) > 0 {")
				g.addLine("\t\t\t\t\t_sp2.HDel(_searchKey2, _sd2...)")
				g.addLine("\t\t\t\t}")
			}
			g.addLine("\t\t\t}")
			g.addLine("\t\t}")
		} else {
			g.addLine(fmt.Sprintf("\t\t_doSearch2 := false"))
			g.addLine(fmt.Sprintf("\t\tfor _, _sf := range []string{%s} {", strings.Join(searchFieldNames, ", ")))
			g.addLine("\t\t\tif _, _ok := e.databaseBind[_sf]; _ok {")
			g.addLine("\t\t\t\t_doSearch2 = true")
			g.addLine("\t\t\t\tbreak")
			g.addLine("\t\t\t}")
			g.addLine("\t\t}")
			g.addLine("\t\tif _doSearch2 {")
			g.addLine(fmt.Sprintf("\t\t\t_sa2 := make([]any, 0, %d)", cap))
			if hasNullable {
				g.addLine(fmt.Sprintf("\t\t\t_sd2 := make([]string, 0, %d)", nullableCount))
			}
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendOnlyChanged(f, "\t\t\t", "_sa2", "_sd2")
			}
			g.addLine("\t\t\tif len(_sa2) > 0 {")
			g.addLine("\t\t\t\t_sp2.HSet(_searchKey2, _sa2...)")
			g.addLine("\t\t\t}")
			if hasNullable {
				g.addLine("\t\t\tif len(_sd2) > 0 {")
				g.addLine("\t\t\t\t_sp2.HDel(_searchKey2, _sd2...)")
				g.addLine("\t\t\t}")
			}
			g.addLine("\t\t}")
		}
	}

	// Cached unique index UPDATE
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")

		// FakeDelete handling: if entity has FakeDelete and it's set to true, delete all cached index keys
		if schema.hasFakeDelete {
			g.addLine("\t\tif _fdv, _fdok := e.databaseBind[\"FakeDelete\"]; _fdok && _fdv.(uint64) != 0 {")
			idxNum := 0
			for _, idxName := range sortedCachedUniqueIndexNames(schema) {
				fIndexes := schema.uniqueIndexFIndexes[idxName]
				index := schema.uniqueIndexes[idxName]
				cols := make([]uniqueIndexColInfo, len(index.Columns))
				for i, colName := range index.Columns {
					cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
				}
				keyVar := fmt.Sprintf("_ufKey%d", idxNum)
				g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t\t", keyVar, false)
				idxNum++
			}
			g.addLine("\t\t} else {")
		}

		updateIndent := "\t\t"
		if schema.hasFakeDelete {
			updateIndent = "\t\t\t"
		}

		idxNum := 0
		for _, idxName := range sortedCachedUniqueIndexNames(schema) {
			fIndexes := schema.uniqueIndexFIndexes[idxName]
			index := schema.uniqueIndexes[idxName]
			cols := make([]uniqueIndexColInfo, len(index.Columns))
			for i, colName := range index.Columns {
				cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
			}

			// Check if any column changed
			changedCheck := ""
			for _, c := range cols {
				if changedCheck != "" {
					changedCheck += " || "
				}
				changedCheck += fmt.Sprintf("_uChk%d_%s", idxNum, c.colName)
			}
			for _, c := range cols {
				g.addLine(fmt.Sprintf("%s_, _uChk%d_%s := e.databaseBind[%q]", updateIndent, idxNum, c.colName, c.colName))
			}
			g.addLine(fmt.Sprintf("%sif %s {", updateIndent, changedCheck))

			innerIndent := updateIndent + "\t"

			// Old key (from origin)
			oldKeyVar := fmt.Sprintf("_uOldKey%d", idxNum)
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, innerIndent, oldKeyVar, false)

			// New key (from bind with fallback to origin)
			hasNullableCol := false
			for _, c := range cols {
				if c.nullable {
					hasNullableCol = true
					break
				}
			}

			// Build new value variables (handle dual source: Redis or DB origin)
			for vi, c := range cols {
				if !c.nullable {
					if schema.hasRedisCache {
						g.addLine(fmt.Sprintf("%svar _uNewV%d_%d any", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%sif _bv, _ok := e.databaseBind[%q]; _ok {", innerIndent, c.colName))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = _bv", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%s} else if e.originRedisValues != nil {", innerIndent))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = e.originRedisValues[%d]", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s} else {", innerIndent))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = e.originDatabaseValues.F%d", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s}", innerIndent))
					} else {
						g.addLine(fmt.Sprintf("%s_uNewV%d_%d := any(e.originDatabaseValues.F%d)", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%sif _bv, _ok := e.databaseBind[%q]; _ok { _uNewV%d_%d = _bv }", innerIndent, c.colName, idxNum, vi))
					}
				} else {
					g.addImport("database/sql")
					if schema.hasRedisCache {
						g.addLine(fmt.Sprintf("%svar _uNewValid%d_%d bool", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%svar _uNewV%d_%d any", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%sif _bv, _ok := e.databaseBind[%q]; _ok {", innerIndent, c.colName))
						g.addLine(fmt.Sprintf("%s\t_bnv := _bv.(%s)", innerIndent, c.bindNullType))
						g.addLine(fmt.Sprintf("%s\t_uNewValid%d_%d = _bnv.Valid", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = _bnv%s", innerIndent, idxNum, vi, c.bindInnerField))
						g.addLine(fmt.Sprintf("%s} else if e.originRedisValues != nil {", innerIndent))
						g.addLine(fmt.Sprintf("%s\t_uNewValid%d_%d = e.originRedisValues[%d] != \"\"", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = e.originRedisValues[%d]", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s} else {", innerIndent))
						g.addLine(fmt.Sprintf("%s\t_uNewValid%d_%d = e.originDatabaseValues.F%d.Valid", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = e.originDatabaseValues.F%d%s", innerIndent, idxNum, vi, c.fIndex, c.bindInnerField))
						g.addLine(fmt.Sprintf("%s}", innerIndent))
					} else {
						g.addLine(fmt.Sprintf("%s_uNewValid%d_%d := e.originDatabaseValues.F%d.Valid", innerIndent, idxNum, vi, c.fIndex))
						g.addLine(fmt.Sprintf("%s_uNewV%d_%d := e.originDatabaseValues.F%d%s", innerIndent, idxNum, vi, c.fIndex, c.bindInnerField))
						g.addLine(fmt.Sprintf("%sif _bv, _ok := e.databaseBind[%q]; _ok {", innerIndent, c.colName))
						g.addLine(fmt.Sprintf("%s\t_bnv := _bv.(%s)", innerIndent, c.bindNullType))
						g.addLine(fmt.Sprintf("%s\t_uNewValid%d_%d = _bnv.Valid", innerIndent, idxNum, vi))
						g.addLine(fmt.Sprintf("%s\t_uNewV%d_%d = _bnv%s", innerIndent, idxNum, vi, c.bindInnerField))
						g.addLine(fmt.Sprintf("%s}", innerIndent))
					}
				}
			}

			// Compute new key hash
			newKeyVar := fmt.Sprintf("_uNewKey%d", idxNum)
			if hasNullableCol {
				validChecks := ""
				for vi, c := range cols {
					if c.nullable {
						if validChecks != "" {
							validChecks += " && "
						}
						validChecks += fmt.Sprintf("_uNewValid%d_%d", idxNum, vi)
					}
				}
				g.addLine(fmt.Sprintf("%sif %s {", innerIndent, validChecks))
				g.generateUniqueIndexHashFromVars(cols, idxNum, innerIndent+"\t", newKeyVar, names, idxName)
				g.addLine(fmt.Sprintf("%s\te.ctx.InvalidateCacheKey(%s.redisCode, %s)", innerIndent, names.providerName, newKeyVar))
				g.addLine(fmt.Sprintf("%s}", innerIndent))
			} else {
				g.generateUniqueIndexHashFromVars(cols, idxNum, innerIndent, newKeyVar, names, idxName)
				g.addLine(fmt.Sprintf("%se.ctx.InvalidateCacheKey(%s.redisCode, %s)", innerIndent, names.providerName, newKeyVar))
			}

			g.addLine(fmt.Sprintf("%s}", updateIndent))
			idxNum++
		}

		if schema.hasFakeDelete {
			g.addLine("\t\t}")
		}
	}

	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.generatePrivateFlushed(schema, names)
	g.addLine("")

	g.generatePrivateReload(schema, names)
	g.addLine("")

	// PrivateFlushEvent
	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlushEvent() (uint8, map[string]any) {", names.entityName))
	g.addLine("\treturn e.flushType, e.flushChanges")
	g.addLine("}")
	g.addLine("")

	// PrivateGetDatabaseBind
	g.addLine(fmt.Sprintf("func (e *%s) PrivateGetDatabaseBind() map[string]any {", names.entityName))
	g.addLine("\treturn e.databaseBind")
	g.addLine("}")
	g.addLine("")

	// Optional capabilities Save type-asserts for. Reflecting on the generated
	// struct would miss, because schemas are keyed by the source struct.
	g.addLine(fmt.Sprintf("func (e *%s) PrivateCacheIndex() string {", names.entityName))
	g.addLine(fmt.Sprintf("\treturn %s.cacheIndex", names.providerName))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateIsNew() bool {", names.entityName))
	g.addLine("\treturn e.new")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateContext() fluxaorm.Context {", names.entityName))
	g.addLine("\treturn e.ctx")
	g.addLine("}")
	g.addLine("")

	// privateGetOriginalColumnValue
	g.generatePrivateGetOriginalColumnValue(schema, names)
}

type colOriginInfo struct {
	colName  string
	fIndex   int
	category string // "uint64", "int64", "bool", "float64", "time", "string", "nullUint64", "nullInt64", "nullBool", "nullFloat64", "nullTime", "nullString"
}

func buildColOriginInfos(fields *tableFields, fIdx *int) []colOriginInfo {
	var cols []colOriginInfo
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			*fIdx++
			continue
		}
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "uint64"})
		*fIdx++
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.referencesRequired[k] {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "uint64"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullUint64"})
		}
		*fIdx++
	}
	for k, i := range fields.referencesMulti {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.referencesMultiRequired[k] {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "string"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		}
		*fIdx++
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "int64"})
		*fIdx++
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.prefix == "" && fields.fields[i].Name == "FakeDelete" {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "fakeDeleteBool"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "bool"})
		}
		*fIdx++
	}
	for _, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "float64"})
		*fIdx++
	}
	for _, i := range fields.times {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "time"})
		*fIdx++
	}
	for _, i := range fields.dates {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "time"})
		*fIdx++
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.stringsRequired[k] {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "string"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		}
		*fIdx++
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullUint64"})
		*fIdx++
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullInt64"})
		*fIdx++
	}
	for k, i := range fields.stringsEnums {
		fieldName := fields.prefix + fields.fields[i].Name
		d := fields.enums[k]
		if d.required {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "string"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		}
		*fIdx++
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		*fIdx++
	}
	for k, i := range fields.sliceStringsSets {
		fieldName := fields.prefix + fields.fields[i].Name
		d := fields.sets[k]
		if d.required {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "string"})
		} else {
			cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		}
		*fIdx++
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullBool"})
		*fIdx++
	}
	for _, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullFloat64"})
		*fIdx++
	}
	for _, i := range fields.timesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullTime"})
		*fIdx++
	}
	for _, i := range fields.datesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullTime"})
		*fIdx++
	}
	for _, i := range fields.jsonStructs {
		fieldName := fields.prefix + fields.fields[i].Name
		cols = append(cols, colOriginInfo{fieldName, *fIdx, "nullString"})
		*fIdx++
	}
	for _, subFields := range fields.structsFields {
		cols = append(cols, buildColOriginInfos(subFields, fIdx)...)
	}
	return cols
}

func (g *codeGenerator) generatePrivateFlushed(schema *entitySchema, names *entityNames) {
	fIdx := 0
	cols := buildColOriginInfos(schema.fields, &fIdx)

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlushed() {", names.entityName))
	g.addLine("\tif e.new {")
	g.addLine("\t\te.new = false")
	g.addLine("\t}")
	g.addLine("\tif e.databaseBind != nil {")
	g.addLine("\t\tfor _col, _v := range e.databaseBind {")
	g.addLine("\t\t\tswitch _col {")
	for _, c := range cols {
		g.addLine(fmt.Sprintf("\t\t\tcase %q:", c.colName))
		g.emitPrivateFlushedCase(schema, c)
	}
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\te.databaseBind = nil")
	g.addLine("\te.deleted = false")
	g.addLine("\te.flushType = 0")
	g.addLine("\te.flushChanges = nil")
	g.addLine("}")
}

// generatePrivateReload emits the in-place refresh Context.Reload drives. It
// reads MySQL directly - never the Redis row cache, which is what a caller
// reloading under a lock is trying not to trust.
func (g *codeGenerator) generatePrivateReload(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("func (e *%s) PrivateReload() (bool, error) {", names.entityName))
	g.appendToLine("\tquery := \"SELECT `ID`")
	for _, columnName := range schema.GetColumns()[1:] {
		g.appendToLine(",`" + columnName + "`")
	}
	g.addLine(fmt.Sprintf(" FROM `%s` WHERE `ID` = ? LIMIT 1\"", schema.tableName))
	g.addLine(fmt.Sprintf("\tsqlRow := &%s{}", names.sqlRowName))
	g.appendToLine(fmt.Sprintf("\tfound, err := e.ctx.DB(%s.dbCode).QueryRow(e.ctx, fluxaorm.NewWhere(query, e.id), &sqlRow.F0", names.providerName))
	for i := 1; i < len(schema.columnNames); i++ {
		g.appendToLine(fmt.Sprintf(", &sqlRow.F%d", i))
	}
	g.addLine(")")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn false, err")
	g.addLine("\t}")
	g.addLine("\tif !found {")
	g.addLine("\t\treturn false, nil")
	g.addLine("\t}")
	g.addLine("\te.originDatabaseValues = sqlRow")
	if schema.hasRedisCache {
		// A getter prefers originRedisValues, so a stale one would mask the fresh row.
		g.addLine("\te.originRedisValues = nil")
	}
	g.addLine("\te.deleted = false")
	g.addLine("\treturn true, nil")
	g.addLine("}")
}

func (g *codeGenerator) emitDbAssign(indent string, fIndex int, valueExpr string) {
	g.addLine(fmt.Sprintf("%sif e.originDatabaseValues != nil {", indent))
	g.addLine(fmt.Sprintf("%s\te.originDatabaseValues.F%d = %s", indent, fIndex, valueExpr))
	g.addLine(fmt.Sprintf("%s}", indent))
}

func (g *codeGenerator) emitPrivateFlushedCase(schema *entitySchema, c colOriginInfo) {
	indent := "\t\t\t\t"
	switch c.category {
	case "uint64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_uv := _v.(uint64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_uv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\te.originRedisValues[%d] = strconv.FormatUint(_uv, 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "int64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_iv := _v.(int64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_iv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\te.originRedisValues[%d] = strconv.FormatInt(_iv, 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "bool":
		g.addLine(fmt.Sprintf("%s_bv := _v.(bool)", indent))
		g.emitDbAssign(indent, c.fIndex, "_bv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _bv {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"1\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"0\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "fakeDeleteBool":
		g.addLine(fmt.Sprintf("%s_fv := _v.(uint64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_fv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _fv != 0 {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"1\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"0\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "float64":
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_fv := _v.(float64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_fv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\te.originRedisValues[%d] = strconv.FormatFloat(_fv, 'f', -1, 64)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "time":
		g.addImport("time")
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_tv := _v.(time.Time)", indent))
		g.emitDbAssign(indent, c.fIndex, "_tv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\te.originRedisValues[%d] = strconv.FormatInt(_tv.Unix(), 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "string":
		g.addLine(fmt.Sprintf("%s_sv := _v.(string)", indent))
		g.emitDbAssign(indent, c.fIndex, "_sv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\te.originRedisValues[%d] = _sv", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullUint64":
		g.addImport("database/sql")
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullInt64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _nv.Valid {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = strconv.FormatUint(uint64(_nv.Int64), 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullInt64":
		g.addImport("database/sql")
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullInt64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _nv.Valid {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = strconv.FormatInt(_nv.Int64, 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullBool":
		g.addImport("database/sql")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullBool)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tswitch {", indent))
			g.addLine(fmt.Sprintf("%s\tcase !_nv.Valid:", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\tcase _nv.Bool:", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"1\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\tdefault:", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"0\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullFloat64":
		g.addImport("database/sql")
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullFloat64)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _nv.Valid {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = strconv.FormatFloat(_nv.Float64, 'f', -1, 64)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullTime":
		g.addImport("database/sql")
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullTime)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _nv.Valid {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = strconv.FormatInt(_nv.Time.Unix(), 10)", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	case "nullString":
		g.addImport("database/sql")
		g.addLine(fmt.Sprintf("%s_nv := _v.(sql.NullString)", indent))
		g.emitDbAssign(indent, c.fIndex, "_nv")
		if schema.hasRedisCache {
			g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
			g.addLine(fmt.Sprintf("%s\tif _nv.Valid {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = _nv.String", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t} else {", indent))
			g.addLine(fmt.Sprintf("%s\t\te.originRedisValues[%d] = \"\"", indent, c.fIndex))
			g.addLine(fmt.Sprintf("%s\t}", indent))
			g.addLine(fmt.Sprintf("%s}", indent))
		}
	}
}

func (g *codeGenerator) generatePrivateGetOriginalColumnValue(schema *entitySchema, names *entityNames) {
	fIdx := 0
	cols := buildColOriginInfos(schema.fields, &fIdx)

	g.addLine(fmt.Sprintf("func (e *%s) privateGetOriginalColumnValue(column string) any {", names.entityName))
	g.addLine("\tswitch column {")
	for _, c := range cols {
		g.addLine(fmt.Sprintf("\tcase %q:", c.colName))
		switch c.category {
		case "uint64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "int64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "bool":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", c.fIndex))
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "fakeDeleteBool":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", c.fIndex))
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d != 0", c.fIndex))
		case "float64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "time":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn time.Unix(_v, 0).UTC()")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "string":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d]", c.fIndex))
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues.F%d", c.fIndex))
		case "nullUint64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn uint64(e.originDatabaseValues.F%d.Int64)", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		case "nullInt64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn e.originDatabaseValues.F%d.Int64", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		case "nullBool":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", c.fIndex))
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn e.originDatabaseValues.F%d.Bool", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		case "nullFloat64":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", c.fIndex))
				g.addLine("\t\t\treturn _v")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn e.originDatabaseValues.F%d.Float64", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		case "nullTime":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\t_v, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", c.fIndex))
				g.addLine("\t\t\treturn time.Unix(_v, 0).UTC()")
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn e.originDatabaseValues.F%d.Time", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		case "nullString":
			if schema.hasRedisCache {
				g.addLine(fmt.Sprintf("\t\tif e.originRedisValues != nil {"))
				g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", c.fIndex))
				g.addLine("\t\t\t\treturn nil")
				g.addLine("\t\t\t}")
				g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d]", c.fIndex))
				g.addLine("\t\t}")
			}
			g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.Valid {", c.fIndex))
			g.addLine(fmt.Sprintf("\t\t\treturn e.originDatabaseValues.F%d.String", c.fIndex))
			g.addLine("\t\t}")
			g.addLine("\t\treturn nil")
		}
	}
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")
}

// indexSegment derives an index's key segment from the same columns the generated code reads and
// writes, so the read and invalidation paths agree by construction.
func indexSegment(indexName string, cols []uniqueIndexColInfo) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.colName
	}

	return UniqueIndexKeySegment(indexName, names)
}
