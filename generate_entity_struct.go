package fluxaorm

import (
	"fmt"
	"strings"
)

type uniqueIndexColInfo struct {
	colName        string
	fIndex         int
	nullable       bool
	bindNullType   string // e.g. "sql.NullInt64" — only for nullable
	bindInnerField string // e.g. ".Int64" — only for nullable
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

func (g *codeGenerator) generateUniqueIndexKeyFromOrigin(schema *entitySchema, names *entityNames, indexName string, cols []uniqueIndexColInfo, indent string, keyVar string, operation string, isInsert bool) {
	g.addImport("hash/fnv")
	g.addImport("fmt")
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
			if operation == "set" {
				g.addLine(fmt.Sprintf("%s\te.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", indent, names.providerName, keyVar))
			} else {
				g.addLine(fmt.Sprintf("%s\te.ctx.RedisPipeLine(%s.redisCode).Del(%s)", indent, names.providerName, keyVar))
			}
			g.addLine(fmt.Sprintf("%s}", indent))
		} else {
			g.generateUniqueIndexHashDualSource(cols, indent, keyVar, names, indexName)
			if operation == "set" {
				g.addLine(fmt.Sprintf("%se.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", indent, names.providerName, keyVar))
			} else {
				g.addLine(fmt.Sprintf("%se.ctx.RedisPipeLine(%s.redisCode).Del(%s)", indent, names.providerName, keyVar))
			}
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
			if operation == "set" {
				g.addLine(fmt.Sprintf("%s\te.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", indent, names.providerName, keyVar))
			} else {
				g.addLine(fmt.Sprintf("%s\te.ctx.RedisPipeLine(%s.redisCode).Del(%s)", indent, names.providerName, keyVar))
			}
			g.addLine(fmt.Sprintf("%s}", indent))
		} else {
			g.generateUniqueIndexHashFromDB(cols, indent, keyVar, names, indexName)
			if operation == "set" {
				g.addLine(fmt.Sprintf("%se.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", indent, names.providerName, keyVar))
			} else {
				g.addLine(fmt.Sprintf("%se.ctx.RedisPipeLine(%s.redisCode).Del(%s)", indent, names.providerName, keyVar))
			}
		}
	}
}

func (g *codeGenerator) generateUniqueIndexHashDualSource(cols []uniqueIndexColInfo, indent string, keyVar string, names *entityNames, indexName string) {
	g.addLine(fmt.Sprintf("%s%s_h := fnv.New32a()", indent, keyVar))
	// Build fmt args from both sources
	fmtStr := ""
	redisArgs := ""
	dbArgs := ""
	for i, c := range cols {
		if i > 0 {
			fmtStr += "\\x00"
			redisArgs += ", "
			dbArgs += ", "
		}
		fmtStr += "%v"
		redisArgs += fmt.Sprintf("e.originRedisValues[%d]", c.fIndex)
		if c.nullable {
			dbArgs += fmt.Sprintf("e.originDatabaseValues.F%d%s", c.fIndex, c.bindInnerField)
		} else {
			dbArgs += fmt.Sprintf("e.originDatabaseValues.F%d", c.fIndex)
		}
	}
	g.addLine(fmt.Sprintf("%sif e.originRedisValues != nil {", indent))
	g.addLine(fmt.Sprintf("%s\t%s_h.Write([]byte(fmt.Sprintf(\"%s\", %s)))", indent, keyVar, fmtStr, redisArgs))
	g.addLine(fmt.Sprintf("%s} else {", indent))
	g.addLine(fmt.Sprintf("%s\t%s_h.Write([]byte(fmt.Sprintf(\"%s\", %s)))", indent, keyVar, fmtStr, dbArgs))
	g.addLine(fmt.Sprintf("%s}", indent))
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + \"u:%s:\" + strconv.FormatUint(uint64(%s_h.Sum32()), 10)", indent, keyVar, names.providerName, indexName, keyVar))
}

func (g *codeGenerator) generateUniqueIndexHashFromDB(cols []uniqueIndexColInfo, indent string, keyVar string, names *entityNames, indexName string) {
	g.addLine(fmt.Sprintf("%s%s_h := fnv.New32a()", indent, keyVar))
	fmtStr := ""
	args := ""
	for i, c := range cols {
		if i > 0 {
			fmtStr += "\\x00"
			args += ", "
		}
		fmtStr += "%v"
		if c.nullable {
			args += fmt.Sprintf("e.originDatabaseValues.F%d%s", c.fIndex, c.bindInnerField)
		} else {
			args += fmt.Sprintf("e.originDatabaseValues.F%d", c.fIndex)
		}
	}
	g.addLine(fmt.Sprintf("%s%s_h.Write([]byte(fmt.Sprintf(\"%s\", %s)))", indent, keyVar, fmtStr, args))
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + \"u:%s:\" + strconv.FormatUint(uint64(%s_h.Sum32()), 10)", indent, keyVar, names.providerName, indexName, keyVar))
}

func (g *codeGenerator) generateUniqueIndexHashFromVars(cols []uniqueIndexColInfo, idxNum int, indent string, keyVar string, names *entityNames, indexName string) {
	g.addLine(fmt.Sprintf("%s%s_h := fnv.New32a()", indent, keyVar))
	fmtStr := ""
	args := ""
	for i := range cols {
		if i > 0 {
			fmtStr += "\\x00"
			args += ", "
		}
		fmtStr += "%v"
		args += fmt.Sprintf("_uNewV%d_%d", idxNum, i)
	}
	g.addLine(fmt.Sprintf("%s%s_h.Write([]byte(fmt.Sprintf(\"%s\", %s)))", indent, keyVar, fmtStr, args))
	g.addLine(fmt.Sprintf("%s%s := %s.redisCachePrefix + \"u:%s:\" + strconv.FormatUint(uint64(%s_h.Sum32()), 10)", indent, keyVar, names.providerName, indexName, keyVar))
}

func (g *codeGenerator) generateUUID(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("func (p %s) uuid(ctx fluxaorm.Context) uint64 {", names.providerNamePrivate))
	g.addLine(fmt.Sprintf("\tr := ctx.Engine().Redis(p.redisCode)"))
	g.addLine(fmt.Sprintf("\tid, err := r.Incr(ctx, \"%s\")", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif id == 1 {"))
	g.addLine(fmt.Sprintf("\t\tp.initUUID(ctx)"))
	g.addLine(fmt.Sprintf("\t\treturn p.uuid(ctx)"))
	g.addLine(fmt.Sprintf("\t}"))
	g.addLine("\treturn uint64(id)")
	g.addLine("}")
	g.addLine("")

	g.addImport("time")
	g.addImport("errors")
	g.addLine(fmt.Sprintf("func (p %s) initUUID(ctx fluxaorm.Context) {", names.providerNamePrivate))
	g.addLine(fmt.Sprintf("\tr := ctx.Engine().Redis(p.redisCode)"))
	g.addLine(fmt.Sprintf("\t%s.uuidRedisKeyMutex.Lock()", names.providerName))
	g.addLine(fmt.Sprintf("\tdefer %s.uuidRedisKeyMutex.Unlock()", names.providerName))
	g.addLine(fmt.Sprintf("\tnow, has, err := r.Get(ctx, \"%s\")", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif has && now != \"1\" {\n\t\treturn\n\t}"))
	g.addLine(fmt.Sprintf("\tlockName := \"%s:lock\"", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tlock, obtained, err := r.GetLocker().Obtain(ctx, lockName, time.Minute, time.Second*5)"))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif !obtained {\n\t\tpanic(errors.New(\"uuid lock timeout\"))\n\t}"))
	g.addLine(fmt.Sprintf("\tdefer lock.Release(ctx)"))
	g.addLine(fmt.Sprintf("\tnow, has, err = r.Get(ctx, \"%s\")", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif has && now != \"1\" {\n\t\treturn\n\t}"))
	g.addLine(fmt.Sprintf("\tmaxID := int64(0)"))
	g.addLine(fmt.Sprintf("\t_, err = ctx.Engine().DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(\"SELECT IFNULL(MAX(ID), 0) FROM `%s`\"), &maxID)", schema.tableName))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif maxID == 0 {\n\t\tmaxID = 1\n\t}"))
	g.addLine(fmt.Sprintf("\t_, err = r.IncrBy(ctx, \"%s\", maxID)", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine("}")
	g.addLine("")
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
		g.addLine("\tredisBind map[int64]any")
		g.addLine("\toriginRedisValues []string")
	}
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) GetID() uint64 {", names.entityName))
	g.addLine("\treturn e.id")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Delete() {", names.entityName))
	if schema.hasFakeDelete {
		g.addLine("\te.SetFakeDelete(true)")
	} else {
		g.addLine("\te.deleted = true")
	}
	g.addLine(fmt.Sprintf("\te.ctx.Track(e, %s.cacheIndex)", names.providerName))
	g.addLine("}")
	g.addLine("")

	if schema.hasFakeDelete {
		g.addLine(fmt.Sprintf("func (e *%s) ForceDelete() {", names.entityName))
		g.addLine("\te.deleted = true")
		g.addLine(fmt.Sprintf("\te.ctx.Track(e, %s.cacheIndex)", names.providerName))
		g.addLine("}")
		g.addLine("")
	}

	g.addLine(fmt.Sprintf("func (e *%s) addToDatabaseBind(column string, value any) {", names.entityName))
	g.addLine("\tif e.databaseBind == nil {")
	g.addLine("\t\te.databaseBind = map[string]any{}")
	g.addLine(fmt.Sprintf("\t\te.ctx.Track(e, %s.cacheIndex)", names.providerName))
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\te.databaseBind[column] = value"))
	g.addLine("}")
	g.addLine("")

	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("func (e *%s) addToRedisBind(index int64, value any) {", names.entityName))
		g.addLine("\tif e.redisBind == nil {")
		g.addLine("\t\te.redisBind = make(map[int64]any)")
		g.addLine("\t}")
		g.addLine(fmt.Sprintf("\te.redisBind[index] = value"))
		g.addLine("}")
		g.addLine("")
	}

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
	insertQueryLine += fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, e.originDatabaseValues.F0", names.providerName)
	for i := 1; i < len(schema.columnNames); i++ {
		insertQueryLine += fmt.Sprintf(", e.originDatabaseValues.F%d", i)
	}
	insertQueryLine += ")\n"
	if schema.hasRedisCache {
		insertQueryLine += fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisCode).RPush(%s.redisCachePrefix+strconv.FormatUint(e.GetID(), 10), e.originDatabaseValues.redisValues()...)", names.providerName, names.providerName)
	}
	g.addLine(insertQueryLine)

	// Cached unique index INSERT
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")
		idxNum := 0
		for idxName, isCached := range schema.cachedUniqueIndexes {
			if !isCached {
				continue
			}
			fIndexes := schema.uniqueIndexFIndexes[idxName]
			index := schema.uniqueIndexes[idxName]
			cols := make([]uniqueIndexColInfo, len(index.Columns))
			for i, colName := range index.Columns {
				cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
			}
			keyVar := fmt.Sprintf("_uKey%d", idxNum)
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t", keyVar, "set", true)
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
			g.addLine(fmt.Sprintf("\t\tif !e.originDatabaseValues.F%d {", fdIndex))
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

	g.addLine("\t\treturn nil")
	g.addLine("\t}")

	// DELETE block
	g.addLine("\tif e.deleted {")
	g.addLine(fmt.Sprintf("\t\tsqlQuery := \"DELETE FROM `%s` WHERE `ID` = ?\"", schema.tableName))
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, e.GetID())", names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisCode).Del(%s.redisCachePrefix + strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}
	if schema.hasRedisSearch {
		g.addLine(fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisSearchCode).Del(%s.redisSearchPrefix + strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}
	// Cached unique index DELETE
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")
		idxNum := 0
		for idxName, isCached := range schema.cachedUniqueIndexes {
			if !isCached {
				continue
			}
			fIndexes := schema.uniqueIndexFIndexes[idxName]
			index := schema.uniqueIndexes[idxName]
			cols := make([]uniqueIndexColInfo, len(index.Columns))
			for i, colName := range index.Columns {
				cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
			}
			keyVar := fmt.Sprintf("_udKey%d", idxNum)
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t", keyVar, "del", false)
			idxNum++
		}
	}
	g.addLine("\t\treturn nil")
	g.addLine("\t}")

	// UPDATE block
	g.addLine("\tif len(e.databaseBind) > 0 {")
	if schema.hasUpdatedAt {
		g.addImport("time")
		g.addLine("\t\te.databaseBind[\"UpdatedAt\"] = time.Now().UTC().Truncate(time.Second)")
		if schema.hasRedisCache {
			g.addLine("\t\tif e.redisBind == nil { e.redisBind = make(map[int64]any) }")
			g.addLine(fmt.Sprintf("\t\te.redisBind[%d] = e.databaseBind[\"UpdatedAt\"].(time.Time).Unix()", schema.updatedAtFIndex+1))
		}
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
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, updateParams...)", names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\tredisPipeLine := e.ctx.RedisPipeLine(%s.redisCode)", names.providerName))
		g.addLine(fmt.Sprintf("\t\tredisKey := %s.redisCachePrefix + strconv.FormatUint(e.GetID(), 10)", names.providerName))
		g.addLine("\t\tfor index, value := range e.redisBind {")
		g.addLine("\t\t\tredisPipeLine.LSet(redisKey, index, value)")
		g.addLine("\t\t}")
		g.addLine("\t\te.redisBind = nil")
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
		if schema.hasFakeDelete {
			g.addLine("\t\t_doSearch := false")
			g.addLine("\t\tif _fdv, _fdok := e.databaseBind[\"FakeDelete\"]; _fdok {")
			g.addLine("\t\t\tif _fdv.(bool) {")
			g.addLine("\t\t\t\t_sp2.Del(_searchKey2)")
			g.addLine("\t\t\t} else {")
			g.addLine("\t\t\t\t_doSearch = true")
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
			g.addLine("\t\t\t_sp2.Del(_searchKey2)")
			g.addLine(fmt.Sprintf("\t\t\t_sa2 := make([]any, 0, %d)", cap))
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendFromDBBind(f, "\t\t\t", "_sa2")
			}
			g.addLine("\t\t\tif len(_sa2) > 0 {")
			g.addLine("\t\t\t\t_sp2.HSet(_searchKey2, _sa2...)")
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
			g.addLine("\t\t\t_sp2.Del(_searchKey2)")
			g.addLine(fmt.Sprintf("\t\t\t_sa2 := make([]any, 0, %d)", cap))
			for _, f := range schema.searchableFields {
				g.body += g.searchHSetAppendFromDBBind(f, "\t\t\t", "_sa2")
			}
			g.addLine("\t\t\tif len(_sa2) > 0 {")
			g.addLine("\t\t\t\t_sp2.HSet(_searchKey2, _sa2...)")
			g.addLine("\t\t\t}")
			g.addLine("\t\t}")
		}
	}

	// Cached unique index UPDATE
	if schema.hasCachedUniqueIndexes {
		g.addImport("strconv")
		g.addImport("hash/fnv")
		g.addImport("fmt")

		// FakeDelete handling: if entity has FakeDelete and it's set to true, delete all cached index keys
		if schema.hasFakeDelete {
			g.addLine("\t\tif _fdv, _fdok := e.databaseBind[\"FakeDelete\"]; _fdok && _fdv.(bool) {")
			idxNum := 0
			for idxName, isCached := range schema.cachedUniqueIndexes {
				if !isCached {
					continue
				}
				fIndexes := schema.uniqueIndexFIndexes[idxName]
				index := schema.uniqueIndexes[idxName]
				cols := make([]uniqueIndexColInfo, len(index.Columns))
				for i, colName := range index.Columns {
					cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
				}
				keyVar := fmt.Sprintf("_ufKey%d", idxNum)
				g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, "\t\t\t", keyVar, "del", false)
				idxNum++
			}
			g.addLine("\t\t} else {")
		}

		updateIndent := "\t\t"
		if schema.hasFakeDelete {
			updateIndent = "\t\t\t"
		}

		idxNum := 0
		for idxName, isCached := range schema.cachedUniqueIndexes {
			if !isCached {
				continue
			}
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
			g.generateUniqueIndexKeyFromOrigin(schema, names, idxName, cols, innerIndent, oldKeyVar, "del", false)

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
				g.addLine(fmt.Sprintf("%s\te.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", innerIndent, names.providerName, newKeyVar))
				g.addLine(fmt.Sprintf("%s}", innerIndent))
			} else {
				g.generateUniqueIndexHashFromVars(cols, idxNum, innerIndent, newKeyVar, names, idxName)
				g.addLine(fmt.Sprintf("%se.ctx.RedisPipeLine(%s.redisCode).Set(%s, strconv.FormatUint(e.GetID(), 10), 0)", innerIndent, names.providerName, newKeyVar))
			}

			g.addLine(fmt.Sprintf("%s}", updateIndent))
			idxNum++
		}

		if schema.hasFakeDelete {
			g.addLine("\t\t}")
		}
	}

	g.addLine("\t\te.databaseBind = nil")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlushed() {", names.entityName))
	g.addLine("\tif e.new {")
	g.addLine("\t\te.new = false")
	g.addLine("\t}")
	g.addLine("}")
	g.addLine("")
}
