package fluxaorm

import (
	"fmt"
	"strings"
)

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
	g.addLine("\t\treturn nil")
	g.addLine("\t}")

	// UPDATE block
	g.addLine("\tif len(e.databaseBind) > 0 {")
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
