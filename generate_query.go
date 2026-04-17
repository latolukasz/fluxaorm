package fluxaorm

import (
	"fmt"
	"sort"
)

func (g *codeGenerator) generateGetByID(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("func (p %s) GetByID(ctx fluxaorm.Context, id uint64) (entity *%s, found bool, err error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\tif cached := ctx.GetFromContextCache(p.cacheIndex, id); cached != nil {")
	g.addLine(fmt.Sprintf("\t\treturn cached.(*%s), true, nil", names.entityName))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tredisKey := p.redisCachePrefix + strconv.FormatUint(id, 10)")
		g.addLine("\tredisValues, err := ctx.Engine().Redis(p.redisCode).LRange(ctx, redisKey, 0, -1)")
		g.addLine("\tif err != nil {")
		g.addLine("\t\treturn nil, false, err")
		g.addLine("\t}")
		g.addLine("\tif len(redisValues) == 1 && redisValues[0] == \"\" {")
		g.addLine("\t\treturn nil, false, nil")
		g.addLine("\t}")
		g.addLine("\tif len(redisValues) > 1 && redisValues[0] == p.redisCacheStamp {")
		g.addLine(fmt.Sprintf("\t\te := &%s{ctx: ctx, id: id, originRedisValues: redisValues[1:]}", names.entityName))
		g.addLine("\t\tctx.SetInContextCache(p.cacheIndex, id, e)")
		g.addLine("\t\treturn e, true, nil")
		g.addLine("\t}")
	}
	g.appendToLine("\tquery := \"SELECT `ID`")
	for _, columnName := range schema.GetColumns()[1:] {
		g.appendToLine(",`" + columnName + "`")
	}
	g.addLine(fmt.Sprintf(" FROM `%s` WHERE `ID` = ? LIMIT 1\"", schema.tableName))
	g.addLine(fmt.Sprintf("\tsqlRow := &%s{}", names.sqlRowName))
	g.appendToLine(fmt.Sprintf("\tfound, err = ctx.Engine().DB(%s.dbCode).QueryRow(ctx, fluxaorm.NewWhere(query, id), &sqlRow.F0", names.providerName))
	for i := 1; i < len(schema.columnNames); i++ {
		g.appendToLine(fmt.Sprintf(", &sqlRow.F%d", i))
	}
	g.addLine(")")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine("\tif !found {")
	if schema.hasRedisCache {
		g.addLine("\t\tredisPipeline := ctx.RedisPipeLine(p.redisCode)")
		g.addLine("\t\tredisPipeline.Del(redisKey)")
		g.addLine("\t\tredisPipeline.RPush(redisKey, \"\")")
		g.addLine("\t\t_, err = redisPipeline.Exec(ctx)")
		g.addLine("\t\tif err != nil {")
		g.addLine("\t\t\treturn nil, false, err")
		g.addLine("\t\t}")
	}
	g.addLine("\t\treturn nil, false, nil")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\t_, err = ctx.Engine().Redis(p.redisCode).RPush(ctx, redisKey, sqlRow.redisValues()...)")
		g.addLine("\tif err != nil {")
		g.addLine("\t\treturn nil, false, err")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te := &%s{ctx: ctx, id: id, originDatabaseValues: sqlRow}", names.entityName))
	g.addLine("\tctx.SetInContextCache(p.cacheIndex, id, e)")
	g.addLine("\treturn e, true, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateMustGetByID(names *entityNames) {
	g.addImport("fmt")
	g.addLine(fmt.Sprintf("func (p %s) MustGetByID(ctx fluxaorm.Context, id uint64) (entity *%s, err error) {",
		names.providerNamePrivate, names.entityName))
	g.addLine("\tentity, found, err := p.GetByID(ctx, id)")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, err")
	g.addLine("\t}")
	g.addLine("\tif !found {")
	g.addLine(fmt.Sprintf("\t\tpanic(fmt.Sprintf(\"%s with id %%d not found\", id))", names.entityName))
	g.addLine("\t}")
	g.addLine("\treturn entity, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateGetByIDs(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) GetByIDs(ctx fluxaorm.Context, id ...uint64) ([]*%s, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\tif len(id) == 0 {")
	g.addLine("\t\treturn nil, nil")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tresultMap := make(map[uint64]*%s, len(id))", names.entityName))
	g.addLine("\tuniqueIDs := make([]uint64, 0, len(id))")
	g.addLine("\tfor _, v := range id {")
	g.addLine("\t\tif _, exists := resultMap[v]; !exists {")
	g.addLine("\t\t\tresultMap[v] = nil")
	g.addLine("\t\t\tuniqueIDs = append(uniqueIDs, v)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\tidsToFetch := make([]uint64, 0, len(uniqueIDs))")
	g.addLine("\tfor _, v := range uniqueIDs {")
	g.addLine("\t\tif cached := ctx.GetFromContextCache(p.cacheIndex, v); cached != nil {")
	g.addLine(fmt.Sprintf("\t\t\tresultMap[v] = cached.(*%s)", names.entityName))
	g.addLine("\t\t} else {")
	g.addLine("\t\t\tidsToFetch = append(idsToFetch, v)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	selectPrefix := "SELECT `ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		selectPrefix += ",`" + columnName + "`"
	}
	g.addLine("\tif len(idsToFetch) > 0 {")
	if schema.hasRedisCache {
		g.addLine("\t\tredisPipeline := ctx.RedisPipeLine(p.redisCode)")
		g.addLine("\t\tredisResults := make([]*fluxaorm.PipeLineSlice, len(idsToFetch))")
		g.addLine("\t\tfor i, v := range idsToFetch {")
		g.addLine("\t\t\tredisResults[i] = redisPipeline.LRange(p.redisCachePrefix+strconv.FormatUint(v, 10), 0, -1)")
		g.addLine("\t\t}")
		g.addLine("\t\t_, err := redisPipeline.Exec(ctx)")
		g.addLine("\t\tif err != nil {")
		g.addLine("\t\t\treturn nil, err")
		g.addLine("\t\t}")
		g.addLine("\t\tmissingIDs := make([]uint64, 0, len(idsToFetch))")
		g.addLine("\t\tfor i, v := range idsToFetch {")
		g.addLine("\t\t\tvalues, err := redisResults[i].Result()")
		g.addLine("\t\t\tif err != nil {")
		g.addLine("\t\t\t\treturn nil, err")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tif len(values) == 1 && values[0] == \"\" {")
		g.addLine("\t\t\t\tcontinue")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tif len(values) > 1 && values[0] == p.redisCacheStamp {")
		g.addLine(fmt.Sprintf("\t\t\t\te := &%s{ctx: ctx, id: v, originRedisValues: values[1:]}", names.entityName))
		g.addLine("\t\t\t\tresultMap[v] = e")
		g.addLine("\t\t\t\tctx.SetInContextCache(p.cacheIndex, v, e)")
		g.addLine("\t\t\t\tcontinue")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tmissingIDs = append(missingIDs, v)")
		g.addLine("\t\t}")
		g.addLine("\t\tif len(missingIDs) > 0 {")
		selectPrefixFull := selectPrefix + fmt.Sprintf(" FROM `%s` WHERE `ID` IN (", schema.tableName)
		g.addLine("\t\t\tvar b strings.Builder")
		g.addLine(fmt.Sprintf("\t\t\tb.WriteString(%q)", selectPrefixFull))
		g.addLine("\t\t\tb.WriteString(strconv.FormatUint(missingIDs[0], 10))")
		g.addLine("\t\t\tfor _, v := range missingIDs[1:] {")
		g.addLine("\t\t\t\tb.WriteByte(',')")
		g.addLine("\t\t\t\tb.WriteString(strconv.FormatUint(v, 10))")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tb.WriteByte(')')")
		g.addLine(fmt.Sprintf("\t\t\trows, cl, err := ctx.Engine().DB(%s.dbCode).Query(ctx, b.String())", names.providerName))
		g.addLine("\t\t\tif err != nil {")
		g.addLine("\t\t\t\treturn nil, err")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tdefer cl()")
		g.addLine("\t\t\tfoundInDB := make(map[uint64]bool, len(missingIDs))")
		g.addLine("\t\t\tcachePipeline := ctx.RedisPipeLine(p.redisCode)")
		g.addLine("\t\t\tfor rows.Next() {")
		g.addLine(fmt.Sprintf("\t\t\t\tsqlRow := &%s{}", names.sqlRowName))
		g.appendToLine("\t\t\t\terr = rows.Scan(&sqlRow.F0")
		for i := 1; i < len(schema.columnNames); i++ {
			g.appendToLine(fmt.Sprintf(", &sqlRow.F%d", i))
		}
		g.addLine(")")
		g.addLine("\t\t\t\tif err != nil {")
		g.addLine("\t\t\t\t\treturn nil, err")
		g.addLine("\t\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\t\te := &%s{ctx: ctx, id: sqlRow.F0, originDatabaseValues: sqlRow}", names.entityName))
		g.addLine("\t\t\t\tresultMap[sqlRow.F0] = e")
		g.addLine("\t\t\t\tfoundInDB[sqlRow.F0] = true")
		g.addLine("\t\t\t\tcachePipeline.RPush(p.redisCachePrefix+strconv.FormatUint(sqlRow.F0, 10), sqlRow.redisValues()...)")
		g.addLine("\t\t\t\tctx.SetInContextCache(p.cacheIndex, sqlRow.F0, e)")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tfor _, v := range missingIDs {")
		g.addLine("\t\t\t\tif !foundInDB[v] {")
		g.addLine("\t\t\t\t\tredisKey := p.redisCachePrefix + strconv.FormatUint(v, 10)")
		g.addLine("\t\t\t\t\tcachePipeline.Del(redisKey)")
		g.addLine("\t\t\t\t\tcachePipeline.RPush(redisKey, \"\")")
		g.addLine("\t\t\t\t}")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\t_, err = cachePipeline.Exec(ctx)")
		g.addLine("\t\t\tif err != nil {")
		g.addLine("\t\t\t\treturn nil, err")
		g.addLine("\t\t\t}")
		g.addLine("\t\t}")
	} else {
		selectPrefix += fmt.Sprintf(" FROM `%s` WHERE `ID` IN (", schema.tableName)
		g.addLine("\t\tvar b strings.Builder")
		g.addLine(fmt.Sprintf("\t\tb.WriteString(%q)", selectPrefix))
		g.addLine("\t\tb.WriteString(strconv.FormatUint(idsToFetch[0], 10))")
		g.addLine("\t\tfor _, v := range idsToFetch[1:] {")
		g.addLine("\t\t\tb.WriteByte(',')")
		g.addLine("\t\t\tb.WriteString(strconv.FormatUint(v, 10))")
		g.addLine("\t\t}")
		g.addLine("\t\tb.WriteByte(')')")
		g.addLine(fmt.Sprintf("\t\trows, cl, err := ctx.Engine().DB(%s.dbCode).Query(ctx, b.String())", names.providerName))
		g.addLine("\t\tif err != nil {")
		g.addLine("\t\t\treturn nil, err")
		g.addLine("\t\t}")
		g.addLine("\t\tdefer cl()")
		g.addLine("\t\tfor rows.Next() {")
		g.addLine(fmt.Sprintf("\t\t\tsqlRow := &%s{}", names.sqlRowName))
		g.appendToLine("\t\t\terr = rows.Scan(&sqlRow.F0")
		for i := 1; i < len(schema.columnNames); i++ {
			g.appendToLine(fmt.Sprintf(", &sqlRow.F%d", i))
		}
		g.addLine(")")
		g.addLine("\t\t\tif err != nil {")
		g.addLine("\t\t\t\treturn nil, err")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\te := &%s{ctx: ctx, id: sqlRow.F0, originDatabaseValues: sqlRow}", names.entityName))
		g.addLine("\t\t\tresultMap[sqlRow.F0] = e")
		g.addLine("\t\t\tctx.SetInContextCache(p.cacheIndex, sqlRow.F0, e)")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tresult := make([]*%s, 0, len(uniqueIDs))", names.entityName))
	g.addLine("\tfor _, v := range uniqueIDs {")
	g.addLine("\t\tif e := resultMap[v]; e != nil {")
	g.addLine("\t\t\tresult = append(result, e)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn result, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateNewMethods(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("func (p %s) New(ctx fluxaorm.Context) (*%s, error)  {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\tid, err := p.uuid(ctx)"))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\treturn nil, err\n\t}"))
	g.addLine(fmt.Sprintf("\treturn p.NewWithID(ctx, id), nil"))
	g.addLine("}")
	g.addLine("")
	defaults := collectRequiredEnumDefaults(schema.fields)
	sqlRowInit := "F0: id"
	for _, d := range defaults {
		sqlRowInit += fmt.Sprintf(", F%d: %q", d.fIndex, d.defaultValue)
	}
	g.addLine(fmt.Sprintf("func (p %s) NewWithID(ctx fluxaorm.Context, id uint64) *%s  {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\te := &%s{ctx: ctx, new: true, id: id, originDatabaseValues: &%s{%s}}", names.entityName, names.sqlRowName, sqlRowInit))
	g.addLine(fmt.Sprintf("\te.ctx.Track(e, %s.cacheIndex)", names.providerName))
	if schema.hasRedisCache {
		g.addImport("strconv")
	}
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
}

type enumDefault struct {
	fIndex       int
	defaultValue string
}

func collectRequiredEnumDefaults(fields *tableFields) []enumDefault {
	var results []enumDefault
	fIndex := 0
	collectRequiredEnumDefaultsRecursive(fields, &fIndex, &results)
	return results
}

func collectRequiredEnumDefaultsRecursive(fields *tableFields, fIndex *int, results *[]enumDefault) {
	for range fields.uIntegers {
		*fIndex++
	}
	for range fields.references {
		*fIndex++
	}
	for range fields.referencesMulti {
		*fIndex++
	}
	for range fields.integers {
		*fIndex++
	}
	for range fields.booleans {
		*fIndex++
	}
	for range fields.floats {
		*fIndex++
	}
	for range fields.times {
		*fIndex++
	}
	for range fields.dates {
		*fIndex++
	}
	for range fields.strings {
		*fIndex++
	}
	for range fields.uIntegersNullable {
		*fIndex++
	}
	for range fields.integersNullable {
		*fIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			*results = append(*results, enumDefault{fIndex: *fIndex, defaultValue: d.defaultValue})
		}
		*fIndex++
	}
	for range fields.bytes {
		*fIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		if d.required {
			*results = append(*results, enumDefault{fIndex: *fIndex, defaultValue: d.defaultValue})
		}
		*fIndex++
	}
	for range fields.booleansNullable {
		*fIndex++
	}
	for range fields.floatsNullable {
		*fIndex++
	}
	for range fields.timesNullable {
		*fIndex++
	}
	for range fields.datesNullable {
		*fIndex++
	}
	for _, subFields := range fields.structsFields {
		collectRequiredEnumDefaultsRecursive(subFields, fIndex, results)
	}
}

func (g *codeGenerator) generateSearchOne(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) SearchOne(ctx fluxaorm.Context, query *fluxaorm.DBQuery) (*%s, bool, error) {", names.providerNamePrivate, names.entityName))

	// Generate smart unique index detection blocks for cached unique indexes
	// Sort index names for deterministic output
	indexNames := make([]string, 0, len(schema.cachedUniqueIndexes))
	for indexName := range schema.cachedUniqueIndexes {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)

	// Group indexes by column count
	type cachedIndex struct {
		name    string
		columns []string
	}
	byColCount := make(map[int][]cachedIndex)
	for _, indexName := range indexNames {
		index, ok := schema.uniqueIndexes[indexName]
		if !ok {
			continue
		}
		colCount := len(index.Columns)
		byColCount[colCount] = append(byColCount[colCount], cachedIndex{name: indexName, columns: index.Columns})
	}

	if len(byColCount) > 0 {
		g.addImport("hash/fnv")
		g.addImport("fmt")
		g.addLine("\t_conditions := query.GetConditions()")
	}

	// Sort column counts for deterministic output
	colCounts := make([]int, 0, len(byColCount))
	for cc := range byColCount {
		colCounts = append(colCounts, cc)
	}
	sort.Ints(colCounts)

	for _, colCount := range colCounts {
		indexes := byColCount[colCount]
		if colCount == 1 {
			g.addLine(fmt.Sprintf("\tif len(_conditions) == %d {", colCount))
			for _, idx := range indexes {
				col := idx.columns[0]
				g.addLine(fmt.Sprintf("\t\tif _c0, _ok := _conditions[0].(fluxaorm.EqCondition); _ok && _c0.ColumnName() == %q {", col))
				g.addLine("\t\t\t_h := fnv.New32a()")
				g.addLine("\t\t\t_h.Write([]byte(fmt.Sprintf(\"%v\", _c0.EqValue())))")
				g.addLine(fmt.Sprintf("\t\t\t_redisKey := p.redisCachePrefix + \"u:%s:\" + strconv.FormatUint(uint64(_h.Sum32()), 10)", idx.name))
				// Redis GET
				g.addLine("\t\t\t_val, _has, _err := ctx.Engine().Redis(p.redisCode).Get(ctx, _redisKey)")
				g.addLine("\t\t\tif _err != nil {")
				g.addLine("\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t}")
				g.addLine("\t\t\tif _has {")
				g.addLine("\t\t\t\t_cachedID, _parseErr := strconv.ParseUint(_val, 10, 64)")
				g.addLine("\t\t\t\tif _parseErr != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _parseErr")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\treturn p.GetByID(ctx, _cachedID)")
				g.addLine("\t\t\t}")
				// MySQL fallback
				whereClause := fmt.Sprintf("`%s` = ?", col)
				if schema.hasFakeDelete {
					whereClause += " AND `FakeDelete` = 0"
				}
				g.addLine(fmt.Sprintf("\t\t\tvar _foundID uint64"))
				g.addLine(fmt.Sprintf("\t\t\t_found, _err := ctx.Engine().DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(\"SELECT `ID` FROM `%s` WHERE %s LIMIT 1\", _c0.EqValue()), &_foundID)", schema.tableName, whereClause))
				g.addLine("\t\t\tif _err != nil {")
				g.addLine("\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t}")
				g.addLine("\t\t\tif !_found {")
				g.addLine("\t\t\t\treturn nil, false, nil")
				g.addLine("\t\t\t}")
				// Cache in Redis
				g.addLine("\t\t\t_redisPipeline := ctx.RedisPipeLine(p.redisCode)")
				g.addLine("\t\t\t_redisPipeline.Set(_redisKey, strconv.FormatUint(_foundID, 10), 0)")
				g.addLine("\t\t\t_, _err = _redisPipeline.Exec(ctx)")
				g.addLine("\t\t\tif _err != nil {")
				g.addLine("\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t}")
				g.addLine("\t\t\treturn p.GetByID(ctx, _foundID)")
				g.addLine("\t\t}")
			}
			g.addLine("\t}")
		} else {
			g.addLine(fmt.Sprintf("\tif len(_conditions) == %d {", colCount))
			g.addLine(fmt.Sprintf("\t\t_cols := make(map[string]fluxaorm.EqCondition, %d)", colCount))
			g.addLine("\t\t_allEq := true")
			g.addLine("\t\tfor _, _c := range _conditions {")
			g.addLine("\t\t\tif _eq, _ok := _c.(fluxaorm.EqCondition); _ok {")
			g.addLine("\t\t\t\t_cols[_eq.ColumnName()] = _eq")
			g.addLine("\t\t\t} else {")
			g.addLine("\t\t\t\t_allEq = false")
			g.addLine("\t\t\t\tbreak")
			g.addLine("\t\t\t}")
			g.addLine("\t\t}")
			g.addLine("\t\tif _allEq {")
			for _, idx := range indexes {
				// Build variable declarations for each column
				varDecls := ""
				hasChecks := ""
				for i, col := range idx.columns {
					varName := fmt.Sprintf("_c%s", g.capitalizeFirst(col))
					hasName := fmt.Sprintf("_has%s", g.capitalizeFirst(col))
					if i > 0 {
						varDecls += "\n"
						hasChecks += " && "
					}
					varDecls += fmt.Sprintf("\t\t\t%s, %s := _cols[%q]", varName, hasName, col)
					hasChecks += hasName
				}
				g.body += varDecls + "\n"
				g.addLine(fmt.Sprintf("\t\t\tif %s {", hasChecks))
				// Build hash
				g.addLine("\t\t\t\t_h := fnv.New32a()")
				fmtStr := ""
				args := ""
				for i, col := range idx.columns {
					if i > 0 {
						fmtStr += "\\x00"
						args += ", "
					}
					fmtStr += "%v"
					args += fmt.Sprintf("_c%s.EqValue()", g.capitalizeFirst(col))
				}
				g.addLine(fmt.Sprintf("\t\t\t\t_h.Write([]byte(fmt.Sprintf(\"%s\", %s)))", fmtStr, args))
				g.addLine(fmt.Sprintf("\t\t\t\t_redisKey := p.redisCachePrefix + \"u:%s:\" + strconv.FormatUint(uint64(_h.Sum32()), 10)", idx.name))
				// Redis GET
				g.addLine("\t\t\t\t_val, _has, _err := ctx.Engine().Redis(p.redisCode).Get(ctx, _redisKey)")
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\tif _has {")
				g.addLine("\t\t\t\t\t_cachedID, _parseErr := strconv.ParseUint(_val, 10, 64)")
				g.addLine("\t\t\t\t\tif _parseErr != nil {")
				g.addLine("\t\t\t\t\t\treturn nil, false, _parseErr")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t\treturn p.GetByID(ctx, _cachedID)")
				g.addLine("\t\t\t\t}")
				// MySQL fallback
				whereClause := ""
				sqlArgs := ""
				for i, col := range idx.columns {
					if i > 0 {
						whereClause += " AND "
						sqlArgs += ", "
					}
					whereClause += fmt.Sprintf("`%s` = ?", col)
					sqlArgs += fmt.Sprintf("_c%s.EqValue()", g.capitalizeFirst(col))
				}
				if schema.hasFakeDelete {
					whereClause += " AND `FakeDelete` = 0"
				}
				g.addLine("\t\t\t\tvar _foundID uint64")
				g.addLine(fmt.Sprintf("\t\t\t\t_found, _err := ctx.Engine().DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(\"SELECT `ID` FROM `%s` WHERE %s LIMIT 1\", %s), &_foundID)", schema.tableName, whereClause, sqlArgs))
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\tif !_found {")
				g.addLine("\t\t\t\t\treturn nil, false, nil")
				g.addLine("\t\t\t\t}")
				// Cache in Redis
				g.addLine("\t\t\t\t_redisPipeline := ctx.RedisPipeLine(p.redisCode)")
				g.addLine("\t\t\t\t_redisPipeline.Set(_redisKey, strconv.FormatUint(_foundID, 10), 0)")
				g.addLine("\t\t\t\t_, _err = _redisPipeline.Exec(ctx)")
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\treturn p.GetByID(ctx, _foundID)")
				g.addLine("\t\t\t}")
			}
			g.addLine("\t\t}")
			g.addLine("\t}")
		}
	}

	// Default fallback: regular MySQL query
	g.addLine("\twhereSQL, params := query.BuildWhereClause()")
	g.addLine("\tvar b strings.Builder")
	g.addLine(fmt.Sprintf("\tb.WriteString(\"SELECT `ID` FROM `%s`\")", schema.tableName))
	g.addLine("\tif whereSQL != \"\" {")
	g.addLine("\t\tb.WriteString(\" WHERE \")")
	g.addLine("\t\tb.WriteString(whereSQL)")
	g.addLine("\t}")
	if schema.hasFakeDelete {
		g.addLine("\tif !query.IsWithFakeDeletes() {")
		g.addLine("\t\tif whereSQL != \"\" {")
		g.addLine("\t\t\tb.WriteString(\" AND `FakeDelete` = 0\")")
		g.addLine("\t\t} else {")
		g.addLine("\t\t\tb.WriteString(\" WHERE `FakeDelete` = 0\")")
		g.addLine("\t\t}")
		g.addLine("\t}")
	}
	g.addLine("\tb.WriteString(\" LIMIT 1\")")
	g.addLine("\tvar _id uint64")
	g.addLine(fmt.Sprintf("\t_found, _err := ctx.Engine().DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(b.String(), params...), &_id)"))
	g.addLine("\tif _err != nil {")
	g.addLine("\t\treturn nil, false, _err")
	g.addLine("\t}")
	g.addLine("\tif !_found {")
	g.addLine("\t\treturn nil, false, nil")
	g.addLine("\t}")
	g.addLine("\treturn p.GetByID(ctx, _id)")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateSearchMany(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addLine(fmt.Sprintf("func (p %s) SearchMany(ctx fluxaorm.Context, query *fluxaorm.DBQuery) ([]*%s, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\twhereSQL, params := query.BuildWhereClause()")
	g.addLine("\tvar b strings.Builder")
	g.addLine(fmt.Sprintf("\tb.WriteString(\"SELECT `ID` FROM `%s`\")", schema.tableName))
	g.addLine("\tif whereSQL != \"\" {")
	g.addLine("\t\tb.WriteString(\" WHERE \")")
	g.addLine("\t\tb.WriteString(whereSQL)")
	g.addLine("\t}")
	if schema.hasFakeDelete {
		g.addLine("\tif !query.IsWithFakeDeletes() {")
		g.addLine("\t\tif whereSQL != \"\" {")
		g.addLine("\t\t\tb.WriteString(\" AND `FakeDelete` = 0\")")
		g.addLine("\t\t} else {")
		g.addLine("\t\t\tb.WriteString(\" WHERE `FakeDelete` = 0\")")
		g.addLine("\t\t}")
		g.addLine("\t}")
	}
	g.addLine("\tif orderBy := query.BuildOrderClause(); orderBy != \"\" {")
	g.addLine("\t\tb.WriteByte(' ')")
	g.addLine("\t\tb.WriteString(orderBy)")
	g.addLine("\t}")
	g.addLine("\tif limit := query.BuildLimitClause(); limit != \"\" {")
	g.addLine("\t\tb.WriteByte(' ')")
	g.addLine("\t\tb.WriteString(limit)")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\trows, cl, err := ctx.Engine().DB(p.dbCode).Query(ctx, b.String(), params...)"))
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, err")
	g.addLine("\t}")
	g.addLine("\tdefer cl()")
	g.addLine("\tvar ids []uint64")
	g.addLine("\tfor rows.Next() {")
	g.addLine("\t\tvar id uint64")
	g.addLine("\t\tif err = rows.Scan(&id); err != nil {")
	g.addLine("\t\t\treturn nil, err")
	g.addLine("\t\t}")
	g.addLine("\t\tids = append(ids, id)")
	g.addLine("\t}")
	g.addLine("\treturn p.GetByIDs(ctx, ids...)")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateSearchManyWithTotal(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addLine(fmt.Sprintf("func (p %s) SearchManyWithTotal(ctx fluxaorm.Context, query *fluxaorm.DBQuery) ([]*%s, int, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\twhereSQL, params := query.BuildWhereClause()")
	g.addLine("\tvar whereClause string")
	g.addLine("\tif whereSQL != \"\" {")
	g.addLine("\t\twhereClause = \" WHERE \" + whereSQL")
	g.addLine("\t}")
	if schema.hasFakeDelete {
		g.addLine("\tif !query.IsWithFakeDeletes() {")
		g.addLine("\t\tif whereClause != \"\" {")
		g.addLine("\t\t\twhereClause += \" AND `FakeDelete` = 0\"")
		g.addLine("\t\t} else {")
		g.addLine("\t\t\twhereClause = \" WHERE `FakeDelete` = 0\"")
		g.addLine("\t\t}")
		g.addLine("\t}")
	}
	g.addLine("\tvar b strings.Builder")
	g.addLine(fmt.Sprintf("\tb.WriteString(\"SELECT COUNT(*) FROM `%s`\")", schema.tableName))
	g.addLine("\tb.WriteString(whereClause)")
	g.addLine("\tvar totalRows int")
	g.addLine(fmt.Sprintf("\t_, err := ctx.Engine().DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(b.String(), params...), &totalRows)"))
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, 0, err")
	g.addLine("\t}")
	g.addLine("\tif totalRows == 0 {")
	g.addLine("\t\treturn nil, 0, nil")
	g.addLine("\t}")
	g.addLine("\tb.Reset()")
	g.addLine(fmt.Sprintf("\tb.WriteString(\"SELECT `ID` FROM `%s`\")", schema.tableName))
	g.addLine("\tb.WriteString(whereClause)")
	g.addLine("\tif orderBy := query.BuildOrderClause(); orderBy != \"\" {")
	g.addLine("\t\tb.WriteByte(' ')")
	g.addLine("\t\tb.WriteString(orderBy)")
	g.addLine("\t}")
	g.addLine("\tif limit := query.BuildLimitClause(); limit != \"\" {")
	g.addLine("\t\tb.WriteByte(' ')")
	g.addLine("\t\tb.WriteString(limit)")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\trows, cl, err := ctx.Engine().DB(p.dbCode).Query(ctx, b.String(), params...)"))
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, 0, err")
	g.addLine("\t}")
	g.addLine("\tdefer cl()")
	g.addLine("\tvar ids []uint64")
	g.addLine("\tfor rows.Next() {")
	g.addLine("\t\tvar id uint64")
	g.addLine("\t\tif err = rows.Scan(&id); err != nil {")
	g.addLine("\t\t\treturn nil, 0, err")
	g.addLine("\t\t}")
	g.addLine("\t\tids = append(ids, id)")
	g.addLine("\t}")
	g.addLine("\tentities, err := p.GetByIDs(ctx, ids...)")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, 0, err")
	g.addLine("\t}")
	g.addLine("\treturn entities, totalRows, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateSearchOneInRedis(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) SearchOneInRedis(ctx fluxaorm.Context, query *fluxaorm.RedisSearchQuery) (*%s, bool, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\tresult, err := ctx.Engine().Redis(p.redisSearchCode).FTSearch(ctx, p.redisSearchIndex, query.BuildQueryString(), query.BuildSearchOptions(0, 1))")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine("\tif len(result.Docs) == 0 {")
	g.addLine("\t\treturn nil, false, nil")
	g.addLine("\t}")
	g.addLine("\tid, err := strconv.ParseUint(strings.TrimPrefix(result.Docs[0].ID, p.redisSearchPrefix), 10, 64)")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine("\tentities, err := p.GetByIDs(ctx, id)")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine("\tif len(entities) == 0 {")
	g.addLine("\t\treturn nil, false, nil")
	g.addLine("\t}")
	g.addLine("\treturn entities[0], true, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateSearchManyInRedis(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) SearchManyInRedis(ctx fluxaorm.Context, query *fluxaorm.RedisSearchQuery) ([]*%s, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\toffset, count := query.GetPagerOffsetCount()")
	g.addLine("\tresult, err := ctx.Engine().Redis(p.redisSearchCode).FTSearch(ctx, p.redisSearchIndex, query.BuildQueryString(), query.BuildSearchOptions(offset, count))")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, err")
	g.addLine("\t}")
	g.addLine("\tif len(result.Docs) == 0 {")
	g.addLine("\t\treturn nil, nil")
	g.addLine("\t}")
	g.addLine("\tids := make([]uint64, 0, len(result.Docs))")
	g.addLine("\tfor _, doc := range result.Docs {")
	g.addLine("\t\tid, err := strconv.ParseUint(strings.TrimPrefix(doc.ID, p.redisSearchPrefix), 10, 64)")
	g.addLine("\t\tif err != nil {")
	g.addLine("\t\t\treturn nil, err")
	g.addLine("\t\t}")
	g.addLine("\t\tids = append(ids, id)")
	g.addLine("\t}")
	g.addLine("\treturn p.GetByIDs(ctx, ids...)")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateSearchManyInRedisWithTotal(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) SearchManyInRedisWithTotal(ctx fluxaorm.Context, query *fluxaorm.RedisSearchQuery) ([]*%s, int, error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\tcountResult, err := ctx.Engine().Redis(p.redisSearchCode).FTSearch(ctx, p.redisSearchIndex, query.BuildQueryString(), query.BuildSearchOptions(0, 0))")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, 0, err")
	g.addLine("\t}")
	g.addLine("\ttotal := countResult.Total")
	g.addLine("\tif total == 0 {")
	g.addLine("\t\treturn nil, 0, nil")
	g.addLine("\t}")
	g.addLine("\toffset, count := query.GetPagerOffsetCount()")
	g.addLine("\tresult, err := ctx.Engine().Redis(p.redisSearchCode).FTSearch(ctx, p.redisSearchIndex, query.BuildQueryString(), query.BuildSearchOptions(offset, count))")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, total, err")
	g.addLine("\t}")
	g.addLine("\tif len(result.Docs) == 0 {")
	g.addLine("\t\treturn nil, total, nil")
	g.addLine("\t}")
	g.addLine("\tids := make([]uint64, 0, len(result.Docs))")
	g.addLine("\tfor _, doc := range result.Docs {")
	g.addLine("\t\tid, err := strconv.ParseUint(strings.TrimPrefix(doc.ID, p.redisSearchPrefix), 10, 64)")
	g.addLine("\t\tif err != nil {")
	g.addLine("\t\t\treturn nil, total, err")
	g.addLine("\t\t}")
	g.addLine("\t\tids = append(ids, id)")
	g.addLine("\t}")
	g.addLine("\tentities, err := p.GetByIDs(ctx, ids...)")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, total, err")
	g.addLine("\t}")
	g.addLine("\treturn entities, total, nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateReindexRedisSearch(schema *entitySchema, names *entityNames) {
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) ReindexRedisSearch(ctx fluxaorm.Context) error {", names.providerNamePrivate))

	// Step 1: delete all existing search hashes for this entity via Lua SCAN+UNLINK
	g.body += "\t_luaScript := `\n"
	g.body += "local cursor = '0'\n"
	g.body += "local deleted = 0\n"
	g.body += "repeat\n"
	g.body += "  local result = redis.call('SCAN', cursor, 'MATCH', KEYS[1], 'COUNT', 1000)\n"
	g.body += "  cursor = result[1]\n"
	g.body += "  local keys = result[2]\n"
	g.body += "  if #keys > 0 then\n"
	g.body += "    deleted = deleted + redis.call('UNLINK', unpack(keys))\n"
	g.body += "  end\n"
	g.body += "until cursor == '0'\n"
	g.body += "return deleted\n"
	g.body += "`\n"
	g.addLine("\t_, _err := ctx.Engine().Redis(p.redisSearchCode).Eval(ctx, _luaScript, []string{p.redisSearchPrefix + \"*\"})")
	g.addLine("\tif _err != nil {")
	g.addLine("\t\treturn _err")
	g.addLine("\t}")

	// Step 2: SELECT all columns from MySQL
	selectQuery := "\"SELECT `ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		selectQuery += ",`" + columnName + "`"
	}
	selectQuery += fmt.Sprintf(" FROM `%s`\"", schema.tableName)
	g.addLine(fmt.Sprintf("\t_rows, _cl, _err := ctx.Engine().DB(p.dbCode).Query(ctx, %s)", selectQuery))
	g.addLine("\tif _err != nil {")
	g.addLine("\t\treturn _err")
	g.addLine("\t}")
	g.addLine("\tdefer _cl()")

	// Step 3: pipeline and row loop
	g.addLine(fmt.Sprintf("\t_pipeline := ctx.RedisPipeLine(p.redisSearchCode)"))
	g.addLine("\t_batchSize := 0")

	// Find FakeDelete field index if applicable
	fdIndex := -1
	if schema.hasFakeDelete {
		for i, cn := range schema.columnNames {
			if cn == "FakeDelete" {
				fdIndex = i
				break
			}
		}
	}

	searchFieldCap := len(schema.searchableFields) * 2

	g.addLine("\tfor _rows.Next() {")
	g.addLine(fmt.Sprintf("\t\t_sqlRow := &%s{}", names.sqlRowName))
	g.appendToLine(fmt.Sprintf("\t\tif _err = _rows.Scan(&_sqlRow.F0"))
	for i := 1; i < len(schema.columnNames); i++ {
		g.appendToLine(fmt.Sprintf(", &_sqlRow.F%d", i))
	}
	g.addLine("); _err != nil {")
	g.addLine("\t\t\treturn _err")
	g.addLine("\t\t}")

	if fdIndex >= 0 {
		g.addLine(fmt.Sprintf("\t\tif _sqlRow.F%d != 0 {", fdIndex))
		g.addLine("\t\t\tcontinue")
		g.addLine("\t\t}")
	}

	g.addLine("\t\t_key := p.redisSearchPrefix + strconv.FormatUint(_sqlRow.F0, 10)")
	g.addLine("\t\t_pipeline.Del(_key)")
	g.addLine(fmt.Sprintf("\t\t_sa := make([]any, 0, %d)", searchFieldCap))
	for _, f := range schema.searchableFields {
		g.body += g.searchHSetAppendFromVar(f, "\t\t", "_sa", "_sqlRow")
	}
	g.addLine("\t\tif len(_sa) > 0 {")
	g.addLine("\t\t\t_pipeline.HSet(_key, _sa...)")
	g.addLine("\t\t}")
	g.addLine("\t\t_batchSize++")
	g.addLine("\t\tif _batchSize >= 1000 {")
	g.addLine(fmt.Sprintf("\t\t\tif _, _err = _pipeline.Exec(ctx); _err != nil {"))
	g.addLine("\t\t\t\treturn _err")
	g.addLine("\t\t\t}")
	g.addLine(fmt.Sprintf("\t\t\t_pipeline = ctx.RedisPipeLine(p.redisSearchCode)"))
	g.addLine("\t\t\t_batchSize = 0")
	g.addLine("\t\t}")
	g.addLine("\t}")

	g.addLine("\tif _batchSize > 0 {")
	g.addLine(fmt.Sprintf("\t\tif _, _err = _pipeline.Exec(ctx); _err != nil {"))
	g.addLine("\t\t\treturn _err")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")
}
