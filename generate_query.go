package fluxaorm

import (
	"fmt"
	"sort"
)

func (g *codeGenerator) generateGetByID(schema *entitySchema, names *entityNames) {
	g.addImport("strconv")
	useLiteralID := true
	for _, field := range schema.fieldDefinitions {
		_, decimal := field.Tags["decimal"]
		if !decimal && (field.TypeName == "float32" || field.TypeName == "*float32") {
			// MySQL's text protocol rounds FLOAT columns more than binary reads.
			useLiteralID = false
			break
		}
	}
	g.addLine(fmt.Sprintf("func (p %s) GetByID(ctx fluxaorm.Context, id uint64) (entity *%s, found bool, err error) {", names.providerNamePrivate, names.entityName))
	g.addLine("\tif cached := ctx.GetFromContextCache(p.cacheIndex, id); cached != nil {")
	g.addLine(fmt.Sprintf("\t\treturn cached.(*%s), true, nil", names.entityName))
	g.addLine("\t}")
	if useLiteralID || schema.hasRedisCache {
		g.addLine("\tvar idBuffer [20]byte")
		g.addLine("\tidDigits := strconv.AppendUint(idBuffer[:0], id, 10)")
	}
	if schema.hasRedisCache {
		g.addLine("\tredisKey := p.redisCachePrefix + string(idDigits)")
		g.addLine("\t_useCache := !ctx.InTransaction()")
		g.addLine("\tif _useCache {")
		g.addLine("\t\tredisValues, err := ctx.Engine().Redis(p.redisCode).LRange(ctx, redisKey, 0, -1)")
		g.addLine("\t\tif err != nil {")
		g.addLine("\t\t\treturn nil, false, err")
		g.addLine("\t\t}")
		g.addLine("\t\tif len(redisValues) == 1 && redisValues[0] == \"\" {")
		g.addLine("\t\t\treturn nil, false, nil")
		g.addLine("\t\t}")
		g.addLine("\t\tif len(redisValues) > 1 && redisValues[0] == p.redisCacheStamp {")
		g.addLine(fmt.Sprintf("\t\t\te := &%s{ctx: ctx, id: id, originRedisValues: redisValues[1:]}", names.entityName))
		g.addLine("\t\t\tctx.SetInContextCache(p.cacheIndex, id, e)")
		g.addLine("\t\t\treturn e, true, nil")
		g.addLine("\t\t}")
		g.addLine("\t}")
	}
	selectPrefix := "SELECT `ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		selectPrefix += ",`" + columnName + "`"
	}
	selectPrefix += fmt.Sprintf(" FROM `%s` WHERE `ID` = ", schema.tableName)
	where := "fluxaorm.NewWhere(query)"
	if useLiteralID {
		g.addLine(fmt.Sprintf("\tquery := %q + string(idDigits) + \" LIMIT 1\"", selectPrefix))
	} else {
		where = fmt.Sprintf("fluxaorm.NewWhere(%q, id)", selectPrefix+"? LIMIT 1")
	}
	g.addLine(fmt.Sprintf("\tsqlRow := &%s{}", names.sqlRowName))
	g.appendToLine(fmt.Sprintf("\tfound, err = ctx.DB(%s.dbCode).QueryRow(ctx, %s, fluxaorm.SQLScanTarget(&sqlRow.F0)", names.providerName, where))
	for i := 1; i < len(schema.columnNames); i++ {
		g.appendToLine(fmt.Sprintf(", fluxaorm.SQLScanTarget(&sqlRow.F%d)", i))
	}
	g.addLine(")")
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine("\tif !found {")
	if schema.hasRedisCache {
		g.addLine("\t\tif _useCache {")
		g.addLine("\t\t\t_, err = ctx.Engine().Redis(p.redisCode).Eval(ctx, fluxaorm.RowCacheRewriteScript,")
		g.addLine("\t\t\t\t[]string{redisKey}, int(fluxaorm.EntityCacheTTL.Seconds()), \"\")")
		g.addLine("\t\t\tif err != nil {")
		g.addLine("\t\t\t\treturn nil, false, err")
		g.addLine("\t\t\t}")
		g.addLine("\t\t}")
	}
	g.addLine("\t\treturn nil, false, nil")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tif _useCache {")
		g.addLine("\t\t_evalArgs := append([]any{int(fluxaorm.EntityCacheTTL.Seconds())}, sqlRow.redisValues()...)")
		g.addLine("\t\t_, err = ctx.Engine().Redis(p.redisCode).Eval(ctx, fluxaorm.RowCacheRewriteScript, []string{redisKey}, _evalArgs...)")
		g.addLine("\t\tif err != nil {")
		g.addLine("\t\t\treturn nil, false, err")
		g.addLine("\t\t}")
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
	g.addLine("\tif len(id) == 0 { return nil, nil }")
	g.addLine("\t// Small batches need no heap scratch; large batches retain linear lookup cost.")
	g.addLine("\tvar idScratch [32]uint64")
	g.addLine("\tvar pendingScratch [32]int")
	g.addLine("\tuniqueIDs := idScratch[:0]")
	g.addLine("\tpending := pendingScratch[:0]")
	g.addLine("\tvar positions map[uint64]int")
	g.addLine("\tif len(id) > len(idScratch) {")
	g.addLine("\t\tuniqueIDs = make([]uint64, 0, len(id))")
	g.addLine("\t\tpending = make([]int, 0, len(id))")
	g.addLine("\t\tpositions = make(map[uint64]int, len(id))")
	g.addLine("\t}")
	g.addLine("\tpositionOf := func(v uint64) int {")
	g.addLine("\t\tif positions != nil {")
	g.addLine("\t\t\tif i, ok := positions[v]; ok { return i }")
	g.addLine("\t\t\treturn -1")
	g.addLine("\t\t}")
	g.addLine("\t\tfor i, existing := range uniqueIDs { if existing == v { return i } }")
	g.addLine("\t\treturn -1")
	g.addLine("\t}")
	g.addLine("\tfor _, v := range id {")
	g.addLine("\t\tif positionOf(v) >= 0 { continue }")
	g.addLine("\t\tpos := len(uniqueIDs)")
	g.addLine("\t\tuniqueIDs = append(uniqueIDs, v)")
	g.addLine("\t\tif positions != nil { positions[v] = pos }")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tresult := make([]*%s, len(uniqueIDs))", names.entityName))
	g.addLine("\tfor pos, v := range uniqueIDs {")
	g.addLine("\t\tif cached := ctx.GetFromContextCache(p.cacheIndex, v); cached != nil {")
	g.addLine(fmt.Sprintf("\t\t\tresult[pos] = cached.(*%s)", names.entityName))
	g.addLine("\t\t} else {")
	g.addLine("\t\t\tpending = append(pending, pos)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tuseCache := !ctx.InTransaction()")
		g.addLine("\tif len(pending) > 0 && useCache {")
		g.addLine("\t\tredisPipeline := ctx.RedisPipeLine(p.redisCode)")
		g.addLine("\t\tvar resultScratch [32]fluxaorm.PipeLineSlice")
		g.addLine("\t\tvar keyScratch [32]string")
		g.addLine("\t\tredisResults := resultScratch[:]")
		g.addLine("\t\tkeys := keyScratch[:]")
		g.addLine("\t\tif len(pending) > len(resultScratch) {")
		g.addLine("\t\t\tredisResults = make([]fluxaorm.PipeLineSlice, len(pending))")
		g.addLine("\t\t\tkeys = make([]string, len(pending))")
		g.addLine("\t\t}")
		g.addLine("\t\tredisResults = redisResults[:len(pending)]")
		g.addLine("\t\tkeys = keys[:len(pending)]")
		g.addLine("\t\tvar idBuffer [20]byte")
		g.addLine("\t\tfor i, pos := range pending {")
		g.addLine("\t\t\tkeys[i] = p.redisCachePrefix + string(strconv.AppendUint(idBuffer[:0], uniqueIDs[pos], 10))")
		g.addLine("\t\t}")
		g.addLine("\t\tredisPipeline.LRangeBatchInto(redisResults, keys, 0, -1)")
		g.addLine("\t\tif _, err := redisPipeline.Exec(ctx); err != nil { return nil, err }")
		g.addLine("\t\tmissing := pending[:0]")
		g.addLine("\t\tfor i, pos := range pending {")
		g.addLine("\t\t\tvalues, err := redisResults[i].Result()")
		g.addLine("\t\t\tif err != nil { return nil, err }")
		g.addLine("\t\t\tif len(values) == 1 && values[0] == \"\" { continue }")
		g.addLine("\t\t\tif len(values) > 1 && values[0] == p.redisCacheStamp {")
		g.addLine("\t\t\t\tv := uniqueIDs[pos]")
		g.addLine(fmt.Sprintf("\t\t\t\te := &%s{ctx: ctx, id: v, originRedisValues: values[1:]}", names.entityName))
		g.addLine("\t\t\t\tresult[pos] = e")
		g.addLine("\t\t\t\tctx.SetInContextCache(p.cacheIndex, v, e)")
		g.addLine("\t\t\t} else { missing = append(missing, pos) }")
		g.addLine("\t\t}")
		g.addLine("\t\tpending = missing")
		g.addLine("\t}")
	}
	g.addLine("\tif len(pending) > 0 {")
	selectPrefix := "SELECT `ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		selectPrefix += ",`" + columnName + "`"
	}
	selectPrefix += fmt.Sprintf(" FROM `%s` WHERE `ID` IN (", schema.tableName)
	g.addLine("\t\tvar b strings.Builder")
	g.addLine(fmt.Sprintf("\t\tb.Grow(%d + len(pending)*21)", len(selectPrefix)))
	g.addLine(fmt.Sprintf("\t\tb.WriteString(%q)", selectPrefix))
	g.addLine("\t\tvar idBuffer [20]byte")
	g.addLine("\t\tfor i, pos := range pending {")
	g.addLine("\t\t\tif i > 0 { b.WriteByte(',') }")
	g.addLine("\t\t\tb.Write(strconv.AppendUint(idBuffer[:0], uniqueIDs[pos], 10))")
	g.addLine("\t\t}")
	g.addLine("\t\tb.WriteByte(')')")
	g.addLine("\t\trows, cl, err := ctx.DB(p.dbCode).Query(ctx, b.String())")
	g.addLine("\t\tif err != nil { return nil, err }")
	g.addLine("\t\tdefer cl()")
	if schema.hasRedisCache {
		g.addLine("\t\tvar cachePipeline *fluxaorm.RedisPipeLine")
		g.addLine("\t\tif useCache { cachePipeline = ctx.RedisPipeLine(p.redisCode) }")
	}
	g.addLine(fmt.Sprintf("\t\tvar scanArgs [%d]any", len(schema.columnNames)))
	g.addLine("\t\tfor rows.Next() {")
	g.addLine(fmt.Sprintf("\t\t\tsqlRow := &%s{}", names.sqlRowName))
	for i := 0; i < len(schema.columnNames); i++ {
		g.addLine(fmt.Sprintf("\t\t\tscanArgs[%d] = fluxaorm.SQLScanTarget(&sqlRow.F%d)", i, i))
	}
	g.addLine("\t\t\terr = rows.Scan(scanArgs[:]...)")
	g.addLine("\t\t\tif err != nil { return nil, err }")
	g.addLine(fmt.Sprintf("\t\t\te := &%s{ctx: ctx, id: sqlRow.F0, originDatabaseValues: sqlRow}", names.entityName))
	g.addLine("\t\t\tresult[positionOf(sqlRow.F0)] = e")
	g.addLine("\t\t\tctx.SetInContextCache(p.cacheIndex, sqlRow.F0, e)")
	if schema.hasRedisCache {
		g.addLine("\t\t\tif useCache {")
		g.addLine("\t\t\t\tkey := p.redisCachePrefix + string(strconv.AppendUint(idBuffer[:0], sqlRow.F0, 10))")
		g.addLine("\t\t\t\tcachePipeline.Del(key)")
		g.addLine("\t\t\t\tcachePipeline.RPush(key, sqlRow.redisValues()...)")
		g.addLine("\t\t\t\tcachePipeline.Expire(key, fluxaorm.EntityCacheTTL)")
		g.addLine("\t\t\t}")
	}
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif useCache {")
		g.addLine("\t\t\tfor _, pos := range pending {")
		g.addLine("\t\t\t\tif result[pos] != nil { continue }")
		g.addLine("\t\t\t\tkey := p.redisCachePrefix + string(strconv.AppendUint(idBuffer[:0], uniqueIDs[pos], 10))")
		g.addLine("\t\t\t\tcachePipeline.Del(key)")
		g.addLine("\t\t\t\tcachePipeline.RPush(key, \"\")")
		g.addLine("\t\t\t\tcachePipeline.Expire(key, fluxaorm.EntityCacheTTL)")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tif _, err = cachePipeline.Exec(ctx); err != nil { return nil, err }")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine("\tn := 0")
	g.addLine("\tfor _, e := range result { if e != nil { result[n] = e; n++ } }")
	g.addLine("\tclear(result[n:])")
	g.addLine("\treturn result[:n], nil")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateNewMethods(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("func (p %s) New(ctx fluxaorm.Context) *%s  {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\treturn p.NewWithID(ctx, ctx.Engine().NextID())"))
	g.addLine("}")
	g.addLine("")
	defaults := collectRequiredEnumDefaults(schema.fields)
	sqlRowInit := "F0: id"
	for _, d := range defaults {
		sqlRowInit += fmt.Sprintf(", F%d: %q", d.fIndex, d.defaultValue)
	}
	g.addLine(fmt.Sprintf("func (p %s) NewWithID(ctx fluxaorm.Context, id uint64) *%s  {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\te := &%s{ctx: ctx, new: true, id: id, originDatabaseValues: &%s{%s}}", names.entityName, names.sqlRowName, sqlRowInit))
	g.addLine(fmt.Sprintf("\te.ctx.SetInContextCache(%s.cacheIndex, id, e)", names.providerName))
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
				g.addLine(fmt.Sprintf("\t\t\t_redisKey := p.redisCachePrefix + %q + fluxaorm.UniqueIndexKeyHash(_c0.EqValue())", UniqueIndexKeySegment(idx.name, idx.columns)))
				// Redis GET
				g.addLine("\t\t\t_useCache := !ctx.InTransaction()")
				g.addLine("\t\t\tif _useCache {")
				g.addLine("\t\t\t\t_val, _has, _err := ctx.Engine().Redis(p.redisCode).Get(ctx, _redisKey)")
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\tif _has {")
				g.addLine("\t\t\t\t\t_cachedID, _parseErr := strconv.ParseUint(_val, 10, 64)")
				g.addLine("\t\t\t\t\tif _parseErr != nil {")
				g.addLine("\t\t\t\t\t\treturn nil, false, _parseErr")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t_vEntity, _vFound, _vErr := p.GetByID(ctx, _cachedID)")
				g.addLine("\t\t\t\t\tif _vErr != nil {")
				g.addLine("\t\t\t\t\t\treturn nil, false, _vErr")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t\tif _vFound {")
				g.verifyCachedUniqueHit(schema, idx.name, idx.columns,
					func(string) string { return "_c0.EqValue()" }, "\t\t\t\t\t\t")
				g.addLine("\t\t\t\t\t\tif _vOK {")
				g.addLine("\t\t\t\t\t\t\treturn _vEntity, true, nil")
				g.addLine("\t\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t}")
				// MySQL fallback
				whereClause := fmt.Sprintf("`%s` = ?", col)
				if schema.hasFakeDelete {
					whereClause += " AND `FakeDelete` = 0"
				}
				g.addLine(fmt.Sprintf("\t\t\tvar _foundID uint64"))
				g.addLine(fmt.Sprintf("\t\t\t_found, _err := ctx.DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(\"SELECT `ID` FROM `%s` WHERE %s LIMIT 1\", _c0.EqValue()), &_foundID)", schema.tableName, whereClause))
				g.addLine("\t\t\tif _err != nil {")
				g.addLine("\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t}")
				g.addLine("\t\t\tif !_found {")
				g.addLine("\t\t\t\treturn nil, false, nil")
				g.addLine("\t\t\t}")
				// Cache in Redis
				g.addLine("\t\t\tif _useCache {")
				g.addLine("\t\t\t\t_redisPipeline := ctx.RedisPipeLine(p.redisCode)")
				g.addLine("\t\t\t\t_redisPipeline.Set(_redisKey, strconv.FormatUint(_foundID, 10), fluxaorm.EntityCacheTTL)")
				g.addLine("\t\t\t\t_, _err = _redisPipeline.Exec(ctx)")
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
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
				// Each index gets its own scope so shared columns don't redeclare vars.
				g.addLine("\t\t\t{")
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
				args := ""
				for i, col := range idx.columns {
					if i > 0 {
						args += ", "
					}
					args += fmt.Sprintf("_c%s.EqValue()", g.capitalizeFirst(col))
				}
				g.addLine(fmt.Sprintf("\t\t\t\t_redisKey := p.redisCachePrefix + %q + fluxaorm.UniqueIndexKeyHash(%s)", UniqueIndexKeySegment(idx.name, idx.columns), args))
				// Redis GET
				g.addLine("\t\t\t\t_useCache := !ctx.InTransaction()")
				g.addLine("\t\t\t\tif _useCache {")
				g.addLine("\t\t\t\t\t_val, _has, _err := ctx.Engine().Redis(p.redisCode).Get(ctx, _redisKey)")
				g.addLine("\t\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t\tif _has {")
				g.addLine("\t\t\t\t\t\t_cachedID, _parseErr := strconv.ParseUint(_val, 10, 64)")
				g.addLine("\t\t\t\t\t\tif _parseErr != nil {")
				g.addLine("\t\t\t\t\t\t\treturn nil, false, _parseErr")
				g.addLine("\t\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t\t_vEntity, _vFound, _vErr := p.GetByID(ctx, _cachedID)")
				g.addLine("\t\t\t\t\t\tif _vErr != nil {")
				g.addLine("\t\t\t\t\t\t\treturn nil, false, _vErr")
				g.addLine("\t\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t\tif _vFound {")
				g.verifyCachedUniqueHit(schema, idx.name, idx.columns,
					func(col string) string { return fmt.Sprintf("_c%s.EqValue()", g.capitalizeFirst(col)) },
					"\t\t\t\t\t\t\t")
				g.addLine("\t\t\t\t\t\t\tif _vOK {")
				g.addLine("\t\t\t\t\t\t\t\treturn _vEntity, true, nil")
				g.addLine("\t\t\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t\t}")
				g.addLine("\t\t\t\t\t}")
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
				g.addLine(fmt.Sprintf("\t\t\t\t_found, _err := ctx.DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(\"SELECT `ID` FROM `%s` WHERE %s LIMIT 1\", %s), &_foundID)", schema.tableName, whereClause, sqlArgs))
				g.addLine("\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\tif !_found {")
				g.addLine("\t\t\t\t\treturn nil, false, nil")
				g.addLine("\t\t\t\t}")
				// Cache in Redis
				g.addLine("\t\t\t\tif _useCache {")
				g.addLine("\t\t\t\t\t_redisPipeline := ctx.RedisPipeLine(p.redisCode)")
				g.addLine("\t\t\t\t\t_redisPipeline.Set(_redisKey, strconv.FormatUint(_foundID, 10), fluxaorm.EntityCacheTTL)")
				g.addLine("\t\t\t\t\t_, _err = _redisPipeline.Exec(ctx)")
				g.addLine("\t\t\t\t\tif _err != nil {")
				g.addLine("\t\t\t\t\t\treturn nil, false, _err")
				g.addLine("\t\t\t\t\t}")
				g.addLine("\t\t\t\t}")
				g.addLine("\t\t\t\treturn p.GetByID(ctx, _foundID)")
				g.addLine("\t\t\t}")
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
	g.addLine("\tif orderBy := query.BuildOrderClause(); orderBy != \"\" {")
	g.addLine("\t\tb.WriteByte(' ')")
	g.addLine("\t\tb.WriteString(orderBy)")
	g.addLine("\t}")
	g.addLine("\tb.WriteString(\" LIMIT 1\")")
	g.addLine("\tvar _id uint64")
	g.addLine(fmt.Sprintf("\t_found, _err := ctx.DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(b.String(), params...), &_id)"))
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
	g.addLine(fmt.Sprintf("\trows, cl, err := ctx.DB(p.dbCode).Query(ctx, b.String(), params...)"))
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
	g.addLine(fmt.Sprintf("\t_, err := ctx.DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(b.String(), params...), &totalRows)"))
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
	g.addLine(fmt.Sprintf("\trows, cl, err := ctx.DB(p.dbCode).Query(ctx, b.String(), params...)"))
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

func (g *codeGenerator) generateCount(schema *entitySchema, names *entityNames) {
	g.addImport("strings")
	g.addLine(fmt.Sprintf("func (p %s) Count(ctx fluxaorm.Context, query *fluxaorm.DBQuery) (int, error) {", names.providerNamePrivate))
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
	g.addLine("\tvar count int")
	g.addLine(fmt.Sprintf("\t_, err := ctx.DB(p.dbCode).QueryRow(ctx, fluxaorm.NewWhere(b.String(), params...), &count)"))
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn 0, err")
	g.addLine("\t}")
	g.addLine("\treturn count, nil")
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
	g.addLine(fmt.Sprintf("\t_rows, _cl, _err := ctx.DB(p.dbCode).Query(ctx, %s)", selectQuery))
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

// verifyCachedUniqueHit emits the guard that a cached unique-index hit really is the row asked for.
// The key is a hash of the queried values, so a collision — or a key outliving the row it pointed
// at — otherwise returns somebody else's entity. Costs no query: the row is already in memory.
func (g *codeGenerator) verifyCachedUniqueHit(schema *entitySchema, idxName string, columns []string,
	condExpr func(col string) string, indent string) {
	g.addImport("fmt")
	fIndexes := schema.uniqueIndexFIndexes[idxName]
	cols := make([]uniqueIndexColInfo, len(columns))
	for i, colName := range columns {
		cols[i] = g.getUniqueIndexColInfo(schema, colName, fIndexes[i])
	}
	fmtStr, wantArgs, redisArgs, dbArgs := "", "", "", ""
	for i, c := range cols {
		if i > 0 {
			fmtStr += "\\x00"
			wantArgs += ", "
			redisArgs += ", "
			dbArgs += ", "
		}
		fmtStr += "%v"
		wantArgs += condExpr(c.colName)
		redisArgs += fmt.Sprintf("_vEntity.originRedisValues[%d]", c.fIndex)
		if c.nullable {
			dbArgs += fmt.Sprintf("_vEntity.originDatabaseValues.F%d%s", c.fIndex, c.bindInnerField)
		} else {
			dbArgs += fmt.Sprintf("_vEntity.originDatabaseValues.F%d", c.fIndex)
		}
	}
	if schema.hasRedisCache {
		g.addLine(indent + "_vGot := \"\"")
		g.addLine(indent + "if _vEntity.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("%s\t_vGot = fmt.Sprintf(\"%s\", %s)", indent, fmtStr, redisArgs))
		g.addLine(indent + "} else {")
		g.addLine(fmt.Sprintf("%s\t_vGot = fmt.Sprintf(\"%s\", %s)", indent, fmtStr, dbArgs))
		g.addLine(indent + "}")
	} else {
		g.addLine(fmt.Sprintf("%s_vGot := fmt.Sprintf(\"%s\", %s)", indent, fmtStr, dbArgs))
	}
	g.addLine(fmt.Sprintf("%s_vOK := _vGot == fmt.Sprintf(\"%s\", %s)", indent, fmtStr, wantArgs))
	if !schema.hasFakeDelete {
		return
	}
	fdIndex := -1
	for i, cn := range schema.columnNames {
		if cn == "FakeDelete" {
			fdIndex = i
			break
		}
	}
	if fdIndex < 0 {
		return
	}
	g.addLine(indent + "if _vOK {")
	if schema.hasRedisCache {
		g.addLine(indent + "\t_vFakeDelete := \"\"")
		g.addLine(indent + "\tif _vEntity.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("%s\t\t_vFakeDelete = fmt.Sprintf(\"%%v\", _vEntity.originRedisValues[%d])", indent, fdIndex))
		g.addLine(indent + "\t} else {")
		g.addLine(fmt.Sprintf("%s\t\t_vFakeDelete = fmt.Sprintf(\"%%v\", _vEntity.originDatabaseValues.F%d)", indent, fdIndex))
		g.addLine(indent + "\t}")
	} else {
		g.addLine(fmt.Sprintf("%s\t_vFakeDelete := fmt.Sprintf(\"%%v\", _vEntity.originDatabaseValues.F%d)", indent, fdIndex))
	}
	g.addLine(indent + "\t_vOK = _vFakeDelete == \"0\"")
	g.addLine(indent + "}")
}

// generateGetAll emits the read behind `orm:"cached"`: every row of a small, rarely written table,
// served without touching MySQL. The id set lives in one Redis list under the row-cache prefix and
// the rows themselves come back through GetByIDs, so the two tiers stay the single source of truth
// for a row's contents - this cache only ever answers "which ids exist".
func (g *codeGenerator) generateGetAll(schema *entitySchema, names *entityNames) {
	g.addImport("strconv")
	g.addLine(fmt.Sprintf("func (p %s) GetAll(ctx fluxaorm.Context) ([]*%s, error) {", names.providerNamePrivate, names.entityName))

	// Inside a transaction MySQL reads go through the tx, so a cached set would hide the write that
	// is in flight and a fill would publish uncommitted ids. Same gate as GetByID.
	g.addLine("\t_useCache := !ctx.InTransaction()")
	g.addLine("\tif _useCache {")
	g.addLine("\t\t_cached, err := ctx.Engine().Redis(p.redisCode).LRange(ctx, p.redisAllKey, 0, -1)")
	g.addLine("\t\tif err != nil {")
	g.addLine("\t\t\treturn nil, err")
	g.addLine("\t\t}")
	g.addLine("\t\tif len(_cached) > 0 && _cached[0] == p.redisCacheStamp {")
	g.addLine("\t\t\t_ids := make([]uint64, 0, len(_cached)-1)")
	g.addLine("\t\t\t_ok := true")
	g.addLine("\t\t\tfor _, _raw := range _cached[1:] {")
	g.addLine("\t\t\t\t_id, _parseErr := strconv.ParseUint(_raw, 10, 64)")
	g.addLine("\t\t\t\tif _parseErr != nil {")
	g.addLine("\t\t\t\t\t_ok = false")
	g.addLine("\t\t\t\t\tbreak")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\t_ids = append(_ids, _id)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t\tif _ok {")
	g.addLine("\t\t\t\treturn p.GetByIDs(ctx, _ids...)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	g.addLine("\t}")

	selectSQL := fmt.Sprintf("SELECT `ID` FROM `%s`", schema.tableName)
	if schema.hasFakeDelete {
		selectSQL += " WHERE `FakeDelete` = 0"
	}
	selectSQL += " ORDER BY `ID`"

	g.addLine(fmt.Sprintf("\trows, cl, err := ctx.DB(p.dbCode).Query(ctx, %q)", selectSQL))
	g.addLine("\tif err != nil {")
	g.addLine("\t\treturn nil, err")
	g.addLine("\t}")
	g.addLine("\tvar ids []uint64")
	g.addLine("\tfor rows.Next() {")
	g.addLine("\t\tvar id uint64")
	g.addLine("\t\tif err = rows.Scan(&id); err != nil {")
	g.addLine("\t\t\tcl()")
	g.addLine("\t\t\treturn nil, err")
	g.addLine("\t\t}")
	g.addLine("\t\tids = append(ids, id)")
	g.addLine("\t}")
	g.addLine("\tcl()")

	// The stamp is always element 0, so an empty table still caches as a one-element list and is
	// told apart from a cold key. It also means RowCacheRewriteScript never RPUSHes nothing.
	g.addLine("\tif _useCache && len(ids) <= fluxaorm.MaxCachedAllRows {")
	g.addLine("\t\t_evalArgs := make([]any, 0, len(ids)+2)")
	g.addLine("\t\t_evalArgs = append(_evalArgs, int(fluxaorm.EntityCacheTTL.Seconds()), p.redisCacheStamp)")
	g.addLine("\t\tfor _, id := range ids {")
	g.addLine("\t\t\t_evalArgs = append(_evalArgs, strconv.FormatUint(id, 10))")
	g.addLine("\t\t}")
	g.addLine("\t\t_, err = ctx.Engine().Redis(p.redisCode).Eval(ctx, fluxaorm.RowCacheRewriteScript, []string{p.redisAllKey}, _evalArgs...)")
	g.addLine("\t\tif err != nil {")
	g.addLine("\t\t\treturn nil, err")
	g.addLine("\t\t}")
	g.addLine("\t}")

	g.addLine("\treturn p.GetByIDs(ctx, ids...)")
	g.addLine("}")
	g.addLine("")
}
