package fluxaorm

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type codeGenerator struct {
	engine      Engine
	dir         string
	enums       map[string]bool
	imports     map[string]bool
	enumsImport string
	body        string
	filedIndex  int
	cacheIndex  int
}

func Generate(engine Engine, outputDirectory string) error {

	if strings.TrimSpace(outputDirectory) == "" {
		return fmt.Errorf("output directory is empty")
	}

	absOutputDirectory, err := filepath.Abs(outputDirectory)
	if err != nil {
		return fmt.Errorf("cannot get absolute path for output directory: %w", err)
	}
	absOutputDirectory = filepath.Clean(absOutputDirectory)

	info, err := os.Stat(absOutputDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("output directory does not exist: %s", absOutputDirectory)
		}
		return fmt.Errorf("cannot access output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory: %s", absOutputDirectory)
	}

	f, err := os.CreateTemp(absOutputDirectory, ".fluxaorm-writecheck-*")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s: %w", absOutputDirectory, err)
	}
	tmp := f.Name()
	_ = f.Close()
	_ = os.Remove(tmp)

	goModPath, err := findGoMod(absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot find go.mod: %w", err)
	}
	moduleName, err := getModuleName(goModPath)
	if err != nil {
		return fmt.Errorf("cannot get module name from go.mod: %w", err)
	}
	goModDir := filepath.Dir(goModPath)
	relPath, err := filepath.Rel(goModDir, absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot get relative path: %w", err)
	}
	enumsImport := moduleName
	if relPath != "." {
		enumsImport += "/" + filepath.ToSlash(relPath)
	}
	enumsImport += "/enums"

	files, err := os.ReadDir(absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot read output directory: %w", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			err = os.Remove(filepath.Join(absOutputDirectory, file.Name()))
			if err != nil {
				return fmt.Errorf("cannot remove file %s: %w", file.Name(), err)
			}
		}
	}

	generator := codeGenerator{engine: engine, dir: absOutputDirectory, enums: nil, enumsImport: enumsImport}

	for _, schema := range engine.Registry().Entities() {
		generator.body = ""
		generator.imports = make(map[string]bool)
		err = generator.generateCodeForEntity(schema.(*entitySchema))
		if err != nil {
			return err
		}
	}

	return nil
}

func findGoMod(dir string) (string, error) {
	dir = filepath.Clean(dir)
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return goModPath, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found")
}

func getModuleName(goModPath string) (string, error) {
	content, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", fmt.Errorf("module name not found in go.mod")
}

func (g *codeGenerator) generateCodeForEntity(schema *entitySchema) error {
	packageName := filepath.Base(g.dir)
	fileName := path.Join(g.dir, fmt.Sprintf("%s.go", schema.GetTableName()))
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	entityName := g.capitalizeFirst(schema.GetTableName())
	entityPrivate := g.lowerFirst(schema.GetTableName())
	providerName := entityName + "Provider"
	providerNamePrivate := entityPrivate + "Provider"
	sqlRowName := entityPrivate + "SQLRow"
	g.addImport("sync")
	g.addImport("github.com/latolukasz/fluxaorm")
	g.addLine(fmt.Sprintf("type %s struct {", providerNamePrivate))
	g.addLine("\ttableName string")
	g.addLine("\tdbCode string")
	g.addLine("\tredisCode string")
	g.addLine("\tcacheIndex uint64")
	g.addLine("\tuuidRedisKeyMutex *sync.Mutex")
	if schema.hasRedisCache {
		g.addLine("\tredisCachePrefix string")
		g.addLine("\tredisCacheStamp string")
		g.addLine("\tredisCacheTTL int")
	}
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("var %s = %s{", providerName, providerNamePrivate))
	g.addLine(fmt.Sprintf("\ttableName: \"%s\",", schema.tableName))
	g.addLine(fmt.Sprintf("\tdbCode: \"%s\",", schema.mysqlPoolCode))
	g.addLine(fmt.Sprintf("\tredisCode: \"%s\",", schema.getForcedRedisCode()))
	g.addLine(fmt.Sprintf("\tcacheIndex: %d,", g.cacheIndex))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\tredisCachePrefix: \"%s\",", schema.cacheKey+":"))
		g.addLine(fmt.Sprintf("\tredisCacheStamp: \"%s\",", schema.structureHash))
		g.addLine(fmt.Sprintf("\tredisCacheTTL: %d,", schema.cacheTTL))
	}
	g.cacheIndex++
	g.addLine(fmt.Sprintf("\tuuidRedisKeyMutex: &sync.Mutex{},"))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("type %s struct {", sqlRowName))
	g.filedIndex = 0
	g.addLine(strings.TrimRight(g.addSQLRowLines(schema.fields), "\t\n"))
	g.filedIndex = 0
	g.addLine("}")
	g.addLine("")

	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("func (r *%s) redisValues() []any {", sqlRowName))
		g.addLine(fmt.Sprintf("\tredisListValues := make([]any, %d)", len(schema.columnNames)+1))
		g.addLine(fmt.Sprintf("\tredisListValues[0] = %s.redisCacheStamp", providerName))
		g.addRedisBindSetLines(schema, schema.fields)
		g.addLine("\treturn redisListValues")
		g.addLine("}")
		g.addLine("")
	}

	g.addLine(fmt.Sprintf("func (p %s) GetByID(ctx fluxaorm.Context, id uint64) (entity *%s, found bool, err error) {", providerNamePrivate, entityName))
	if schema.hasRedisCache {
		g.addLine("\tredisKey := p.redisCachePrefix + strconv.FormatUint(id, 10)")
		g.addLine("\tredisValues, err := ctx.Engine().Redis(p.redisCode).LRange(ctx, redisKey, 0, -1)")
		g.addLine("\tif err != nil {")
		g.addLine("\t\treturn nil, false, err")
		g.addLine("\t}")
		g.addLine("\tif len(redisValues) <= 1 {")
		g.addLine("\t\treturn nil, false, nil")
		g.addLine("\t}")
		g.addLine("\tif redisValues[0] == p.redisCacheStamp {")
		g.addLine(fmt.Sprintf("\t\treturn &%s{ctx: ctx, id: id, originRedisValues: redisValues[1:]}, true, nil", entityName))
		g.addLine("\t}")
	}
	g.appendToLine("\tquery := \"SELECT `ID`")
	for _, columnName := range schema.GetColumns()[1:] {
		g.appendToLine(",`" + columnName + "`")
	}
	g.addLine(fmt.Sprintf(" FROM `%s` WHERE `ID` = ? LIMIT 1\"", schema.tableName))
	g.addLine(fmt.Sprintf("\tsqlRow := &%s{}", sqlRowName))
	g.appendToLine(fmt.Sprintf("\tfound, err = ctx.Engine().DB(%s.dbCode).QueryRow(ctx, fluxaorm.NewWhere(query, id), &sqlRow.F0", providerName))
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
	g.addLine(fmt.Sprintf("\treturn &%s{ctx: ctx, id: id, originDatabaseValues: sqlRow}, true, nil", entityName))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (p %s) GetByIDs(ctx fluxaorm.Context, id ...uint64) (fluxaorm.EntityIterator[%s], error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) GetAll(ctx fluxaorm.Context) (fluxaorm.EntityIterator[%s], error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) New(ctx fluxaorm.Context) *%s  {", providerNamePrivate, entityName))
	g.addLine(fmt.Sprintf("\treturn p.NewWithID(ctx, p.uuid(ctx))"))
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) NewWithID(ctx fluxaorm.Context, id uint64) *%s  {", providerNamePrivate, entityName))
	g.addLine(fmt.Sprintf("\te := &%s{ctx: ctx, new: true, id: id, originDatabaseValues: &%s{F0: id}}", entityName, sqlRowName))
	g.addLine(fmt.Sprintf("\te.ctx.Track(e, %s.cacheIndex)", providerName))
	if schema.hasRedisCache {
		g.addImport("strconv")
	}
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	for indexName, index := range schema.indexes {
		g.body += fmt.Sprintf("func (p %s) GetByIndex%s(ctx fluxaorm.Context", providerNamePrivate, indexName)
		for _, columnName := range index.Columns {
			g.body += fmt.Sprintf(", %s any", g.lowerFirst(columnName))
		}
		g.addLine(fmt.Sprintf(") (fluxaorm.EntityIterator[%s], error) {", entityName))
		g.addLine("\treturn nil, nil")
		g.addLine("}")
		g.addLine("")
	}
	for indexName, index := range schema.uniqueIndexes {
		g.body += fmt.Sprintf("func (p %s) GetByIndex%s(ctx fluxaorm.Context", providerNamePrivate, indexName)
		for _, columnName := range index.Columns {
			g.body += fmt.Sprintf(", %s any", g.lowerFirst(columnName))
		}
		g.addLine(fmt.Sprintf(") (entity *%s, found bool, err error) {", entityName))
		g.addLine("\treturn nil, false, nil")
		g.addLine("}")
		g.addLine("")
	}
	g.addLine(fmt.Sprintf("func (p %s) SearchWithCount(ctx fluxaorm.Context, where fluxaorm.Where, pager *fluxaorm.Pager) (entities fluxaorm.EntityIterator[%s], totalRows int, err error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, 0, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) Search(ctx fluxaorm.Context, where fluxaorm.Where, pager *fluxaorm.Pager) (entities fluxaorm.EntityIterator[%s], err error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) SearchIDsWithCount(ctx fluxaorm.Context, where fluxaorm.Where, pager *fluxaorm.Pager) (results []uint64, totalRows int, err error) {", providerNamePrivate))
	g.addLine("\treturn nil, 0, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) SearchIDs(ctx fluxaorm.Context, where fluxaorm.Where, pager *fluxaorm.Pager) (results []uint64, err error) {", providerNamePrivate))
	g.addLine("\treturn nil, nil")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) SearchOne(ctx fluxaorm.Context, where fluxaorm.Where) (entity *%s, found bool, err error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, false, nil")
	g.addLine("}")
	g.addLine("")

	// private methods
	g.addLine(fmt.Sprintf("func (p %s) uuid(ctx fluxaorm.Context) uint64 {", providerNamePrivate))
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

	g.addLine(fmt.Sprintf("func (p %s) initUUID(ctx fluxaorm.Context) {", providerNamePrivate))
	g.addLine(fmt.Sprintf("\tr := ctx.Engine().Redis(p.redisCode)"))
	g.addLine(fmt.Sprintf("\t%s.uuidRedisKeyMutex.Lock()", providerName))
	g.addLine(fmt.Sprintf("\tdefer %s.uuidRedisKeyMutex.Unlock()", providerName))
	g.addLine(fmt.Sprintf("\tnow, has, err := r.Get(ctx, \"%s\")", schema.uuidCacheKey))
	g.addLine(fmt.Sprintf("\tif err != nil {\n\t\tpanic(err)\n\t}"))
	g.addLine(fmt.Sprintf("\tif has && now != \"1\" {\n\t\treturn\n\t}"))
	g.addLine(fmt.Sprintf("\tlockName := \"%s:lock\"", schema.uuidCacheKey))
	g.addImport("time")
	g.addImport("errors")
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

	g.addLine(fmt.Sprintf("type %s struct {", entityName))
	g.addLine("\tctx fluxaorm.Context")
	g.addLine("\tid uint64")
	g.addLine("\tnew bool")
	g.addLine("\tdeleted bool")
	g.addLine(fmt.Sprintf("\toriginDatabaseValues *%s", sqlRowName))
	g.addLine("\tdatabaseBind fluxaorm.Bind")
	if schema.hasRedisCache {
		g.addLine("\tredisBind map[int64]any")
		g.addLine("\toriginRedisValues []string")
	}
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) GetID() uint64 {", entityName))
	g.addLine("\treturn e.id")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) Delete() {", entityName))
	g.addLine("\te.deleted = true")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) addToDatabaseBind(column string, value any) {", entityName))
	g.addLine("\tif e.databaseBind == nil {")
	g.addLine("\t\te.databaseBind = fluxaorm.Bind{}")
	g.addLine(fmt.Sprintf("\t\te.ctx.Track(e, %s.cacheIndex)", providerName))
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\te.databaseBind[column] = value"))
	g.addLine("}")
	g.addLine("")

	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("func (e *%s) addToRedisBind(index int64, value any) {", entityName))
		g.addLine("\tif e.redisBind == nil {")
		g.addLine("\t\te.redisBind = make(map[int64]any)")
		g.addLine("\t}")
		g.addLine(fmt.Sprintf("\te.redisBind[index] = value"))
		g.addLine("}")
		g.addLine("")
	}

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlush() error {", entityName))
	g.addLine("\tif e.new {")
	insertQueryLine := "\t\tsqlQuery := \"INSERT INTO `" + schema.tableName + "` (`ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		insertQueryLine += ",`" + columnName + "`"
	}
	insertQueryLine += fmt.Sprintf(") VALUES (?%s)\"\n", strings.Repeat(",?", len(schema.columnNames)-1))
	insertQueryLine += fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, e.originDatabaseValues.F0", providerName)
	for i := 1; i < len(schema.columnNames); i++ {
		insertQueryLine += fmt.Sprintf(", e.originDatabaseValues.F%d", i)
	}
	insertQueryLine += ")\n"
	if schema.hasRedisCache {
		insertQueryLine += fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisCode).RPush(%s.redisCachePrefix+strconv.FormatUint(e.GetID(), 10), e.originDatabaseValues.redisValues()...)", providerName, providerName)
	}
	g.addLine(insertQueryLine)
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
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
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, updateParams...)", providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\tredisPipeLine := e.ctx.RedisPipeLine(%s.redisCode)", providerName))
		g.addLine(fmt.Sprintf("\t\tredisKey := %s.redisCachePrefix + strconv.FormatUint(e.GetID(), 10)", providerName))
		g.addLine("\t\tfor index, value := range e.redisBind {")
		g.addLine("\t\t\tredisPipeLine.LSet(redisKey, index, value)")
		g.addLine("\t\t}")
		g.addLine("\t\te.redisBind = nil")
	}
	g.addLine("\t\te.databaseBind = nil")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlushed() {", entityName))
	g.addLine("\tif e.new {")
	g.addLine("\t\te.new = false")
	g.addLine("\t}")
	g.addLine("}")
	g.addLine("")

	g.filedIndex = 0
	err = g.generateGettersSetters(entityName, providerName, schema, schema.fields)
	if err != nil {
		return err
	}

	g.writeToFile(f, fmt.Sprintf("package %s\n", packageName))
	g.writeToFile(f, "\n")
	if len(g.imports) == 1 {
		for i := range g.imports {
			g.writeToFile(f, fmt.Sprintf("import \"%s\"\n\n", i))
		}
	} else if len(g.imports) > 1 {
		g.writeToFile(f, "import (\n")
		for i := range g.imports {
			g.writeToFile(f, fmt.Sprintf("\t\"%s\"\n", i))
		}
		g.writeToFile(f, ")\n")
		g.writeToFile(f, "\n")
	}
	g.writeToFile(f, g.body)
	return nil
}

func (g *codeGenerator) addImport(value string) {
	g.imports[value] = true
}

func (g *codeGenerator) createGetterSetterUint64(schema *entitySchema, fieldName, entityName, getterSuffix, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s%s() uint64 {", entityName, fieldName, getterSuffix))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(uint64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value uint64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterInt64(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() int64 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(int64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value int64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBool(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() bool {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(bool)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d;", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value bool) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis := e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif value {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"1\")", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"0\")", g.filedIndex+1))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterFloat(schema *entitySchema, fieldName, entityName, providerName string, precision, size int) {
	g.addImport("math")
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() float64 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(float64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value float64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(fromRedis*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", precision, precision))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(e.originDatabaseValues.F%d*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", g.filedIndex, precision, precision))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif math.Round(e.originDatabaseValues.F%d*math.Pow10(%d)) == math.Round(value*math.Pow10(%d)) {", g.filedIndex, precision, precision))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, strconv.FormatFloat(value, 'f', %d, %d))", g.filedIndex+1, precision, size))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterTime(schema *entitySchema, fieldName, entityName, providerName string, dateOnly bool) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() time.Time {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(time.Time)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn time.Unix(fromRedis, 0)")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) {", entityName, fieldName))
	if dateOnly {
		g.addLine("\tvalue = value.Truncate(time.Hour * 24)")
	} else {
		g.addLine("\tvalue = value.Truncate(time.Second)")
	}
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value.Unix()")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d.Unix() == value.Unix()", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Unix() == value.Unix() {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value.Unix())", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterString(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() string {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(string)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value string) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterEnum(schema *entitySchema, fieldName, entityName, enumName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, enumName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine(fmt.Sprintf("\t\t\t\treturn %s(v.(string))", enumName))
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn %s(e.originRedisValues[%d])", enumName, g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn %s(e.originDatabaseValues.F%d)", enumName, g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, enumName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = string(value)", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == string(value)", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == string(value)", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == string(value) {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", string(value))", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, string(value))", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterSet(schema *entitySchema, fieldName, entityName, setName, providerName string) {
	g.addImport("sort")
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() []%s {", entityName, fieldName, setName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tsliced := strings.Split(v.(string), \",\")")
	g.addLine(fmt.Sprintf("\t\t\t\tvalue := make([]%s, len(sliced))", setName))
	g.addLine("\t\t\t\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\t\t\t\tvalue[k] = %s(code)", setName))
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn value")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tsliced := strings.Split(e.originRedisValues[%d], \",\")", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tvalue := make([]%s, len(sliced))", setName))
		g.addLine("\t\t\tfor k, code := range sliced {")
		g.addLine(fmt.Sprintf("\t\t\t\tvalue[k] = %s(code)", setName))
		g.addLine("\t\t\t}")
		g.addLine("\t\t\treturn value")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == \"\" {", g.filedIndex))
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tsliced := strings.Split(e.originDatabaseValues.F%d, \",\")", g.filedIndex))
	g.addLine(fmt.Sprintf("\tfinalValue := make([]%s, len(sliced))", setName))
	g.addLine("\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\tfinalValue[k] = %s(code)", setName))
	g.addLine("\t}")
	g.addLine("\treturn finalValue")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value ...%s) {", entityName, fieldName, setName))
	g.addLine("\tslice := make([]string, len(value))")
	g.addLine("\tfor k, v := range value {")
	g.addLine("\t\tslice[k] = string(v)")
	g.addLine("\t}")
	g.addLine("\tsort.Strings(slice)")
	g.addLine("\tasString := strings.Join(slice, \",\")")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = asString", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == asString", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == asString {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", asString)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, asString)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterUint64Nullable(schema *entitySchema, fieldName, entityName, getterSuffix, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s%s() *uint64 {", entityName, fieldName, getterSuffix))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullInt64)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\tasUint64 := uint64(vNullable.Int64)")
	g.addLine("\t\t\t\t\treturn &asUint64")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\tuintValue := uint64(e.originDatabaseValues.F%d.Int64)", g.filedIndex))
	g.addLine("\t\treturn &uintValue")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")
	if getterSuffix == "" {
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *uint64) {", entityName, fieldName))
		g.addLine("\tbindValue := sql.NullInt64{}")
		g.addLine("\tif value != nil {")
		g.addLine("\t\tbindValue.Valid = true")
		g.addLine("\t\tbindValue.Int64 = int64(*value)")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value uint64) {", entityName, fieldName))
		g.addLine("\tbindValue := sql.NullInt64{}")
		g.addLine("\tif value != 0 {")
		g.addLine("\t\tbindValue.Valid = true")
		g.addLine("\t\tbindValue.Int64 = int64(value)")
		g.addLine("\t}")
	}
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		if getterSuffix == "" {
			g.addLine("\t\tif value != nil {")
			g.addLine("\t\t\tasString = strconv.FormatUint(*value, 10)")
			g.addLine("\t\t}")
		} else {
			g.addLine("\t\tif value != 0 {")
			g.addLine("\t\t\tasString = strconv.FormatUint(value, 10)")
			g.addLine("\t\t}")
		}
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif bindValue.Valid {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, uint64(bindValue.Int64))", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"\")", g.filedIndex+1))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterInt64Nullable(schema *entitySchema, fieldName, entityName, getterSuffix, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s%s() *int64 {", entityName, fieldName, getterSuffix))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullInt64)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn &vNullable.Int64")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn &e.originDatabaseValues.F%d.Int64", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *int64) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullInt64{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.Int64 = *value")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = strconv.FormatInt(*value, 10)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif bindValue.Valid {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, bindValue.Int64)", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"\")", g.filedIndex+1))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterStringNullable(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() *string {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullString)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn &vNullable.String")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis := e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn &e.originDatabaseValues.F%d.String", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value string) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullString{}")
	g.addLine("\tif value != \"\" {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.String = value")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, bindValue.String)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterTimeNullable(schema *entitySchema, fieldName, entityName, providerName string, dateOnly bool) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() *time.Time {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullTime)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn &vNullable.Time")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\tt := time.Unix(fromRedis, 0)")
		g.addLine("\t\t\treturn &t")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn &e.originDatabaseValues.F%d.Time", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *time.Time) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullTime{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.Time = *value")
	if dateOnly {
		g.addLine("\t\tbindValue.Time = bindValue.Time.Truncate(time.Hour * 24)")
	} else {
		g.addLine("\t\tbindValue.Time = bindValue.Time.Truncate(time.Second)")
	}
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = strconv.FormatInt(value.Unix(), 10)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif !bindValue.Valid {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"\")", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, bindValue.Time.Unix())", g.filedIndex+1))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBoolNullable(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() *bool {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullBool)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn &vNullable.Bool")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis := e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn &e.originDatabaseValues.F%d.Bool", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *bool) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullBool{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.Bool = *value")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tif *value {")
		g.addLine("\t\t\t\tasString = \"1\"")
		g.addLine("\t\t\t} else {")
		g.addLine("\t\t\t\tasString = \"0\"")
		g.addLine("\t\t\t}")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif !bindValue.Valid {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"\")", g.filedIndex+1))
		g.addLine("\t} else if bindValue.Bool {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"1\")", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"0\")", g.filedIndex+1))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterFloatNullable(schema *entitySchema, fieldName, entityName, providerName string, precision, size int) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() *float64 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullFloat64)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn &vNullable.Float64")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn &e.originDatabaseValues.F%d.Float64", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *float64) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullFloat64{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.Float64 = *value")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tfromRedis := sql.NullFloat64{}")
		g.addLine(fmt.Sprintf("\t\tif e.originRedisValues[%d] != \"\" {", g.filedIndex))
		g.addLine("\t\t\tfromRedis.Valid = true")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis.Float64, _ = strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = fromRedis.Valid == bindValue.Valid && math.Round(fromRedis.Float64*math.Pow10(%d)) == math.Round(bindValue.Float64*math.Pow10(%d))", precision, precision))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsource := e.originDatabaseValues.F%d", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tsame = source.Valid == bindValue.Valid && math.Round(source.Float64*math.Pow10(%d)) == math.Round(bindValue.Float64*math.Pow10(%d))", precision, precision))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tsource := e.originDatabaseValues.F%d", g.filedIndex))
		g.addLine(fmt.Sprintf("\tif source.Valid == bindValue.Valid && math.Round(source.Float64*math.Pow10(%d)) == math.Round(bindValue.Float64*math.Pow10(%d)) {", precision, precision))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif !bindValue.Valid {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"\")", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, strconv.FormatFloat(bindValue.Float64, 'f', %d, %d))", g.filedIndex+1, precision, size))
		g.addLine("\t}")
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterSetNullable(schema *entitySchema, fieldName, entityName, setName, providerName string) {
	g.addImport("sort")
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() []%s {", entityName, fieldName, setName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullString)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\tsliced := strings.Split(v.(string), \",\")")
	g.addLine(fmt.Sprintf("\t\t\t\t\tvalue := make([]%s, len(sliced))", setName))
	g.addLine("\t\t\t\t\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\t\t\t\t\tvalue[k] = %s(code)", setName))
	g.addLine("\t\t\t\t\t}")
	g.addLine("\t\t\t\t\treturn value")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tsliced := strings.Split( e.originRedisValues[%d], \",\")", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tvalue := make([]%s, len(sliced))", setName))
		g.addLine("\t\t\tfor k, code := range sliced {")
		g.addLine(fmt.Sprintf("\t\t\t\tvalue[k] = %s(code)", setName))
		g.addLine("\t\t\t}")
		g.addLine("\t\t\treturn value")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\tif e.originDatabaseValues.F%d.String == \"\" {", g.filedIndex))
	g.addLine("\t\t\treturn nil")
	g.addLine("\t\t}")
	g.addLine(fmt.Sprintf("\t\tsliced := strings.Split(e.originDatabaseValues.F%d.String, \",\")", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\tfinalValue := make([]%s, len(sliced))", setName))
	g.addLine("\t\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\t\tfinalValue[k] = %s(code)", setName))
	g.addLine("\t\t}")
	g.addLine("\t\treturn finalValue")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value ...%s) {", entityName, fieldName, setName))
	g.addLine("\tbindValue := sql.NullString{}")
	g.addLine("\tif len(value) > 0 {")
	g.addLine("\t\tbindValue.Valid = true")

	g.addLine("\t\tslice := make([]string, len(value))")
	g.addLine("\t\tfor k, v := range value {")
	g.addLine("\t\t\tslice[k] = string(v)")
	g.addLine("\t\t}")
	g.addLine("\t\tsort.Strings(slice)")
	g.addLine("\t\tasString := strings.Join(slice, \",\")")
	g.addLine("\t\tbindValue.String = asString")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == bindValue.String", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, bindValue.String)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBytesNullable(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() []uint8 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullString)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\t\treturn []uint8(vNullable.String)")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis := e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t\treturn []uint8(fromRedis)")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\treturn []uint8(e.originDatabaseValues.F%d.String)", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value []uint8) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullString{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.String = string(value)")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = string(value)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, bindValue.String)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterEnumNullable(schema *entitySchema, fieldName, entityName, enumName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() *%s {", entityName, fieldName, enumName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tvNullable := v.(sql.NullString)")
	g.addLine("\t\t\t\tif vNullable.Valid {")
	g.addLine(fmt.Sprintf("\t\t\t\t\tvalue := %s(vNullable.String)", enumName))
	g.addLine("\t\t\t\t\treturn &value")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn nil")

	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis := %s(e.originRedisValues[%d])", enumName, g.filedIndex))
		g.addLine("\t\t\treturn &fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine(fmt.Sprintf("\t\tenumValue := %s(e.originDatabaseValues.F%d.String)", enumName, g.filedIndex))
	g.addLine("\t\treturn &enumValue")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *%s) {", entityName, fieldName, enumName))
	g.addLine("\tbindValue := sql.NullString{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.String = string(*value)")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = bindValue", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = string(*value)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", bindValue)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, bindValue.String)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) generateGettersSetters(entityName, providerName string, schema *entitySchema, fields *tableFields) error {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			g.filedIndex++
			continue
		}
		g.createGetterSetterUint64(schema, fieldName, entityName, "", providerName)
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		refTypeName := schema.references[fieldName].Type.String()
		refName := g.capitalizeFirst(refTypeName[strings.LastIndex(refTypeName, ".")+1:])
		required := fields.referencesRequired[k]
		if required {
			g.createGetterSetterUint64(schema, fieldName, entityName, "ID", providerName)
		} else {
			g.createGetterSetterUint64Nullable(schema, fieldName, entityName, "ID", providerName)
		}
		g.addLine(fmt.Sprintf("func (e *%s) Get%s(ctx fluxaorm.Context) (reference *%s, found bool, err error) {", entityName, fieldName, refName))
		g.addLine(fmt.Sprintf("\tid := e.Get%sID()", fieldName))
		if required {
			g.addLine("\tif id == 0 {\n\t\treturn nil, false, nil\n\t}")
			g.addLine(fmt.Sprintf("\treturn %sProvider.GetByID(ctx, id)", refName))
		} else {
			g.addLine("\tif id == nil || *id == 0 {\n\t\treturn nil, false, nil\n\t}")
			g.addLine(fmt.Sprintf("\treturn %sProvider.GetByID(ctx, *id)", refName))
		}
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterInt64(schema, fieldName, entityName, providerName)
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterBool(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterFloat(schema, fieldName, entityName, providerName, fields.floatsPrecision[k], fields.floatsSize[k])
	}
	for _, i := range fields.times {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, providerName, false)
	}
	for _, i := range fields.dates {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, providerName, true)
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.stringsRequired[k] {
			g.createGetterSetterString(schema, fieldName, entityName, providerName)
		} else {
			g.addImport("database/sql")
			g.createGetterSetterStringNullable(schema, fieldName, entityName, providerName)
		}
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterUint64Nullable(schema, fieldName, entityName, "", providerName)
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterInt64Nullable(schema, fieldName, entityName, "", providerName)
	}
	for k, i := range fields.stringsEnums {
		g.addImport(g.enumsImport)
		if g.enums == nil {
			g.enums = make(map[string]bool)
			err := os.MkdirAll(path.Join(g.dir, "enums"), 0755)
			if err != nil {
				return err
			}
		}
		d := fields.enums[k]
		enumName := g.capitalizeFirst(d.name[strings.LastIndex(d.name, ".")+1:])
		_, enumCreated := g.enums[fields.fields[i].Name]
		if !enumCreated {
			err := g.createEnumDefinition(d, enumName)
			if err != nil {
				return err
			}
		}
		enumFullName := "enums." + enumName
		fieldName := fields.prefix + fields.fields[i].Name
		if d.required {
			g.createGetterSetterEnum(schema, fieldName, entityName, enumFullName, providerName)
		} else {
			g.addImport("database/sql")
			g.createGetterSetterEnumNullable(schema, fieldName, entityName, enumFullName, providerName)
		}
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.addImport("database/sql")
		g.createGetterSetterBytesNullable(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.sliceStringsSets {
		g.addImport(g.enumsImport)
		if g.enums == nil {
			g.enums = make(map[string]bool)
			err := os.MkdirAll(path.Join(g.dir, "enums"), 0755)
			if err != nil {
				return err
			}
		}
		d := fields.sets[k]
		enumName := g.capitalizeFirst(d.name[strings.LastIndex(d.name, ".")+1:])
		_, enumCreated := g.enums[fields.fields[i].Name]
		if !enumCreated {
			err := g.createEnumDefinition(d, enumName)
			if err != nil {
				return err
			}
		}
		enumFullName := "enums." + enumName
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("strings")
		if d.required {
			g.createGetterSetterSet(schema, fieldName, entityName, enumFullName, providerName)
		} else {
			g.createGetterSetterSetNullable(schema, fieldName, entityName, enumFullName, providerName)
		}
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterBoolNullable(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterFloatNullable(schema, fieldName, entityName, providerName, fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
	}
	for _, i := range fields.timesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTimeNullable(schema, fieldName, entityName, providerName, false)
	}
	for _, i := range fields.datesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTimeNullable(schema, fieldName, entityName, providerName, true)
	}
	for _, subFields := range fields.structsFields {
		err := g.generateGettersSetters(entityName, providerName, schema, subFields)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *codeGenerator) createEnumDefinition(d *enumDefinition, name string) error {
	fileName := path.Join(g.dir, "enums", fmt.Sprintf("%s.go", name))
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	g.writeToFile(f, "package enums\n")
	g.writeToFile(f, "\n")
	g.writeToFile(f, fmt.Sprintf("type %s string\n", name))
	g.writeToFile(f, "")
	g.writeToFile(f, fmt.Sprintf("var %sList = struct {\n", name))
	for i := range d.fields {
		tName := d.t.Field(i).Name
		g.writeToFile(f, fmt.Sprintf("\t%s %s\n", tName, name))
	}
	g.writeToFile(f, "}{\n")
	for i, v := range d.fields {
		tName := d.t.Field(i).Name
		g.writeToFile(f, fmt.Sprintf("\t%s: \"%s\",\n", tName, v))
	}
	g.writeToFile(f, "}\n")
	return nil
}

func (g *codeGenerator) addLine(line string) {
	g.body += line + "\n"
}

func (g *codeGenerator) appendToLine(value string) {
	g.body += value
}

func (g *codeGenerator) writeToFile(f *os.File, value string) {
	_, _ = f.WriteString(value)
}

func (g *codeGenerator) capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] = b[0] - ('a' - 'A')
	}
	return string(b)
}

func (g *codeGenerator) lowerFirst(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'A' && b[0] <= 'Z' {
		b[0] = b[0] + ('a' - 'A')
	}
	return string(b)
}

func (g *codeGenerator) addSQLRowLines(fields *tableFields) string {
	result := ""
	for range fields.uIntegers {
		result += fmt.Sprintf("\tF%d uint64\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.references {
		if fields.referencesRequired[k] {
			result += fmt.Sprintf("\tF%d uint64\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.integers {
		result += fmt.Sprintf("\tF%d int64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.booleans {
		result += fmt.Sprintf("\tF%d bool\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floats {
		result += fmt.Sprintf("\tF%d float64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.times {
		result += fmt.Sprintf("\tF%d time.Time\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.dates {
		result += fmt.Sprintf("\tF%d time.Time\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.strings {
		if fields.stringsRequired[k] {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.uIntegersNullable {
		result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.integersNullable {
		result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.bytes {
		result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		if d.required {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.booleansNullable {
		result += fmt.Sprintf("\tF%d sql.NullBool\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floatsNullable {
		result += fmt.Sprintf("\tF%d sql.NullFloat64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.timesNullable {
		result += fmt.Sprintf("\tF%d sql.NullTime\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.datesNullable {
		result += fmt.Sprintf("\tF%d sql.NullTime\n", g.filedIndex)
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		result += g.addSQLRowLines(subFields)
	}
	return result
}

func (g *codeGenerator) addRedisBindSetLines(schema *entitySchema, fields *tableFields) {
	for range fields.uIntegers {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for k := range fields.references {
		if fields.referencesRequired[k] {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
			g.addLine("\t} else {")
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.Int64", g.filedIndex+1, g.filedIndex))
			g.addLine("\t}")
		}
		g.filedIndex++
	}
	for range fields.integers {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for range fields.booleans {
		g.addLine(fmt.Sprintf("\tif r.F%d  {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"1\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t} else {"))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"0\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t}"))
		g.filedIndex++
	}
	for k := range fields.floats {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = strconv.FormatFloat(r.F%d, 'f', %d, %d)", g.filedIndex+1, g.filedIndex, fields.floatsPrecision[k], fields.floatsSize[k]))
		g.filedIndex++
	}
	for range fields.times {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.Unix()", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for range fields.dates {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.Unix()", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for k := range fields.strings {
		if fields.stringsRequired[k] {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		}
		g.filedIndex++
	}
	for range fields.uIntegersNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = uint64(r.F%d.Int64)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.integersNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.Int64", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
			g.addLine("\t} else {")
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
			g.addLine("\t}")
		}
		g.filedIndex++
	}
	for range fields.bytes {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		g.addImport("strings")
		if d.required {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		}
		g.filedIndex++
	}
	for range fields.booleansNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tif r.F%d.Bool {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tredisListValues[%d] = \"1\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t\t} else {"))
		g.addLine(fmt.Sprintf("\t\t\tredisListValues[%d] = \"0\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t\t}"))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.floatsNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatFloat(r.F%d.Float64, 'f', %d, %d)", g.filedIndex+1, g.filedIndex, fields.floatsNullablePrecision[k], fields.floatsNullableSize[k]))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.timesNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatInt(r.F%d.Time.Unix(), 10)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.datesNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatInt(r.F%d.Time.Unix(), 10)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		g.addRedisBindSetLines(schema, subFields)
	}
}

type Flushable interface {
	PrivateFlush() error
	PrivateFlushed()
	GetID() uint64
}
