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
	g.addImport("sync")
	g.addImport("github.com/latolukasz/fluxaorm")
	g.addLine(fmt.Sprintf("type %s struct {", providerNamePrivate))
	g.addLine("\ttableName string")
	g.addLine("\tdbCode string")
	g.addLine("\tredisCode string")
	g.addLine("\tcacheIndex uint64")
	g.addLine("\tuuidRedisKeyMutex *sync.Mutex")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("var %s = %s{", providerName, providerNamePrivate))
	g.addLine(fmt.Sprintf("\ttableName: \"%s\",", schema.tableName))
	g.addLine(fmt.Sprintf("\tdbCode: \"%s\",", schema.mysqlPoolCode))
	g.addLine(fmt.Sprintf("\tredisCode: \"%s\",", schema.getForcedRedisCode()))
	g.addLine(fmt.Sprintf("\tcacheIndex: %d,", g.cacheIndex))
	g.cacheIndex++
	g.addLine(fmt.Sprintf("\tuuidRedisKeyMutex: &sync.Mutex{},"))
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) GetByID(ctx fluxaorm.Context, id uint64) (entity *%s, found bool, err error) {", providerNamePrivate, entityName))

	g.appendToLine("\tquery := \"SELECT `ID`")
	for _, columnName := range schema.GetColumns()[1:] {
		g.appendToLine(",`" + columnName + "`")
	}
	g.addLine(fmt.Sprintf(" FROM `%s` WHERE `ID` = ? LIMIT 1\"", schema.tableName))
	g.addLine(fmt.Sprintf("\tparams := make([]any, %d)", len(schema.columnNames)))
	g.filedIndex = 0
	g.addLine(g.addQueryParamsLines(schema.fields))
	g.filedIndex = 0
	g.appendToLine(fmt.Sprintf("\tfound, err = ctx.Engine().DB(%s.dbCode).QueryRow(ctx, fluxaorm.NewWhere(query, id), &params[0]", providerName))
	for i := 1; i < len(schema.columnNames); i++ {
		g.appendToLine(fmt.Sprintf(", &params[%d]", i))
	}
	g.addLine(")")
	g.addLine("\tif err != nil || !found {")
	g.addLine("\t\treturn nil, false, err")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn &%s{ctx: ctx, id: id, originDatabaseValues: params}, true, nil", entityName))
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
	g.addLine(fmt.Sprintf("\te := &%s{ctx: ctx, new: true, id: id}", entityName))
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
	g.addLine("\tconvertedValues []any")
	g.addLine("\toriginDatabaseValues []any")
	g.addLine("\tdatabaseBind fluxaorm.Bind")
	if schema.hasRedisCache {
		g.addLine("\tredisBind map[int]string")
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

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlush() error {", entityName))
	g.addLine("\tif e.new {")
	insertQueryLine := "\t\tsqlQuery := \"INSERT INTO `" + schema.tableName + "` (`ID`"
	for _, columnName := range schema.GetColumns()[1:] {
		insertQueryLine += ",`" + columnName + "`"
	}
	insertQueryLine += fmt.Sprintf(") VALUES (?%s)\"\n", strings.Repeat(",?", len(schema.columnNames)-1))
	insertQueryLine += fmt.Sprintf("\t\te.originDatabaseValues = make([]any, %d)\n", len(schema.columnNames))
	g.filedIndex = 0
	insertQueryLine += g.addBindSetLines(schema.fields)
	insertQueryLine += fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, e.originDatabaseValues...)", providerName)
	g.addLine(insertQueryLine)
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
	err = g.generateGettersSetters(entityName, schema, schema.fields)
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

type getterSetterGenerateSettings struct {
	ValueType               string
	FromRedisCode           string
	ToRedisCode             string
	FromConverted           string
	DefaultValue            string
	DatabaseBindConvertCode string
	AfterConvertedSet       string
	OriginDatabaseCompare   string
}

func (g *codeGenerator) generateGetterSetter(entityName, fieldName string, schema *entitySchema, settings getterSetterGenerateSettings) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, settings.ValueType))
	g.addLine("\tif e.convertedValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.convertedValues[%d]; value != nil {", g.filedIndex))
	g.addLine(settings.FromConverted)
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\tif !e.new {")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originDatabaseValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\t\treturn value.(%s)", settings.ValueType))
		g.addLine("\t\t\t}")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tvar v %s", settings.ValueType))
		g.addLine(fmt.Sprintf("\t\tif value := e.originRedisValues[%d]; value != \"\" {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\t%s", settings.FromRedisCode))
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\te.convertedValues[%d] = v", g.filedIndex))
		g.addLine("\t\treturn v")
	} else {
		g.addLine(fmt.Sprintf("\t\treturn e.originDatabaseValues[%d].(%s)", g.filedIndex, settings.ValueType))
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn %s", settings.DefaultValue))
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, settings.ValueType))
	g.addLine("\tif e.convertedValues == nil {")
	g.addLine(fmt.Sprintf("\t\te.convertedValues = make([]any, %d)", len(schema.columnNames)))
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\te.convertedValues[%d] = value", g.filedIndex))
	if settings.AfterConvertedSet != "" {
		g.addLine(settings.AfterConvertedSet)
	}
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.originDatabaseValues != nil {")
	if settings.OriginDatabaseCompare != "" {
		g.addLine(fmt.Sprintf("\t\t\t%s", settings.OriginDatabaseCompare))
	} else {
		g.addLine(fmt.Sprintf("\t\t\tif e.originDatabaseValues[%d] == value {", g.filedIndex))
	}
	g.addLine(fmt.Sprintf("\t\t\t\tdelete(e.databaseBind, \"%s\")", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\t\t\tdelete(e.redisBind, %d)", g.filedIndex))
	}
	g.addLine("\t\t\t\treturn")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		if settings.ToRedisCode != "" {
			g.addLine(fmt.Sprintf("\t\t%s", settings.ToRedisCode))
		}
		g.addLine(fmt.Sprintf("\t\tif e.originRedisValues[%d] == asString {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\t\tdelete(e.redisBind, %d)", g.filedIndex))
		g.addLine("\t\t\treturn")
		g.addLine("\t\t}")
		g.addLine("\t\tif e.redisBind == nil {")
		g.addLine("\t\t\te.redisBind = make(map[int]string)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\te.redisBind[%d] = asString", g.filedIndex))
	}
	g.addLine("\t\tif e.databaseBind == nil {")
	g.addLine("\t\t\te.databaseBind = fluxaorm.Bind{}")
	g.addLine("\t\t}")
	if settings.DatabaseBindConvertCode != "" {
		g.addLine(fmt.Sprintf("\t\t%s", settings.DatabaseBindConvertCode))
	} else {
		g.addLine(fmt.Sprintf("\t\te.databaseBind[\"%s\"] = value", fieldName))
	}
	g.addLine("\t}")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterUint64(schema *entitySchema, fieldName, entityName, getterSuffix string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(uint64)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn 0")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value uint64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d] == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d] == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterInt64(schema *entitySchema, fieldName, entityName string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(int64)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn 0")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value int64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d] == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d] == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBool(schema *entitySchema, fieldName, entityName string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(bool)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn false")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value bool) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis := e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d] == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d] == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterFloat(schema *entitySchema, fieldName, entityName string, precision int) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(float64)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn 0")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value float64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(fromRedis*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", precision, precision))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(e.originDatabaseValues[%d].(float64)*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", g.filedIndex, precision, precision))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif math.Round(e.originDatabaseValues[%d].(float64)*math.Pow10(%d)) == math.Round(value*math.Pow10(%d)) {", g.filedIndex, precision, precision))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterTime(schema *entitySchema, fieldName, entityName string, dateOnly bool) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(time.Time)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) {", entityName, fieldName))
	if dateOnly {
		g.addLine("\tvalue = value.Truncate(time.Hour * 24)")
	} else {
		g.addLine("\tvalue = value.Truncate(time.Second)")
	}
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value.Unix()")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d].(time.Time).Unix() == value.Unix()", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d].(time.Time).Unix() == value.Unix() {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterString(schema *entitySchema, fieldName, entityName string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\treturn value.(string)")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn \"\"")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value string) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = value", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d].(string) == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d].(string) == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = value", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterUint64Nullable(schema *entitySchema, fieldName, entityName, getterSuffix string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\tvNullable := value.(sql.NullInt64)")
	g.addLine("\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\tasUint64 := uint64(vNullable.Int64)")
	g.addLine("\t\t\t\treturn &asUint64")
	g.addLine("\t\t\t}")
	g.addLine("\t\t\treturn nil")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *uint64) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullInt64{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.Int64 = int64(*value)")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = bindValue", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = strconv.FormatUint(*value, 10)")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d].(sql.NullInt64) == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d].(sql.NullInt64) == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = bindValue", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterStringNullable(schema *entitySchema, fieldName, entityName string) {
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
	g.addLine("\tif e.originDatabaseValues != nil {")
	g.addLine(fmt.Sprintf("\t\tif value := e.originDatabaseValues[%d]; value != nil {", g.filedIndex))
	g.addLine("\t\t\tvNullable := value.(sql.NullString)")
	g.addLine("\t\t\tif vNullable.Valid {")
	g.addLine("\t\t\t\treturn &vNullable.String")
	g.addLine("\t\t\t}")
	g.addLine("\t\t\treturn nil")
	g.addLine("\t\t}")
	g.addLine("\t}")
	g.addLine("\treturn nil")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *string) {", entityName, fieldName))
	g.addLine("\tbindValue := sql.NullString{}")
	g.addLine("\tif value != nil {")
	g.addLine("\t\tbindValue.Valid = true")
	g.addLine("\t\tbindValue.String = *value")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues[%d] = bindValue", g.filedIndex))
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine("\t\tasString := \"\"")
		g.addLine("\t\tif value != nil {")
		g.addLine("\t\t\tasString = *value")
		g.addLine("\t\t}")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues[%d].(sql.NullString) == bindValue", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues[%d].(sql.NullString) == bindValue {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.databaseBind[\"%s\"] = bindValue", fieldName))
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) generateGettersSetters(entityName string, schema *entitySchema, fields *tableFields) error {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			g.filedIndex++
			continue
		}
		g.createGetterSetterUint64(schema, fieldName, entityName, "")
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		refTypeName := schema.references[fieldName].Type.String()
		refName := g.capitalizeFirst(refTypeName[strings.LastIndex(refTypeName, ".")+1:])
		required := fields.referencesRequired[k]
		if required {
			g.createGetterSetterUint64(schema, fieldName, entityName, "ID")
		} else {
			g.createGetterSetterUint64Nullable(schema, fieldName, entityName, "ID")
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
		g.createGetterSetterInt64(schema, fieldName, entityName)
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterBool(schema, fieldName, entityName)
	}
	for k, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterFloat(schema, fieldName, entityName, fields.floatsPrecision[k])
	}
	for _, i := range fields.times {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, false)
	}
	for _, i := range fields.dates {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, true)
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.stringsRequired[k] {
			g.createGetterSetterString(schema, fieldName, entityName)
		} else {
			g.addImport("database/sql")
			g.createGetterSetterStringNullable(schema, fieldName, entityName)
		}
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		fromConverted := "\t\t\tv := value.(sql.NullInt64)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\tasUint64 := uint64(v.Int64)\n\t\t\t\treturn &asUint64\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*uint64",
			FromRedisCode: "vSource, _ := strconv.ParseUint(value, 10, 64)\n\t\t\tv = &vSource",
			ToRedisCode:   "var asString string\n\t\t\tif value != nil {\n\t\t\tasString = strconv.FormatUint(*value, 10)\n\t\t}",
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		fromConverted := "\t\t\tv := value.(sql.NullInt64)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn &v.Int64\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*int64",
			FromRedisCode: "vSource, _ := strconv.ParseInt(value, 10, 64)\n\t\t\tv = &vSource",
			ToRedisCode:   "var asString string\n\t\t\tif value != nil {\n\t\t\tasString = strconv.FormatInt(*value, 10)\n\t\t}",
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
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
			settings := getterSetterGenerateSettings{
				ValueType:     enumFullName,
				FromRedisCode: fmt.Sprintf("v = %s(value)", enumFullName),
				ToRedisCode:   "asString := string(value)",
				FromConverted: fmt.Sprintf("\t\t\treturn value.(%s)", enumFullName),
				DefaultValue:  fmt.Sprintf("\"%s\"", d.defaultValue),
			}
			g.generateGetterSetter(entityName, fieldName, schema, settings)
		} else {
			g.addImport("database/sql")
			settings := getterSetterGenerateSettings{
				ValueType:     "*" + enumFullName,
				FromRedisCode: fmt.Sprintf("v2 := %s(value)\n\t\t\tv = &v2", enumFullName),
				ToRedisCode:   "asString := string(*value)",
				FromConverted: fmt.Sprintf("\t\t\treturn value.(*%s)", enumFullName),
				DefaultValue:  "nil",
			}
			g.generateGetterSetter(entityName, fieldName, schema, settings)
		}
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		fromConverted := "\t\t\tv := value.(sql.NullString)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn []uint8(v.String)\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:             "[]uint8",
			FromRedisCode:         "v = []uint8(value)",
			ToRedisCode:           "",
			FromConverted:         fromConverted,
			DefaultValue:          "nil",
			AfterConvertedSet:     "\tasString := fmt.Sprintf(\"%v\", value)",
			OriginDatabaseCompare: fmt.Sprintf("if e.originDatabaseValues[%d] == asString {", g.filedIndex),
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
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
		if schema.hasRedisCache {
			g.addImport("strings")
		}
		g.addImport("fmt")
		settings := getterSetterGenerateSettings{
			ValueType:             "[]" + enumFullName,
			FromRedisCode:         fmt.Sprintf("values := strings.Split(value, \",\")\n\t\t\tv = make([]%s, len(values))\n\t\t\tfor k, code := range values {\n\t\t\t\tv[k] = enums.TestGenerateEnum(code)\n\t\t\t}", enumFullName),
			ToRedisCode:           "",
			FromConverted:         fmt.Sprintf("\t\t\treturn value.([]%s)", enumFullName),
			DefaultValue:          "nil",
			AfterConvertedSet:     "\tasString := fmt.Sprintf(\"%v\", value)",
			OriginDatabaseCompare: fmt.Sprintf("if e.originDatabaseValues[%d] == asString {", g.filedIndex),
		}
		if d.required {
			settings.DefaultValue = fmt.Sprintf("[]%s{\"%s\"}", enumFullName, d.defaultValue)
			settings.DatabaseBindConvertCode = fmt.Sprintf("e.databaseBind[\"%s\"] = asString", fieldName)
		} else {
			g.addImport("database/sql")
			settings.DatabaseBindConvertCode = "if len(value) == 0 {\n"
			settings.DatabaseBindConvertCode += fmt.Sprintf("\t\t\te.databaseBind[\"%s\"] = nil\n", fieldName)
			settings.DatabaseBindConvertCode += "\t\t} else {\n"
			settings.DatabaseBindConvertCode += fmt.Sprintf("\t\t\te.databaseBind[\"%s\"] = asString\n", fieldName)
			settings.DatabaseBindConvertCode += "\t\t}"
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		fromConverted := "\t\t\tv := value.(sql.NullBool)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn &v.Bool\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*bool",
			FromRedisCode: "vSource := value == \"1\"\n\t\t\tv = &vSource",
			ToRedisCode:   "var asString string\n\t\tif value != nil {\n\t\t\tif *value { asString = \"1\" } else { asString = \"0\" }\n\t\t}",
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for k, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		fromConverted := "\t\t\tv := value.(sql.NullFloat64)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn &v.Float64\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*float64",
			FromRedisCode: "vSource, _ := strconv.ParseFloat(value, 64)\n\t\t\tv = &vSource",
			ToRedisCode:   fmt.Sprintf("var asString string\n\t\t\tif value != nil {\n\t\t\tasString = strconv.FormatFloat(*value, 'f', %d, %d)\n\t\t}", fields.floatsPrecision[k], fields.floatsSize[k]),
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for _, i := range fields.timesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		fromConverted := "\t\t\tv := value.(sql.NullTime)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn &v.Time\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*time.Time",
			FromRedisCode: "vSource, _ := time.ParseInLocation(time.DateTime, value, time.UTC)\n\t\t\tv = &vSource",
			ToRedisCode:   "var asString string\n\t\t\tif value != nil {\n\t\t\tasString = value.Format(time.DateTime)\n\t\t}",
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for _, i := range fields.datesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		fromConverted := "\t\t\tv := value.(sql.NullTime)"
		fromConverted += "\n\t\t\tif v.Valid {\n\t\t\t\treturn &v.Time\n\t\t\t}"
		fromConverted += "\n\t\t\treturn nil"
		settings := getterSetterGenerateSettings{
			ValueType:     "*time.Time",
			FromRedisCode: "vSource, _ := time.ParseInLocation(time.DateOnly, value, time.UTC)\n\t\t\tv = &vSource",
			ToRedisCode:   "var asString string\n\t\t\tif value != nil {\n\t\t\tasString = value.Format(time.DateOnly)\n\t\t}",
			FromConverted: fromConverted,
			DefaultValue:  "nil",
		}
		g.generateGetterSetter(entityName, fieldName, schema, settings)
	}
	for _, subFields := range fields.structsFields {
		err := g.generateGettersSetters(entityName, schema, subFields)
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

func (g *codeGenerator) addQueryParamsLines(fields *tableFields) string {
	result := ""
	for range fields.uIntegers {
		result += fmt.Sprintf("\tparams[%d] = uint64(0)\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.references {
		if fields.referencesRequired[k] {
			result += fmt.Sprintf("\tparams[%d] = uint64(0)\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tparams[%d] = sql.NullInt64{}\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.integers {
		result += fmt.Sprintf("\tparams[%d] = int64(0)\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.booleans {
		result += fmt.Sprintf("\tparams[%d] = false\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floats {
		result += fmt.Sprintf("\tparams[%d] = float64(0)\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.times {
		result += fmt.Sprintf("\tparams[%d] = time.Time{}\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.dates {
		result += fmt.Sprintf("\tparams[%d] = time.Time{}\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.strings {
		if fields.stringsRequired[k] {
			result += fmt.Sprintf("\tparams[%d] = \"\"\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tparams[%d] = sql.NullString{}\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.uIntegersNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullInt64{}\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.integersNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullInt64{}\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			result += fmt.Sprintf("\tparams[%d] = \"\"\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tparams[%d] = sql.NullString{}\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.bytes {
		result += fmt.Sprintf("\tparams[%d] = sql.NullString{}\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		if d.required {
			result += fmt.Sprintf("\tparams[%d] = \"\"\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tparams[%d] = sql.NullString{}\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.booleansNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullBool{}\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floatsNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullFloat64{}\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.timesNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullTime{}\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.datesNullable {
		result += fmt.Sprintf("\tparams[%d] = sql.NullTime{}\n", g.filedIndex)
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		result += g.addQueryParamsLines(subFields)
	}
	return result
}

func (g *codeGenerator) addBindSetLines(fields *tableFields) string {
	result := ""
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.referencesRequired[k] {
			result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%sID()\n", g.filedIndex, fieldName)
		} else {
			result += fmt.Sprintf("\t\tvalue%s := e.Get%sID()\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{}\n", g.filedIndex)
			result += "\t\t} else {\n"
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{Valid: true, Int64: int64(*value%s)}\n", g.filedIndex, fieldName)
			result += "\t\t}\n"
		}
		g.filedIndex++
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for _, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for _, i := range fields.times {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for _, i := range fields.dates {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		g.filedIndex++
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.stringsRequired[k] {
			result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = e.Get%s()\n", g.filedIndex, fieldName)
		} else {
			result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{}\n", g.filedIndex)
			result += "\t\t} else {\n"
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{Valid: true, String: *value%s}\n", g.filedIndex, fieldName)
			result += "\t\t}\n"
		}
		g.filedIndex++
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{Valid: true, Int64: int64(*value%s)}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullInt64{Valid: true, Int64: *value%s}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for k, i := range fields.stringsEnums {
		d := fields.enums[k]
		fieldName := fields.prefix + fields.fields[i].Name
		if d.required {
			result += fmt.Sprintf("\t\te.originDatabaseValues[%d] = string(e.Get%s())\n", g.filedIndex, fieldName)
		} else {
			result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{}\n", g.filedIndex)
			result += "\t\t} else {\n"
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{Valid: true, String: string(*value%s)}\n", g.filedIndex, fieldName)
			result += "\t\t}\n"
		}
		g.filedIndex++
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{Valid: true, String: string(value%s)}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for k, i := range fields.sliceStringsSets {
		d := fields.sets[k]
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("strings")
		if d.required {
			result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tvalue%sStrings := make([]string, len(value%s))\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tfor i, v := range value%s {\n", fieldName)
			result += fmt.Sprintf("\t\t\tvalue%sStrings[i] = string(v)\n", fieldName)
			result += "\t\t}\n"
			result += fmt.Sprintf("\t\te.originDatabaseValues[%d] =  strings.Join(value%sStrings, \",\")\n", g.filedIndex, fieldName)
		} else {
			result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\tif value%s == nil {\n", fieldName)
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{}\n", g.filedIndex)
			result += "\t\t} else {\n"
			result += fmt.Sprintf("\t\t\tvalue%sStrings := make([]string, len(value%s))\n", fieldName, fieldName)
			result += fmt.Sprintf("\t\t\tfor i, v := range value%s {\n", fieldName)
			result += fmt.Sprintf("\t\t\t\tvalue%sStrings[i] = string(v)\n", fieldName)
			result += "\t\t\t}\n"
			result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullString{Valid: true, String: strings.Join(value%sStrings, \",\")}\n", g.filedIndex, fieldName)
			result += "\t\t}\n"
		}
		g.filedIndex++
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullBool{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullBool{Valid: true, Bool: *value%s}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for _, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullFloat64{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullFloat64{Valid: true, Float64: *value%s}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for _, i := range fields.timesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullTime{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullTime{Valid: true, Time: *value%s}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for _, i := range fields.datesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		result += fmt.Sprintf("\t\tvalue%s := e.Get%s()\n", fieldName, fieldName)
		result += fmt.Sprintf("\t\tif value%s == nil { \n", fieldName)
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullTime{}\n", g.filedIndex)
		result += "\t\t} else {\n"
		result += fmt.Sprintf("\t\t\te.originDatabaseValues[%d] = sql.NullTime{Valid: true, Time: *value%s}\n", g.filedIndex, fieldName)
		result += "\t\t}\n"
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		result += g.addBindSetLines(subFields)
	}
	return result
}

type Flushable interface {
	PrivateFlush() error
	PrivateFlushed()
	GetID() uint64
}
