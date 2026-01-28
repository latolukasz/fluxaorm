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
	g.addLine("\tuuidRedisKeyMutex *sync.Mutex")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("var %s = %s{", providerName, providerNamePrivate))
	g.addLine(fmt.Sprintf("\ttableName: \"%s\",", schema.tableName))
	g.addLine(fmt.Sprintf("\tdbCode: \"%s\",", schema.mysqlPoolCode))
	g.addLine(fmt.Sprintf("\tredisCode: \"%s\",", schema.getForcedRedisCode()))
	g.addLine(fmt.Sprintf("\tuuidRedisKeyMutex: &sync.Mutex{},"))
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (p %s) GetByID(ctx fluxaorm.Context, id uint64) (entity *%s, found bool, err error) {", providerNamePrivate, entityName))
	g.addLine("\treturn nil, false, nil")
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
	g.addLine(fmt.Sprintf("\te := &%s{new: true, bind: fluxaorm.Bind{}, id: id}", entityName))
	g.addLine("\te.bind[\"ID\"] = e.id")
	if schema.hasRedisCache {
		g.addImport("strconv")
		g.addLine("\te.redisValues[0] = strconv.FormatUint(e.id, 10)")
	}
	for k, i := range schema.fields.stringsEnums {
		def := schema.fields.enums[k]
		if def.required {
			fieldName := schema.fields.prefix + schema.fields.fields[i].Name
			g.addLine(fmt.Sprintf("\te.bind[\"%s\"] = \"%s\"", fieldName, def.defaultValue))
		}
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
	g.addLine("\tid uint64")
	g.addLine("\tnew bool")
	g.addLine(fmt.Sprintf("\tvalues [%d]any", len(schema.columnNames)))
	g.addLine("\tbind fluxaorm.Bind")
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\tredisValues [%d]string", len(schema.columnNames)))
	}
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) GetID() uint64 {", entityName))
	g.addLine("\treturn e.id")
	g.addLine("}")
	g.addLine("")
	g.addLine(fmt.Sprintf("func (e *%s) Delete() {", entityName))
	g.addLine("}")
	g.addLine("")
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

func (g *codeGenerator) generateGettersSetters(entityName string, schema *entitySchema, fields *tableFields) error {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			continue
		}
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		if fields.fields[i].Type.String() == "uint64" {
			g.addLine(fmt.Sprintf("\treturn e.values[%d].(uint64)", i))
		} else {
			g.addLine(fmt.Sprintf("\treturn %s(e.values[%d].(uint64))", fields.fields[i].Type.String(), i))
		}
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("\treturn 0")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("\treturn \"\"")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		g.addLine("}")
		g.addLine("")
	}
	for k, i := range fields.stringsEnums {
		if g.enums == nil {
			g.addImport(g.enumsImport)
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

		g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, enumFullName))
		g.addLine("\treturn \"\"")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, enumFullName))
		g.addLine("}")
		g.addLine("")
	}
	for k, i := range fields.sliceStringsSets {
		if g.enums == nil {
			g.addImport(g.enumsImport)
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

		g.addLine(fmt.Sprintf("func (e *%s) Get%s() []%s {", entityName, fieldName, enumFullName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(values ...%s) {", entityName, fieldName, enumFullName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() []uint8 {", entityName, fieldName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value []uint8) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() bool {", entityName, fieldName))
		g.addLine("\treturn false")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value bool) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() *bool {", entityName, fieldName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *bool) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() float64 {", entityName, fieldName))
		g.addLine("\treturn 0")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value float64) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() *float64 {", entityName, fieldName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *float64) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.timesNullable {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() *time.Time {", entityName, fieldName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *time.Time) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.datesNullable {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() *time.Time {", entityName, fieldName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *time.Time) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.times {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() time.Time {", entityName, fieldName))
		g.addLine("\treturn time.Now()")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.dates {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() time.Time {", entityName, fieldName))
		g.addLine("\treturn time.Now()")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		refTypeName := schema.references[fieldName].Type.String()
		refName := g.capitalizeFirst(refTypeName[strings.LastIndex(refTypeName, ".")+1:])
		g.addLine(fmt.Sprintf("func (e *%s) Get%s() *%s {", entityName, fieldName, refName))
		g.addLine("\treturn nil")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Get%sID() uint64 {", entityName, fieldName))
		g.addLine("\treturn 0")
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%s(value *%s) {", entityName, fieldName, refName))
		g.addLine("}")
		g.addLine("")
		g.addLine(fmt.Sprintf("func (e *%s) Set%sID(id uint64) {", entityName, fieldName))
		g.addLine("}")
		g.addLine("")
	}
	for i, subFields := range fields.structsFields {
		field := fields.fields[fields.structs[i]]
		prefixName := fields.prefix
		if !field.Anonymous {
			prefixName += field.Name
		}
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
