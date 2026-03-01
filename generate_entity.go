package fluxaorm

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

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
	names := &entityNames{
		entityName:          entityName,
		entityPrivate:       entityPrivate,
		providerName:        entityName + "Provider",
		providerNamePrivate: entityPrivate + "Provider",
		sqlRowName:          entityPrivate + "SQLRow",
	}

	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.generateProviderAndSQLRow(schema, names)
	g.generateGetByID(schema, names)
	g.generateGetByIDs(schema, names)
	g.generateNewMethods(schema, names)
	g.generateUniqueIndexGetters(schema, names)
	g.generateSearchWithCount(schema, names)
	g.generateSearch(schema, names)
	g.generateSearchIDsWithCount(schema, names)
	g.generateSearchIDs(schema, names)
	g.generateSearchOne(schema, names)
	if schema.hasRedisSearch {
		g.generateSearchInRedis(schema, names)
		g.generateSearchOneInRedis(schema, names)
		g.generateSearchInRedisWithCount(schema, names)
		g.generateSearchIDsInRedis(schema, names)
		g.generateSearchIDsInRedisWithCount(schema, names)
		g.generateReindexRedisSearch(schema, names)
	}
	g.generateUUID(schema, names)
	g.generateEntityStruct(schema, names)

	g.filedIndex = 0
	err = g.generateGettersSetters(names.entityName, names.providerName, schema, schema.fields)
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
