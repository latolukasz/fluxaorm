package fluxaorm

import (
	"fmt"
	"strings"
)

func (g *codeGenerator) generateProviderAndSQLRow(schema *entitySchema, names *entityNames) {
	g.addImport("sync")

	g.addLine(fmt.Sprintf("type %s struct {", names.providerNamePrivate))
	g.addLine("\ttableName string")
	g.addLine("\tdbCode string")
	g.addLine("\tredisCode string")
	g.addLine("\tcacheIndex uint64")
	g.addLine("\tuuidRedisKeyMutex *sync.Mutex")
	if schema.hasRedisCache {
		g.addLine("\tredisCachePrefix string")
		g.addLine("\tredisCacheStamp string")
		g.addLine("\tredisCacheTTL int")
	} else if schema.hasCachedUniqueIndexes {
		g.addLine("\tredisCachePrefix string")
	}
	if schema.hasRedisSearch {
		g.addLine("\tredisSearchCode   string")
		g.addLine("\tredisSearchIndex  string")
		g.addLine("\tredisSearchPrefix string")
	}
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("var %s = %s{", names.providerName, names.providerNamePrivate))
	g.addLine(fmt.Sprintf("\ttableName: \"%s\",", schema.tableName))
	g.addLine(fmt.Sprintf("\tdbCode: \"%s\",", schema.mysqlPoolCode))
	g.addLine(fmt.Sprintf("\tredisCode: \"%s\",", schema.getForcedRedisCode()))
	g.addLine(fmt.Sprintf("\tcacheIndex: %d,", g.cacheIndex))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\tredisCachePrefix: \"%s\",", schema.cacheKey+":"))
		g.addLine(fmt.Sprintf("\tredisCacheStamp: \"%s\",", schema.structureHash))
		g.addLine(fmt.Sprintf("\tredisCacheTTL: %d,", schema.cacheTTL))
	} else if schema.hasCachedUniqueIndexes {
		g.addLine(fmt.Sprintf("\tredisCachePrefix: \"%s\",", schema.cacheKey+":"))
	}
	if schema.hasRedisSearch {
		g.addLine(fmt.Sprintf("\tredisSearchCode:   \"%s\",", schema.redisSearchPoolCode))
		g.addLine(fmt.Sprintf("\tredisSearchIndex:  \"%s\",", schema.redisSearchIndex))
		g.addLine(fmt.Sprintf("\tredisSearchPrefix: \"%s\",", schema.redisSearchPrefix))
	}
	g.cacheIndex++
	g.addLine(fmt.Sprintf("\tuuidRedisKeyMutex: &sync.Mutex{},"))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("type %s struct {", names.sqlRowName))
	g.filedIndex = 0
	g.addLine(strings.TrimRight(g.addSQLRowLines(schema.fields), "\t\n"))
	g.filedIndex = 0
	g.addLine("}")
	g.addLine("")

	if schema.hasRedisCache {
		g.addImport("strconv")
		g.addLine(fmt.Sprintf("func (r *%s) redisValues() []any {", names.sqlRowName))
		g.addLine(fmt.Sprintf("\tredisListValues := make([]any, %d)", len(schema.columnNames)+1))
		g.addLine(fmt.Sprintf("\tredisListValues[0] = %s.redisCacheStamp", names.providerName))
		g.addRedisBindSetLines(schema, schema.fields)
		g.addLine("\treturn redisListValues")
		g.addLine("}")
		g.addLine("")
	}
}
