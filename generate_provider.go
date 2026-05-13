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
	g.addLine(fmt.Sprintf("\tFields %sFields", names.entityPrivate))
	if schema.hasRedisSearch {
		g.addLine(fmt.Sprintf("\tFieldsRedisSearch %sFieldsRedisSearch", names.entityPrivate))
	}
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("var %s = %s{", names.providerName, names.providerNamePrivate))
	g.addLine(fmt.Sprintf("\ttableName: \"%s\",", schema.tableName))
	g.addLine(fmt.Sprintf("\tdbCode: \"%s\",", schema.mysqlPoolCode))
	g.addLine(fmt.Sprintf("\tredisCode: \"%s\",", schema.getForcedRedisCode()))
	g.addLine(fmt.Sprintf("\tcacheIndex: %d,", schema.index))
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
	g.addLine(fmt.Sprintf("\tuuidRedisKeyMutex: &sync.Mutex{},"))
	g.generateTypedFieldsInit(schema, names)
	g.generateRedisSearchFieldsInit(schema, names)
	g.addLine("}")
	g.addLine("")

	// EntityProvider interface methods (all providers)
	g.addLine(fmt.Sprintf("func (p %s) TableName() string { return p.tableName }", names.providerNamePrivate))
	g.addLine(fmt.Sprintf("func (p %s) DBCode() string { return p.dbCode }", names.providerNamePrivate))
	g.addLine("")

	// RedisCacheEntityProvider interface methods (only when hasRedisCache)
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("func (p %s) RedisCode() string { return p.redisCode }", names.providerNamePrivate))
		g.addLine(fmt.Sprintf("func (p %s) RedisCachePrefix() string { return p.redisCachePrefix }", names.providerNamePrivate))
		g.addLine("")
		g.addLine(fmt.Sprintf("func (p %s) ClearRedisCache(ctx fluxaorm.Context) (int, error) {", names.providerNamePrivate))
		g.addLine("\tscript := `")
		g.addLine("local cursor = '0'")
		g.addLine("local deleted = 0")
		g.addLine("")
		g.addLine("repeat")
		g.addLine("  local result = redis.call('SCAN', cursor, 'MATCH', KEYS[1], 'COUNT', 1000)")
		g.addLine("  cursor = result[1]")
		g.addLine("  local keys = result[2]")
		g.addLine("")
		g.addLine("  if #keys > 0 then")
		g.addLine("    deleted = deleted + redis.call('UNLINK', unpack(keys))")
		g.addLine("  end")
		g.addLine("until cursor == '0'")
		g.addLine("")
		g.addLine("return deleted")
		g.addLine("`")
		g.addLine("\tres, err := ctx.Engine().Redis(p.redisCode).Eval(ctx, script, []string{p.redisCachePrefix + \"*\"})")
		g.addLine("\tif err != nil {")
		g.addLine("\t\treturn 0, err")
		g.addLine("\t}")
		g.addLine("\treturn int(res.(int64)), nil")
		g.addLine("}")
		g.addLine("")
	}

	// RedisSearchEntityProvider interface methods (only when hasRedisSearch)
	if schema.hasRedisSearch {
		g.addLine(fmt.Sprintf("func (p %s) RedisSearchCode() string { return p.redisSearchCode }", names.providerNamePrivate))
		g.addLine(fmt.Sprintf("func (p %s) RedisSearchIndexName() string { return p.redisSearchIndex }", names.providerNamePrivate))
		g.addLine(fmt.Sprintf("func (p %s) RedisSearchHashPrefix() string { return p.redisSearchPrefix }", names.providerNamePrivate))
		g.addLine("")
	}

	// DebeziumSubjectName method (only when entity has debezium tag)
	if schema.debeziumNatsPool != "" {
		g.addLine(fmt.Sprintf("func (p %s) DebeziumSubjectName(ctx fluxaorm.Context) string {", names.providerNamePrivate))
		g.addLine("\treturn \"fluxa_\" + p.dbCode + \".\" + ctx.Engine().DB(p.dbCode).GetConfig().GetDatabaseName() + \".\" + p.tableName")
		g.addLine("}")
		g.addLine("")
	}

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

	// OnAfterInsert
	g.addLine(fmt.Sprintf("func (p %s) OnAfterInsert(engine fluxaorm.Engine, handler func(ctx fluxaorm.Context, entity *%s) error) {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterEntityLoader(engine, p.cacheIndex, p.dbCode, func(ctx fluxaorm.Context, id uint64) (fluxaorm.Entity, bool, error) {"))
	g.addLine(fmt.Sprintf("\t\treturn p.GetByID(ctx, id)"))
	g.addLine("\t})")
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterAfterInsertHandler(engine, p.cacheIndex, func(ctx fluxaorm.Context, e fluxaorm.Entity) error {"))
	g.addLine(fmt.Sprintf("\t\treturn handler(ctx, e.(*%s))", names.entityName))
	g.addLine("\t})")
	g.addLine("}")
	g.addLine("")

	// OnAfterUpdate
	g.addLine(fmt.Sprintf("func (p %s) OnAfterUpdate(engine fluxaorm.Engine, handler func(ctx fluxaorm.Context, entity *%s, changes map[string]any) error) {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterEntityLoader(engine, p.cacheIndex, p.dbCode, func(ctx fluxaorm.Context, id uint64) (fluxaorm.Entity, bool, error) {"))
	g.addLine(fmt.Sprintf("\t\treturn p.GetByID(ctx, id)"))
	g.addLine("\t})")
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterAfterUpdateHandler(engine, p.cacheIndex, func(ctx fluxaorm.Context, e fluxaorm.Entity, changes map[string]any) error {"))
	g.addLine(fmt.Sprintf("\t\treturn handler(ctx, e.(*%s), changes)", names.entityName))
	g.addLine("\t})")
	g.addLine("}")
	g.addLine("")

	// OnAfterDelete
	g.addLine(fmt.Sprintf("func (p %s) OnAfterDelete(engine fluxaorm.Engine, handler func(ctx fluxaorm.Context, entity *%s) error) {", names.providerNamePrivate, names.entityName))
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterEntityLoader(engine, p.cacheIndex, p.dbCode, func(ctx fluxaorm.Context, id uint64) (fluxaorm.Entity, bool, error) {"))
	g.addLine(fmt.Sprintf("\t\treturn p.GetByID(ctx, id)"))
	g.addLine("\t})")
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterAfterDeleteHandler(engine, p.cacheIndex, func(ctx fluxaorm.Context, e fluxaorm.Entity) error {"))
	g.addLine(fmt.Sprintf("\t\treturn handler(ctx, e.(*%s))", names.entityName))
	g.addLine("\t})")
	g.addLine("}")
	g.addLine("")

	// BeforeInsert/BeforeUpdate/BeforeDelete callbacks (package-level, fire inside PrivateFlush before SQL)
	for _, hook := range []string{"BeforeInsert", "BeforeUpdate", "BeforeDelete"} {
		sliceName := names.entityPrivate + hook + "Callbacks"
		g.addLine(fmt.Sprintf("var %s []func(*%s)", sliceName, names.entityName))
		g.addLine(fmt.Sprintf("func Register%s%s(cb func(entity *%s)) {", names.entityName, hook, names.entityName))
		g.addLine(fmt.Sprintf("\t%s = append(%s, cb)", sliceName, sliceName))
		g.addLine("}")
		g.addLine("")
	}
}
