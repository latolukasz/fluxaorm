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

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlush() error {", names.entityName))
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
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
	g.addLine("\tif e.deleted {")
	g.addLine(fmt.Sprintf("\t\tsqlQuery := \"DELETE FROM `%s` WHERE `ID` = ?\"", schema.tableName))
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, e.GetID())", names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\te.ctx.RedisPipeLine(%s.redisCode).Del(%s.redisCachePrefix + strconv.FormatUint(e.GetID(), 10))", names.providerName, names.providerName))
	}
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
	g.addLine(fmt.Sprintf("\t\te.ctx.DatabasePipeLine(%s.dbCode).AddQuery(sqlQuery, updateParams...)", names.providerName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\t\tredisPipeLine := e.ctx.RedisPipeLine(%s.redisCode)", names.providerName))
		g.addLine(fmt.Sprintf("\t\tredisKey := %s.redisCachePrefix + strconv.FormatUint(e.GetID(), 10)", names.providerName))
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

	g.addLine(fmt.Sprintf("func (e *%s) PrivateFlushed() {", names.entityName))
	g.addLine("\tif e.new {")
	g.addLine("\t\te.new = false")
	g.addLine("\t}")
	g.addLine("}")
	g.addLine("")
}
