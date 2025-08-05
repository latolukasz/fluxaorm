package orm

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
)

type redisSearchIndexDefinition struct {
	FieldType  string
	Sortable   bool
	NoStem     bool
	IndexEmpty bool
}

type RedisSearchAlter struct {
	IndexName       string
	DocumentsInDB   uint64
	Pool            string
	Drop            bool
	IndexDefinition string
	indexOptions    *redis.FTCreateOptions
	indexSchema     []*redis.FieldSchema
}

func GetRedisSearchAlters(ctx Context) (alters []RedisSearchAlter) {
	indicesInDB := make(map[string]map[string]bool)
	indicesInEntities := make(map[string]map[string]bool)
	for poolName, pool := range ctx.Engine().Registry().RedisPools() {
		indicesInDB[poolName] = make(map[string]bool)
		indicesInEntities[poolName] = make(map[string]bool)
		for _, index := range pool.FTList(ctx) {
			indicesInDB[poolName][index] = true
		}
	}
	alters = make([]RedisSearchAlter, 0)
	for _, schemaInterface := range ctx.Engine().Registry().Entities() {
		schema := schemaInterface.(*entitySchema)
		if schema.redisSearchIndexName == "" {
			continue
		}
		redisPool, hasRedisCache := schema.GetRedisCache()
		if !hasRedisCache {
			redisPool = ctx.Engine().Redis(DefaultPoolCode)
		}
		indicesInEntities[redisPool.GetCode()][schema.GetTableName()] = true
		alter, hasAlter := getRedisIndexAlter(ctx, schema, redisPool)
		if hasAlter {
			alters = append(alters, *alter)
		}
	}
	for poolName, indices := range indicesInDB {
		for indexName := range indices {
			if !strings.HasPrefix(indexName, redisSearchIndexPrefix) {
				continue
			}
			_, has := indicesInEntities[poolName][indexName]
			if !has {
				info, valid := ctx.Engine().Redis(poolName).FTInfo(ctx, indexName)
				if valid {
					alter := RedisSearchAlter{
						IndexName:       indexName,
						Pool:            poolName,
						Drop:            true,
						IndexDefinition: createRedisSearchIndexDefinitionFromInfo(info),
					}
					alters = append(alters, alter)
				}
			}
		}
	}
	return alters
}

func (a RedisSearchAlter) Exec(ctx Context) {
	if a.Drop {
		ctx.Engine().Redis(a.Pool).FTDrop(ctx, a.IndexName, true)
		return
	}
	ctx.Engine().Redis(a.Pool).FTCreate(ctx, a.IndexName, a.indexOptions, a.indexSchema...)
	// TODO recreate hashes and drop
	ss
}

func getRedisIndexAlter(ctx Context, schema *entitySchema, r RedisCache) (alter *RedisSearchAlter, has bool) {
	_, isInRedis := r.FTInfo(ctx, schema.redisSearchIndexName)
	rsColumns := make([]*redis.FieldSchema, 0)
	for _, columnName := range schema.columnNames {
		def, isSearchable := schema.redisSearchFields[columnName]
		if !isSearchable {
			continue
		}
		rsColumn := &redis.FieldSchema{
			FieldName:  columnName,
			Sortable:   def.Sortable,
			NoStem:     def.NoStem,
			IndexEmpty: def.IndexEmpty,
		}
		switch def.FieldType {
		case "TEXT":
			rsColumn.FieldType = redis.SearchFieldTypeText
			break
		case "TAG":
			rsColumn.FieldType = redis.SearchFieldTypeTag
			break
		case "NUMERIC":
			rsColumn.FieldType = redis.SearchFieldTypeNumeric
			break
		}
		rsColumns = append(rsColumns, rsColumn)
	}
	options := &redis.FTCreateOptions{
		OnHash: true,
		Prefix: []any{schema.redisSearchIndexPrefix},
	}
	if !isInRedis {
		alter = &RedisSearchAlter{
			IndexName:    schema.redisSearchIndexName,
			Pool:         r.GetCode(),
			indexOptions: options,
			indexSchema:  rsColumns,
		}
		has = true
	}
	if !has {
		return nil, false
	}
	alter.IndexDefinition = schema.createRedisSearchIndexDefinition(alter.IndexName)
	query := fmt.Sprintf("SELECT COUNT(ID) FROM `%s`", schema.GetTableName())
	total := uint64(0)
	schema.GetDB().QueryRow(ctx, NewWhere(query), &total)
	alter.DocumentsInDB = total
	return
}

func createRedisSearchIndexDefinitionFromInfo(info *redis.FTInfoResult) string {
	query := "FT.CREATE " + info.IndexName + " ON HASH PREFIX "
	query += strconv.Itoa(len(info.IndexDefinition.Prefixes))
	for _, prefix := range info.IndexDefinition.Prefixes {
		query += " " + prefix
	}
	query += " SCHEMA"
	for _, attribute := range info.Attributes {
		query += " " + attribute.Attribute + " " + attribute.Type
		if attribute.Sortable {
			query += " SORTABLE"
		}
		if attribute.NoStem {
			query += " NOSTEM"
		}
	}
	return query
}

func (e *entitySchema) createRedisSearchIndexDefinition(indexName string) string {
	query := "FT.CREATE " + indexName + " ON HASH PREFIX 1 " + e.redisSearchIndexPrefix + " SCHEMA"
	for _, columnName := range e.columnNames {
		def, isSearchable := e.redisSearchFields[columnName]
		if !isSearchable {
			continue
		}
		query += " " + columnName + " " + def.FieldType
		if def.Sortable {
			query += " SORTABLE"
		}
		if def.NoStem {
			query += " NOSTEM"
		}
		if def.IndexEmpty {
			query += " INDEXEMPTY"
		}
	}
	return query
}
