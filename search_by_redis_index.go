package orm

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"sort"
	"strconv"
	"strings"
)

type redisSearchIndexDefinition struct {
	FieldType     string
	Sortable      bool
	NoStem        bool
	IndexEmpty    bool
	sqlFieldQuery string
}

type RedisSearchAlter struct {
	IndexName       string
	DocumentsInDB   uint64
	Pool            string
	Drop            bool
	IndexDefinition string
	indexOptions    *redis.FTCreateOptions
	indexSchema     []*redis.FieldSchema
	schema          *entitySchema
}

func GetRedisSearchAlters(ctx Context) (alters []RedisSearchAlter) {
	indicesInRedis := make(map[string]map[string]bool)
	indicesInEntities := make(map[string]map[string]bool)
	for poolName, pool := range ctx.Engine().Registry().RedisPools() {
		if pool.GetConfig().GetDatabaseNumber() > 0 {
			continue
		}
		indicesInRedis[poolName] = make(map[string]bool)
		indicesInEntities[poolName] = make(map[string]bool)
		for _, index := range pool.FTList(ctx) {
			indicesInRedis[poolName][index] = true
		}
	}
	fmt.Printf("%v\n", indicesInRedis)
	alters = make([]RedisSearchAlter, 0)
	for _, schemaInterface := range ctx.Engine().Registry().Entities() {
		schema := schemaInterface.(*entitySchema)
		if schema.redisSearchIndexName == "" {
			continue
		}
		redisPool := ctx.Engine().Redis(schema.redisSearchIndexPoolCode)
		indicesInEntities[redisPool.GetCode()][schema.redisSearchIndexName] = true
		alter, hasAlter := getRedisIndexAlter(ctx, schema, redisPool)
		if hasAlter {
			alters = append(alters, *alter)
		}
	}
	for poolName, indices := range indicesInRedis {
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
	sort.Slice(alters, func(i int, j int) bool {
		if !alters[i].Drop && alters[j].Drop {
			return true
		}
		return false
	})
	return alters
}

func (a RedisSearchAlter) Exec(ctx Context) {
	if a.Drop {
		ctx.Engine().Redis(a.Pool).FTDrop(ctx, a.IndexName, true)
		return
	}
	ctx.Engine().Redis(a.Pool).FTCreate(ctx, a.IndexName, a.indexOptions, a.indexSchema...)
	go func() {
		a.schema.ReindexRedisIndex(ctx.Clone())
	}()
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
			schema:       schema,
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

func (e *entitySchema) ReindexRedisIndex(ctx Context) {
	if e.redisSearchIndexName == "" {
		return
	}
	lastIDRedisKey := "lastID:" + e.redisSearchIndexName
	r := ctx.Engine().Redis(e.redisSearchIndexPoolCode)
	lastID := uint64(0)
	lastIDInRedis, hasLastID := r.Get(ctx, lastIDRedisKey)
	if hasLastID {
		lastID, _ = strconv.ParseUint(lastIDInRedis, 10, 64)
	} else {
		r.Set(ctx, lastIDRedisKey, "0", 0)
	}
	where := "SELECT ID"
	pointers := make([]any, len(e.redisSearchFields)+1)
	var id uint64
	pointers[0] = &id
	i := 1
	for _, def := range e.redisSearchFields {
		where += ", " + def.sqlFieldQuery
		var value string
		pointers[i] = &value
		i++
	}
	where += " FROM `" + e.GetTableName() + "` WHERE ID > ? ORDER BY ID LIMIT 0, 5000"
	pStmt, pClose := e.GetDB().Prepare(ctx, where)
	defer pClose()
	for {
		func() {
			rows, cl := pStmt.Query(ctx, lastID)
			defer cl()
			i = 0
			for rows.Next() {
				rows.Scan(pointers...)
				i++
				lastID = *pointers[0].(*uint64)
			}
		}()
		if i < 5000 {
			r.Del(ctx, lastIDRedisKey)
			break
		} else {
			r.Set(ctx, lastIDRedisKey, "0", 0)
		}
	}

}
