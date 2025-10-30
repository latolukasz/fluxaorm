package fluxaorm

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"sort"
	"strconv"
	"strings"
	"time"
)

type redisSearchIndexDefinition struct {
	FieldType              string
	Sortable               bool
	NoStem                 bool
	IndexEmpty             bool
	sqlFieldQuery          string
	convertBindToHashValue func(any) any
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
	a.schema.ReindexRedisIndex(ctx)
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

type RedisSearchQuery struct {
	Query   string
	SortBy  []RedisSearchSortBy
	Filters []RedisSearchFilter
	Tags    string
}

func (q *RedisSearchQuery) AddSortBy(fieldName string, desc bool) {
	q.SortBy = append(q.SortBy, RedisSearchSortBy{fieldName, desc})
}

func (q *RedisSearchQuery) AddFilterTag(fieldName string, tag ...string) {
	if len(tag) == 0 {
		return
	}
	if q.Tags != "" {
		q.Tags += " "
	}
	q.Tags += "@" + fieldName + fmt.Sprintf(":{%s}", strings.Join(tag, "|"))
}

func (q *RedisSearchQuery) AddFilterBoolean(fieldName string, value bool) {
	if value {
		q.AddFilterTag(fieldName, "1")
	} else {
		q.AddFilterTag(fieldName, "0")
	}
}

func (q *RedisSearchQuery) AddFilterNumberRange(fieldName string, min, max int64) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, min, max})
}

func (q *RedisSearchQuery) AddFilterNumber(fieldName string, value int64) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, value, value})
}

func (q *RedisSearchQuery) AddFilterNumberGreaterEqual(fieldName string, value int64) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, value, nil})
}

func (q *RedisSearchQuery) AddFilterNumberLessEqual(fieldName string, value int64) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, nil, value})
}

func (q *RedisSearchQuery) AddFilterDateRange(fieldName string, min, max time.Time) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, min, max})
}

func (q *RedisSearchQuery) AddFilterDate(fieldName string, value time.Time) {
	q.Filters = append(q.Filters, RedisSearchFilter{fieldName, value, value})
}

type RedisSearchSortBy struct {
	FieldName string
	Desc      bool
}

type RedisSearchFilter struct {
	FieldName string
	Min       any
	Max       any
}

func RedisSearchIDs[E any](ctx Context, query *RedisSearchQuery, pager *Pager) (results []uint64, totalRows int) {
	return redisSearchIDs(ctx, GetEntitySchema[E](ctx), query, pager)
}

func RedisSearch[E any](ctx Context, query *RedisSearchQuery, pager *Pager) (results EntityIterator[E], totalRows int) {
	ids, totalRows := redisSearchIDs(ctx, GetEntitySchema[E](ctx), query, pager)
	schema := getEntitySchema[E](ctx)
	if schema.hasLocalCache {
		if totalRows == 0 {
			return &emptyResultsIterator[E]{}, 0
		}
		return &localCacheIDsIterator[E]{orm: ctx.(*ormImplementation), schema: schema, ids: ids, index: -1}, totalRows
	}
	return GetByIDs[E](ctx, ids...), totalRows
}

func RedisSearchOne[E any](ctx Context, query *RedisSearchQuery) (entity *E, found bool) {
	if query == nil {
		query = &RedisSearchQuery{}
	}
	ids, total := RedisSearchIDs[E](ctx, query, NewPager(1, 1))
	if total == 0 {
		return nil, false
	}
	e, found := GetByID[E](ctx, ids[0])
	if !found {
		return nil, false
	}
	return e, true
}

func (e *entitySchema) ReindexRedisIndex(ctx Context) {
	if e.redisSearchIndexName == "" {
		return
	}
	lastIDRedisKey := "lastID:" + e.redisSearchIndexName
	r := ctx.Engine().Redis(e.redisSearchIndexPoolCode)
	scanCursor := uint64(0)
	keysPattern := e.redisSearchIndexPrefix + "*"
	for {
		keys, newCursor := r.Scan(ctx, scanCursor, keysPattern, 1000)
		if len(keys) > 0 {
			p := ctx.RedisPipeLine(r.GetConfig().GetCode())
			p.Del(keys...)
			p.Exec(ctx)
		}
		if len(keys) < 1000 {
			break
		}
		scanCursor = newCursor
	}
	lastID, hasLastID := r.Get(ctx, lastIDRedisKey)
	if !hasLastID {
		lastID = "0"
		r.Set(ctx, lastIDRedisKey, lastID, 0)
	}
	where := "SELECT ID"
	pointers := make([]any, len(e.redisSearchFields)+1)
	var id string
	pointers[0] = &id
	i := 1
	columns := make([]string, len(e.redisSearchFields))
	for columnName, def := range e.redisSearchFields {
		where += ", " + def.sqlFieldQuery
		var value string
		pointers[i] = &value
		columns[i-1] = columnName
		i++
	}
	where += " FROM `" + e.GetTableName() + "` WHERE ID > ? ORDER BY ID LIMIT 0, 3000"
	pStmt, pClose := e.GetDB().Prepare(ctx, where)
	defer pClose()
	for {
		p := ctx.RedisPipeLine(r.GetConfig().GetCode())
		func() {
			rows, cl := pStmt.Query(ctx, lastID)
			defer cl()
			i = 0
			for rows.Next() {
				rows.Scan(pointers...)
				lastID = id
				i++
				values := make([]any, len(e.redisSearchFields)*2)
				k := 0
				for j, columnName := range columns {
					values[k] = columnName
					values[k+1] = *pointers[j+1].(*string)
					k += 2
				}
				p.HSet(e.redisSearchIndexPrefix+id, values...)
			}
		}()
		if i < 3000 {
			p.Del(lastIDRedisKey)
			p.Exec(ctx)
			break
		} else {
			p.Set(lastIDRedisKey, "0", 0)
			p.Exec(ctx)
		}
	}
}

func redisSearchIDs(ctx Context, schema EntitySchema, query *RedisSearchQuery, pager *Pager) (ids []uint64, total int) {
	indexName := schema.GetRedisSearchIndexName()
	if indexName == "" {
		panic(fmt.Errorf("entity %s is not searchable by Redis Search", schema.GetType().Name()))
	}
	r := ctx.Engine().Redis(schema.GetRedisSearchPoolCode())
	isDragonFlyDB := r.(*redisCache).dragonfly
	searchOptions := &redis.FTSearchOptions{
		NoContent: true,
	}
	q := "*"
	if query != nil {
		if query.Query != "" {
			q = query.Query
		}
		if pager != nil {
			searchOptions.LimitOffset = (pager.GetCurrentPage() - 1) * pager.GetPageSize()
			searchOptions.Limit = pager.GetPageSize()
		}
		for _, sortOption := range query.SortBy {
			sortBy := redis.FTSearchSortBy{FieldName: sortOption.FieldName}
			if sortOption.Desc {
				sortBy.Desc = true
			} else {
				sortBy.Asc = true
			}
			searchOptions.SortBy = append(searchOptions.SortBy, sortBy)
		}
		for _, filter := range query.Filters {
			fieldDef, valid := schema.(*entitySchema).fieldDefinitions[filter.FieldName]
			if !valid {
				panic(fmt.Errorf("field %s is not searchable by Redis Search", filter.FieldName))
			}
			minV := filter.Min
			if minV == nil {
				minV = "-inf"
			} else {
				asTime, isTime := minV.(time.Time)
				if isTime {
					minV = asTime.UTC().Unix()
					if fieldDef.Tags["time"] == "true" {
						minV = asTime.UTC().Unix()
					} else {
						minV = asTime.UTC().Truncate(24 * time.Hour).Unix()
					}
				}
			}
			maxV := filter.Max
			if maxV == nil {
				maxV = "+inf"
			} else {
				asTime, isTime := maxV.(time.Time)
				if isTime {
					maxV = asTime.UTC().Unix()
					if fieldDef.Tags["time"] == "true" {
						maxV = asTime.UTC().Unix()
					} else {
						maxV = asTime.UTC().Truncate(24 * time.Hour).Unix()
					}
				}
			}
			if isDragonFlyDB {
				if q == "*" {
					q = ""
				}
				if q != "" {
					q += " "
				}
				q += "@" + filter.FieldName + fmt.Sprintf(":[%v %v]", minV, maxV)
			} else {
				searchOptions.Filters = append(searchOptions.Filters, redis.FTSearchFilter{
					FieldName: filter.FieldName,
					Min:       minV,
					Max:       maxV,
				})
			}
		}
		if query.Tags != "" {
			if q == "*" {
				q = ""
			}
			if q != "" {
				q += " "
			}
			q += query.Tags
		}
	} else if pager != nil {
		searchOptions.LimitOffset = (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		searchOptions.Limit = pager.GetPageSize()
	}

	res := r.FTSearch(ctx, indexName, q, searchOptions)
	total = res.Total
	if total == 0 {
		return []uint64{}, total
	}
	ids = make([]uint64, len(res.Docs))
	for i, doc := range res.Docs {
		lastPart := doc.ID[strings.LastIndex(doc.ID, ":")+1:]
		id, err := strconv.ParseUint(lastPart, 10, 64)
		if err != nil {
			panic(err)
		}
		ids[i] = id
	}
	return ids, total
}

func defaultConvertBindToHashValueNotNullable(a any) any {
	return a
}

func defaultConvertBindToHashValueNullable(a any) any {
	if a == nil {
		return nullRedisValue
	}
	return a
}
