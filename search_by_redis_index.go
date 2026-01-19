package fluxaorm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

const maxRedisSearchLimit = 100000

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

func GetRedisSearchAlters(ctx Context) (alters []RedisSearchAlter, err error) {
	indicesInRedis := make(map[string]map[string]bool)
	indicesInEntities := make(map[string]map[string]bool)
	for poolName, pool := range ctx.Engine().Registry().RedisPools() {
		if pool.GetConfig().GetDatabaseNumber() > 0 {
			continue
		}
		indicesInRedis[poolName] = make(map[string]bool)
		indicesInEntities[poolName] = make(map[string]bool)
		l, err := pool.FTList(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range l {
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
		alter, hasAlter, err := getRedisIndexAlter(ctx, schema, redisPool)
		if err != nil {
			return nil, err
		}
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
				info, valid, err := ctx.Engine().Redis(poolName).FTInfo(ctx, indexName)
				if err != nil {
					return nil, err
				}
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
	return alters, nil
}

func (a RedisSearchAlter) Exec(ctx Context) error {
	if a.Drop {
		return ctx.Engine().Redis(a.Pool).FTDrop(ctx, a.IndexName, true)
	}
	err := ctx.Engine().Redis(a.Pool).FTCreate(ctx, a.IndexName, a.indexOptions, a.indexSchema...)
	if err != nil {
		return err
	}
	if !a.schema.IsVirtual() {
		err = a.schema.ReindexRedisIndex(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func getRedisIndexAlter(ctx Context, schema *entitySchema, r RedisCache) (alter *RedisSearchAlter, has bool, err error) {
	_, isInRedis, err := r.FTInfo(ctx, schema.redisSearchIndexName)
	if err != nil {
		return nil, false, err
	}
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
		return nil, false, nil
	}
	alter.IndexDefinition = schema.createRedisSearchIndexDefinition(alter.IndexName)
	if !schema.virtual {
		query := fmt.Sprintf("SELECT COUNT(ID) FROM `%s`", schema.GetTableName())
		total := uint64(0)
		_, err = schema.GetDB().QueryRow(ctx, NewWhere(query), &total)
		if err != nil {
			return nil, false, err
		}
		alter.DocumentsInDB = total
	}
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

func NewRedisSearchQuery() *RedisSearchQuery {
	return &RedisSearchQuery{}
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

func RedisSearchIDs[E any](ctx Context, query *RedisSearchQuery, pager *Pager) (results []uint64, totalRows int, err error) {
	schema := getEntitySchema[E](ctx)
	if schema == nil {
		return nil, 0, nil
	}
	return redisSearchIDs(ctx, schema, query, pager)
}

func RedisSearch[E any](ctx Context, query *RedisSearchQuery, pager *Pager) (results EntityIterator[E], totalRows int, err error) {
	schema := getEntitySchema[E](ctx)
	if schema == nil {
		return nil, 0, nil
	}
	ids, totalRows, err := redisSearchIDs(ctx, schema, query, pager)
	if err != nil {
		return nil, 0, err
	}
	if schema.hasLocalCache {
		if totalRows == 0 {
			return &emptyResultsIterator[E]{}, 0, nil
		}
		return &localCacheIDsIterator[E]{orm: ctx.(*ormImplementation), schema: schema, ids: ids, index: -1}, totalRows, nil
	}
	ret, err := GetByIDs[E](ctx, ids...)
	if err != nil {
		return nil, 0, err
	}
	return ret, totalRows, nil
}

func RedisSearchOne[E any](ctx Context, query *RedisSearchQuery) (entity *E, found bool, err error) {
	if query == nil {
		query = NewRedisSearchQuery()
	}
	ids, total, err := RedisSearchIDs[E](ctx, query, NewPager(1, 1))
	if err != nil {
		return nil, false, err
	}
	if total == 0 {
		return nil, false, nil
	}
	e, found, err := GetByID[E](ctx, ids[0])
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return e, true, nil
}

func (e *entitySchema) ReindexRedisIndex(ctx Context) error {
	if e.redisSearchIndexName == "" {
		return nil
	}
	if e.virtual {
		return fmt.Errorf("virtual entity %s can't be reindexed", e.GetType().Name())
	}
	lastIDRedisKey := "lastID:" + e.redisSearchIndexName
	r := ctx.Engine().Redis(e.redisSearchIndexPoolCode)
	scanCursor := uint64(0)
	keysPattern := e.redisSearchIndexPrefix + "*"
	for {
		keys, newCursor, err := r.Scan(ctx, scanCursor, keysPattern, 1000)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			p := ctx.RedisPipeLine(r.GetConfig().GetCode())
			p.Del(keys...)
			_, err = p.Exec(ctx)
			if err != nil {
				return err
			}
		}
		if len(keys) < 1000 {
			break
		}
		scanCursor = newCursor
	}
	lastID, hasLastID, err := r.Get(ctx, lastIDRedisKey)
	if err != nil {
		return err
	}
	if !hasLastID {
		lastID = "0"
		err = r.Set(ctx, lastIDRedisKey, lastID, 0)
		if err != nil {
			return err
		}
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
	where += " FROM `" + e.GetTableName() + "` WHERE ID > ?"
	if e.hasFakeDelete {
		where += " AND FakeDelete = 0"
	}
	where += " ORDER BY ID ASC"
	db := e.GetDB()
	for {
		p := ctx.RedisPipeLine(r.GetConfig().GetCode())
		err = func() error {
			rows, cl, err := db.Query(ctx, where, lastID)
			if err != nil {
				return err
			}
			defer cl()
			i = 0
			for rows.Next() {
				err = rows.Scan(pointers...)
				if err != nil {
					return err
				}
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
			return nil
		}()
		if err != nil {
			return err
		}
		if i < 3000 {
			p.Del(lastIDRedisKey)
			_, err = p.Exec(ctx)
			if err != nil {
				return err
			}
			break
		} else {
			p.Set(lastIDRedisKey, "0", 0)
			_, err = p.Exec(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func redisSearchIDs(ctx Context, schema EntitySchema, query *RedisSearchQuery, pager *Pager) (ids []uint64, total int, err error) {
	indexName := schema.GetRedisSearchIndexName()
	if indexName == "" {
		return nil, 0, fmt.Errorf("entity %s is not searchable by Redis Search", schema.GetType().Name())
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
		} else {
			searchOptions.Limit = maxRedisSearchLimit
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
				return nil, 0, fmt.Errorf("field %s is not searchable by Redis Search", filter.FieldName)
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
	} else {
		searchOptions.Limit = maxRedisSearchLimit
	}

	res, err := r.FTSearch(ctx, indexName, q, searchOptions)
	if err != nil {
		return nil, 0, err
	}
	total = res.Total
	if total == 0 {
		return []uint64{}, total, nil
	}
	ids = make([]uint64, len(res.Docs))
	for i, doc := range res.Docs {
		lastPart := doc.ID[strings.LastIndex(doc.ID, ":")+1:]
		id, err := strconv.ParseUint(lastPart, 10, 64)
		if err != nil {
			return nil, 0, err
		}
		ids[i] = id
	}
	return ids, total, nil
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
