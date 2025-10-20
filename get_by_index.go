package fluxaorm

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

type indexDefinition struct {
	Cached     bool
	Columns    []string
	Where      string
	Duplicated bool
}

func (d indexDefinition) CreteWhere(hasNil bool, attributes []any) Where {
	if !hasNil {
		return NewWhere(d.Where, attributes...)
	}
	query := ""
	newAttributes := make([]any, 0)
	for i, column := range d.Columns {
		if i > 0 {
			query += " AND "
		}
		if attributes[i] == nil {
			query += "`" + column + "` IS NULL"
		} else {
			query += "`" + column + "`=?"
			newAttributes = append(newAttributes, attributes[i])
		}
	}
	return NewWhere(query, newAttributes)
}

func GetByIndex[E any](ctx Context, pager *Pager, indexName string, attributes ...any) EntityIterator[E] {
	var e E
	schema := ctx.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	def, has := schema.indexes[indexName]
	if !has {
		panic(fmt.Errorf("unknow index name `%s`", indexName))
	}
	if len(attributes) != len(def.Columns) {
		panic(fmt.Errorf("invalid attributes length, %d is required, %d provided", len(def.Columns), len(attributes)))
	}
	hasNil := false
	for i, attribute := range attributes {
		if attribute == nil {
			hasNil = true
			continue
		}
		setter := schema.fieldBindSetters[def.Columns[i]]
		bind, err := setter(attribute)
		if err != nil {
			panic(err)
		}
		attributes[i] = bind
	}
	if !def.Cached {
		return Search[E](ctx, def.CreteWhere(hasNil, attributes), pager)
	}
	return getCachedByColumns[E](ctx, pager, indexName, def, schema, attributes, hasNil)
}

func getCachedByColumns[E any](ctx Context, pager *Pager, indexName string, index indexDefinition, schema *entitySchema, attributes []any, hasNil bool) EntityIterator[E] {
	bindID := hashIndexAttributes(attributes)
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(ctx, indexName, bindID)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}
			}
			ids := fromCache.([]uint64)
			if pager != nil {
				return GetByIDs[E](ctx, pager.paginateSlice(ids)...)
			}
			return GetByIDs[E](ctx, ids...)
		}
	}
	rc := ctx.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + indexName + ":" + strconv.FormatUint(bindID, 10)
	fromRedis := rc.SMembers(ctx, redisSetKey)
	if len(fromRedis) > 0 {
		ids := make([]uint64, len(fromRedis))
		k := 0
		hasValidValue := false
		for _, value := range fromRedis {
			if value == redisValidSetValue {
				hasValidValue = true
				continue
			} else if value == cacheNilValue {
				continue
			}
			ids[k], _ = strconv.ParseUint(value, 10, 64)
			k++
		}
		if hasValidValue {
			if k == 0 {
				if schema.hasLocalCache {
					schema.localCache.setList(ctx, indexName, bindID, cacheNilValue)
				}
				return &emptyResultsIterator[E]{}
			}
			ids = ids[0:k]
			slices.Sort(ids)
			var values EntityIterator[E]
			if pager != nil {
				values = GetByIDs[E](ctx, pager.paginateSlice(ids)...)
			} else {
				values = GetByIDs[E](ctx, ids...)
			}

			if schema.hasLocalCache {
				if len(ids) == 0 {
					schema.localCache.setList(ctx, indexName, bindID, cacheNilValue)
				} else {
					schema.localCache.setList(ctx, indexName, bindID, ids)
				}
			}
			return values
		}
	}
	if schema.hasLocalCache {
		ids := SearchIDs[E](ctx, index.CreteWhere(hasNil, attributes), nil)
		if len(ids) == 0 {
			schema.localCache.setList(ctx, indexName, bindID, cacheNilValue)
			rc.SAdd(ctx, redisSetKey, cacheNilValue)
			return &emptyResultsIterator[E]{}
		}
		idsForRedis := make([]any, len(ids))
		for i, value := range ids {
			idsForRedis[i] = strconv.FormatUint(value, 10)
		}
		p := ctx.RedisPipeLine(rc.GetCode())
		p.Del(redisSetKey)
		p.SAdd(redisSetKey, redisValidSetValue)
		p.SAdd(redisSetKey, idsForRedis...)
		p.Exec(ctx)
		var values EntityIterator[E]
		if pager != nil {
			values = GetByIDs[E](ctx, pager.paginateSlice(ids)...)
		} else {
			values = GetByIDs[E](ctx, ids...)
		}

		schema.localCache.setList(ctx, indexName, bindID, ids)
		return values
	}
	values := Search[E](ctx, index.CreteWhere(hasNil, attributes), nil)
	if values.Len() == 0 {
		rc.SAdd(ctx, redisSetKey, redisValidSetValue, cacheNilValue)
	} else {
		idsForRedis := make([]any, values.Len()+1)
		idsForRedis[0] = redisValidSetValue
		i := 0
		for values.Next() {
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(values.Entity()).Elem().Field(0).Uint(), 10)
			i++
		}
		values.Reset()
		rc.SAdd(ctx, redisSetKey, idsForRedis...)
	}
	if pager != nil {
		values.setIndex((pager.CurrentPage - 1) * pager.PageSize)
	}

	return values
}

func hashIndexAttributes(attributes []any) uint64 {
	attributesHash, err := jsoniter.ConfigFastest.Marshal(attributes)
	hash := fnv.New64()
	_, err = hash.Write(attributesHash)
	checkError(err)
	bindID := hash.Sum64()
	return bindID
}
