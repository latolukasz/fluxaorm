package fluxaorm

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

type IndexDefinition struct {
	Columns string
	Cached  bool
}

type UniqueIndexDefinition struct {
	Columns string
	Cached  bool
}

type IndexInterface interface {
	Indexes() any
}

type indexDefinition struct {
	Cached     bool
	Columns    []string
	Where      string
	Duplicated bool
}

type dirtyDefinition struct {
	Stream  string
	Columns map[string]bool
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

func GetByIndex[E any](ctx Context, pager *Pager, index IndexDefinition, attributes ...any) (EntityIterator[E], error) {
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, err
	}
	indexName, hasName := schema.indexesMapping[index]
	if !hasName {
		return nil, fmt.Errorf("unknown index for columns `%s`", index.Columns)
	}
	def, has := schema.indexes[indexName]
	if !has {
		return nil, fmt.Errorf("unknow index name `%s`", indexName)
	}
	if len(attributes) != len(def.Columns) {
		return nil, fmt.Errorf("invalid attributes length, %d is required, %d provided", len(def.Columns), len(attributes))
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
			return nil, err
		}
		attributes[i] = bind
	}
	if !def.Cached {
		return Search[E](ctx, def.CreteWhere(hasNil, attributes), pager)
	}
	iterator, _, err := getCachedByColumns[E](ctx, pager, indexName, def, schema, attributes, hasNil, false)
	return iterator, err
}

func GetByIndexWithCount[E any](ctx Context, pager *Pager, index IndexDefinition, attributes ...any) (res EntityIterator[E], total int, err error) {
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, 0, err
	}
	indexName, hasName := schema.indexesMapping[index]
	if !hasName {
		return nil, 0, fmt.Errorf("unknown index for columns `%s`", index.Columns)
	}
	def, has := schema.indexes[indexName]
	if !has {
		return nil, 0, fmt.Errorf("unknow index name `%s`", indexName)
	}
	if len(attributes) != len(def.Columns) {
		return nil, 0, fmt.Errorf("invalid attributes length, %d is required, %d provided", len(def.Columns), len(attributes))
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
			return nil, 0, err
		}
		attributes[i] = bind
	}
	if !def.Cached {
		return SearchWithCount[E](ctx, def.CreteWhere(hasNil, attributes), pager)
	}
	return getCachedByColumns[E](ctx, pager, indexName, def, schema, attributes, hasNil, true)
}

func getCachedByColumns[E any](ctx Context, pager *Pager, indexName string, index indexDefinition, schema *entitySchema, attributes []any, hasNil, withTotal bool) (EntityIterator[E], int, error) {
	bindID, err := hashIndexAttributes(attributes)
	if err != nil {
		return nil, 0, err
	}
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(ctx, indexName, bindID)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}, 0, nil
			}
			ids := fromCache.([]uint64)
			if pager != nil {
				ret, err := GetByIDs[E](ctx, pager.paginateSlice(ids)...)
				if err != nil {
					return nil, 0, err
				}
				return ret, len(ids), nil
			}
			ret, err := GetByIDs[E](ctx, ids...)
			if err != nil {
				return nil, 0, err
			}
			return ret, len(ids), nil
		}
	}
	rc := ctx.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + indexName + ":" + strconv.FormatUint(bindID, 10)
	fromRedis, err := rc.SMembers(ctx, redisSetKey)
	if err != nil {
		return nil, 0, err
	}
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
				return &emptyResultsIterator[E]{}, 0, nil
			}
			ids = ids[0:k]
			slices.Sort(ids)
			var values EntityIterator[E]
			if pager != nil {
				values, err = GetByIDs[E](ctx, pager.paginateSlice(ids)...)
			} else {
				values, err = GetByIDs[E](ctx, ids...)
			}
			if err != nil {
				return nil, 0, err
			}

			if schema.hasLocalCache {
				if len(ids) == 0 {
					schema.localCache.setList(ctx, indexName, bindID, cacheNilValue)
				} else {
					schema.localCache.setList(ctx, indexName, bindID, ids)
				}
			}
			return values, len(ids), nil
		}
	}
	if schema.hasLocalCache {
		ids, err := SearchIDs[E](ctx, index.CreteWhere(hasNil, attributes), nil)
		if err != nil {
			return nil, 0, err
		}
		if len(ids) == 0 {
			schema.localCache.setList(ctx, indexName, bindID, cacheNilValue)
			_, err = rc.SAdd(ctx, redisSetKey, cacheNilValue)
			if err != nil {
				return nil, 0, err
			}
			return &emptyResultsIterator[E]{}, 0, nil
		}
		idsForRedis := make([]any, len(ids))
		for i, value := range ids {
			idsForRedis[i] = strconv.FormatUint(value, 10)
		}
		p := ctx.RedisPipeLine(rc.GetCode())
		p.Del(redisSetKey)
		p.SAdd(redisSetKey, redisValidSetValue)
		p.SAdd(redisSetKey, idsForRedis...)
		_, err = p.Exec(ctx)
		if err != nil {
			return nil, 0, err
		}
		var values EntityIterator[E]
		if pager != nil {
			values, err = GetByIDs[E](ctx, pager.paginateSlice(ids)...)
		} else {
			values, err = GetByIDs[E](ctx, ids...)
		}
		if err != nil {
			return nil, 0, err
		}

		schema.localCache.setList(ctx, indexName, bindID, ids)
		return values, len(ids), nil
	}
	var values EntityIterator[E]
	total := 0
	if withTotal {
		values, total, err = SearchWithCount[E](ctx, index.CreteWhere(hasNil, attributes), nil)
	} else {
		values, err = Search[E](ctx, index.CreteWhere(hasNil, attributes), nil)
	}
	if err != nil {
		return nil, 0, err
	}

	if values.Len() == 0 {
		_, err = rc.SAdd(ctx, redisSetKey, redisValidSetValue, cacheNilValue)
		if err != nil {
			return nil, 0, err
		}
	} else {
		idsForRedis := make([]any, values.Len()+1)
		idsForRedis[0] = redisValidSetValue
		i := 0
		for values.Next() {
			e, err := values.Entity()
			if err != nil {
				return nil, 0, err
			}
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(e).Elem().Field(0).Uint(), 10)
			i++
		}
		values.Reset()
		_, err = rc.SAdd(ctx, redisSetKey, idsForRedis...)
		if err != nil {
			return nil, 0, err
		}
	}
	if pager != nil && values.Len() > 0 {
		valuesIterator := values.(*entityIterator[E])
		valuesIterator.ids = pager.paginateSlice(valuesIterator.ids)
		start, end := pager.cutsSlice(len(valuesIterator.rows))
		valuesIterator.rows = valuesIterator.rows[start:end]
	}

	return values, total, nil
}

func hashIndexAttributes(attributes []any) (uint64, error) {
	attributesHash, err := jsoniter.ConfigFastest.Marshal(attributes)
	if err != nil {
		return 0, err
	}
	hash := fnv.New64()
	_, err = hash.Write(attributesHash)
	if err != nil {
		return 0, err
	}
	bindID := hash.Sum64()
	return bindID, nil
}
