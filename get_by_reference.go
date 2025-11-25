package fluxaorm

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

const redisValidSetValue = "Y"

func GetByReference[E any, I ID](ctx Context, pager *Pager, referenceName string, id I) (EntityIterator[E], error) {
	if id == 0 {
		return nil, nil
	}
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, err
	}
	def, has := schema.references[referenceName]
	if !has {
		return nil, fmt.Errorf("unknow reference name `%s`", referenceName)
	}
	if !def.Cached {
		return Search[E](ctx, NewWhere("`"+referenceName+"` = ?", id), pager)
	}
	iterator, _, err := getCachedByReference[E](ctx, pager, referenceName, uint64(id), schema, false)
	return iterator, err
}

func GetByReferenceWithCount[E any, I ID](ctx Context, pager *Pager, referenceName string, id I) (ret EntityIterator[E], total int, err error) {
	if id == 0 {
		return nil, 0, err
	}
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, 0, err
	}
	def, has := schema.references[referenceName]
	if !has {
		return nil, 0, fmt.Errorf("unknow reference name `%s`", referenceName)
	}
	if !def.Cached {
		return SearchWithCount[E](ctx, NewWhere("`"+referenceName+"` = ?", id), pager)
	}
	return getCachedByReference[E](ctx, pager, referenceName, uint64(id), schema, true)
}

func getCachedByReference[E any](ctx Context, pager *Pager, key string, id uint64, schema *entitySchema, withTotal bool) (EntityIterator[E], int, error) {
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(ctx, key, id)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}, 0, nil
			}
			if pager != nil {
				ret, err := GetByIDs[E](ctx, pager.paginateSlice(fromCache.([]uint64))...)
				if err != nil {
					return nil, 0, err
				}
				return ret, len(fromCache.([]uint64)), nil
			}
			asIDs := fromCache.([]uint64)
			ret, err := GetByIDs[E](ctx, asIDs...)
			if err != nil {
				return nil, 0, err
			}
			return ret, len(asIDs), nil
		}
	}
	rc := ctx.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + key
	if id > 0 {
		idAsString := strconv.FormatUint(id, 10)
		redisSetKey += ":" + idAsString
	}
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
					schema.localCache.setList(ctx, key, id, cacheNilValue)
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
				if values.Len() == 0 {
					schema.localCache.setList(ctx, key, id, cacheNilValue)
				} else {
					schema.localCache.setList(ctx, key, id, ids)
				}
			}
			return values, len(ids), nil
		}
	}
	if schema.hasLocalCache {
		var where Where
		if id > 0 {
			where = NewWhere("`"+key+"` = ?", id)
		} else {
			where = allEntitiesWhere
		}
		ids, err := SearchIDs[E](ctx, where, nil)
		if err != nil {
			return nil, 0, err
		}
		if len(ids) == 0 {
			schema.localCache.setList(ctx, key, id, cacheNilValue)
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
		schema.localCache.setList(ctx, key, id, ids)
		return values, len(ids), nil
	}
	var where Where
	if id > 0 {
		where = NewWhere("`"+key+"` = ?", id)
	} else {
		where = allEntitiesWhere
	}
	var values EntityIterator[E]
	var total int
	if withTotal {
		values, total, err = SearchWithCount[E](ctx, where, nil)
	} else {
		values, err = Search[E](ctx, where, nil)
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
