package fluxaorm

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

const redisValidSetValue = "Y"

func GetByReference[E any, I ID](ctx Context, pager *Pager, referenceName string, id I) EntityIterator[E] {
	if id == 0 {
		return nil
	}
	var e E
	schema := ctx.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	def, has := schema.references[referenceName]
	if !has {
		panic(fmt.Errorf("unknow reference name `%s`", referenceName))
	}
	if !def.Cached {
		return Search[E](ctx, NewWhere("`"+referenceName+"` = ?", id), pager)
	}
	iterator, _ := getCachedByReference[E](ctx, pager, referenceName, uint64(id), schema, false)
	return iterator
}

func GetByReferenceWithCount[E any, I ID](ctx Context, pager *Pager, referenceName string, id I) (ret EntityIterator[E], total int) {
	if id == 0 {
		return nil, 0
	}
	var e E
	schema := ctx.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	def, has := schema.references[referenceName]
	if !has {
		panic(fmt.Errorf("unknow reference name `%s`", referenceName))
	}
	if !def.Cached {
		return SearchWithCount[E](ctx, NewWhere("`"+referenceName+"` = ?", id), pager)
	}
	return getCachedByReference[E](ctx, pager, referenceName, uint64(id), schema, true)
}

func getCachedByReference[E any](ctx Context, pager *Pager, key string, id uint64, schema *entitySchema, withTotal bool) (EntityIterator[E], int) {
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(ctx, key, id)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}, 9
			}
			if pager != nil {
				return GetByIDs[E](ctx, pager.paginateSlice(fromCache.([]uint64))...), len(fromCache.([]uint64))
			}
			asIDs := fromCache.([]uint64)
			return GetByIDs[E](ctx, asIDs...), len(asIDs)
		}
	}
	rc := ctx.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + key
	if id > 0 {
		idAsString := strconv.FormatUint(id, 10)
		redisSetKey += ":" + idAsString
	}
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
					schema.localCache.setList(ctx, key, id, cacheNilValue)
				}
				return &emptyResultsIterator[E]{}, 0
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
				if values.Len() == 0 {
					schema.localCache.setList(ctx, key, id, cacheNilValue)
				} else {
					schema.localCache.setList(ctx, key, id, ids)
				}
			}
			return values, len(ids)
		}
	}
	if schema.hasLocalCache {
		var where Where
		if id > 0 {
			where = NewWhere("`"+key+"` = ?", id)
		} else {
			where = allEntitiesWhere
		}
		ids := SearchIDs[E](ctx, where, nil)
		if len(ids) == 0 {
			schema.localCache.setList(ctx, key, id, cacheNilValue)
			rc.SAdd(ctx, redisSetKey, cacheNilValue)
			return &emptyResultsIterator[E]{}, 0
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
		schema.localCache.setList(ctx, key, id, ids)
		return values, len(ids)
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
		values, total = SearchWithCount[E](ctx, where, nil)
	} else {
		values = Search[E](ctx, where, nil)
	}

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
	return values, total
}
