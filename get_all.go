package fluxaorm

import (
	"reflect"
	"slices"
	"strconv"
)

const cacheAllKey = "all"

var allEntitiesWhere = NewWhere("1")

func GetAll[E any](ctx Context) (EntityIterator[E], error) {
	var e E
	schema := getEntitySchemaFromSource(ctx, e)
	if schema == nil {
		return nil, nil
	}
	if !schema.cacheAll {
		return Search[E](ctx, allEntitiesWhere, nil)
	}
	iterator, err := getAll[E](ctx, schema)
	return iterator, err
}

func getAll[E any](ctx Context, schema *entitySchema) (EntityIterator[E], error) {
	if schema.hasLocalCache {
		fromCache, hasInCache := schema.localCache.getList(ctx, cacheAllKey, 0)
		if hasInCache {
			if fromCache == cacheNilValue {
				return &emptyResultsIterator[E]{}, nil
			}
			asIDs := fromCache.([]uint64)
			ret, err := GetByIDs[E](ctx, asIDs...)
			if err != nil {
				return nil, err
			}
			return ret, nil
		}
	}
	rc := ctx.Engine().Redis(schema.getForcedRedisCode())
	redisSetKey := schema.cacheKey + ":" + cacheAllKey
	fromRedis, err := rc.SMembers(ctx, redisSetKey)
	if err != nil {
		return nil, err
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
					schema.localCache.setList(ctx, cacheAllKey, 0, cacheNilValue)
				}
				return &emptyResultsIterator[E]{}, nil
			}
			ids = ids[0:k]
			slices.Sort(ids)
			values, err := GetByIDs[E](ctx, ids...)
			if err != nil {
				return nil, err
			}
			if schema.hasLocalCache {
				if values.Len() == 0 {
					schema.localCache.setList(ctx, cacheAllKey, 0, cacheNilValue)
				} else {
					schema.localCache.setList(ctx, cacheAllKey, 0, ids)
				}
			}
			return values, nil
		}
	}
	if schema.hasLocalCache {
		where := allEntitiesWhere
		ids, err := SearchIDs[E](ctx, where, nil)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			schema.localCache.setList(ctx, cacheAllKey, 0, cacheNilValue)
			_, err = rc.SAdd(ctx, redisSetKey, cacheNilValue)
			if err != nil {
				return nil, err
			}
			return &emptyResultsIterator[E]{}, nil
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
			return nil, err
		}
		values, err := GetByIDs[E](ctx, ids...)
		if err != nil {
			return nil, err
		}
		schema.localCache.setList(ctx, cacheAllKey, 0, ids)
		return values, nil
	}
	where := allEntitiesWhere
	values, err := Search[E](ctx, where, nil)
	if err != nil {
		return nil, err
	}

	if values.Len() == 0 {
		_, err = rc.SAdd(ctx, redisSetKey, redisValidSetValue, cacheNilValue)
		if err != nil {
			return nil, err
		}
	} else {
		idsForRedis := make([]any, values.Len()+1)
		idsForRedis[0] = redisValidSetValue
		i := 0
		for values.Next() {
			e, err := values.Entity()
			if err != nil {
				return nil, err
			}
			idsForRedis[i+1] = strconv.FormatUint(reflect.ValueOf(e).Elem().Field(0).Uint(), 10)
			i++
		}
		values.Reset()
		_, err = rc.SAdd(ctx, redisSetKey, idsForRedis...)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
