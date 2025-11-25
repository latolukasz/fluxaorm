package fluxaorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByUniqueIndex[E any](ctx Context, indexName string, attributes ...any) (entity *E, found bool, err error) {
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, false, err
	}
	definition, has := schema.uniqueIndexes[indexName]
	if !has {
		return nil, false, fmt.Errorf("unknown index name `%s`", indexName)
	}
	if len(definition.Columns) != len(attributes) {
		return nil, false, fmt.Errorf("invalid number of index `%s` attributes, got %d, %d expected",
			indexName, len(attributes), len(definition.Columns))
	}
	var redisForCache RedisCache
	var hSetKey, hField string
	if definition.Cached {
		hSetKey = schema.getCacheKey() + ":" + indexName
		s := ""
		for i, attr := range attributes {
			if attr == nil {
				return nil, false, fmt.Errorf("nil attribute for index name `%s` is not allowed", indexName)
			}
			val, err := schema.columnAttrToStringSetters[definition.Columns[i]](attr, false)
			if err != nil {
				return nil, false, err
			}
			s += val
		}
		hField = hashString(s)
		cache, hasRedis := schema.GetRedisCache()
		if !hasRedis {
			cache = ctx.Engine().Redis(DefaultPoolCode)
		}
		redisForCache = cache
		previousID, inUse, err := cache.HGet(ctx, hSetKey, hField)
		if err != nil {
			return nil, false, err
		}
		if inUse {
			if previousID == "0" {
				return nil, false, nil
			}
			id, _ := strconv.ParseUint(previousID, 10, 64)
			entity, found, err = GetByID[E](ctx, id)
			if err != nil {
				return nil, false, err
			}
			if !found {
				err = cache.HDel(ctx, hSetKey, hField)
				if err != nil {
					return nil, false, err
				}
			}
			return entity, found, nil
		}
	}

	for i, attribute := range attributes {
		setter := schema.fieldBindSetters[definition.Columns[i]]
		bind, err := setter(attribute)
		if err != nil {
			return nil, false, err
		}
		attributes[i] = bind
	}
	entity, found, err = SearchOne[E](ctx, definition.CreteWhere(false, attributes))
	if err != nil {
		return nil, false, err
	}
	if !found {
		if definition.Cached {
			err = redisForCache.HSet(ctx, hSetKey, hField, "0")
			if err != nil {
				return nil, false, err
			}
		}
		return nil, false, nil
	}
	if definition.Cached {
		id := strconv.FormatUint(reflect.ValueOf(entity).Elem().FieldByName("ID").Uint(), 10)
		err = redisForCache.HSet(ctx, hSetKey, hField, id)
		if err != nil {
			return nil, false, err
		}
	}
	return entity, true, nil
}
