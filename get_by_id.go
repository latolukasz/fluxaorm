package fluxaorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func GetByID[E any, I ID](ctx Context, id I) (entity *E, found bool, err error) {
	var e E
	cE := ctx.(*ormImplementation)
	schema, err := getEntitySchemaFromSource(ctx, e)
	if err != nil {
		return nil, false, err
	}
	value, found, err := getByID(cE, uint64(id), schema)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}
	return value.(*E), true, nil
}

func getByID(orm *ormImplementation, id uint64, schema *entitySchema) (any, bool, error) {
	if schema.hasLocalCache {
		e, has := schema.localCache.getEntity(orm, id)
		if has {
			if e == nil {
				return nil, false, nil
			}
			return e, true, nil
		}
	}
	fromLocalCache, inLocalCache := orm.getEntityFromCache(schema, id)
	if inLocalCache {
		if fromLocalCache == nil {
			return nil, false, nil
		}
		return fromLocalCache, true, nil
	}
	cacheRedis, hasRedis := schema.GetRedisCache()
	var cacheKey string
	if hasRedis {
		cacheKey = schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
		row, err := cacheRedis.LRange(orm, cacheKey, 0, int64(len(schema.columnNames)+1))
		if err != nil {
			return nil, false, err
		}
		l := len(row)
		if len(row) > 0 {
			if l == 1 {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, nil)
				}
				return nil, false, nil
			}
			value := reflect.New(schema.t)
			entity := value.Interface()
			if deserializeFromRedis(row, schema, value.Elem()) {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, entity)
				} else if !orm.disabledContextCache {
					orm.cacheEntity(schema, id, entity)
				}
				return entity, true, nil
			}
		}
	}
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE ID = ? LIMIT 1"
	pointers := prepareScan(schema)
	found, err := schema.GetDB().QueryRow(orm, NewWhere(query, id), pointers...)
	if err != nil {
		return nil, false, err
	}
	if found {
		value := reflect.New(schema.t)
		entity := value.Interface()
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		if schema.hasLocalCache {
			schema.localCache.setEntity(orm, id, entity)
		} else if !orm.disabledContextCache {
			orm.cacheEntity(schema, id, entity)
		}
		if hasRedis {
			bind := make(Bind)
			err := fillBindFromOneSource(orm, bind, reflect.ValueOf(entity).Elem(), schema.fields, "")
			if err != nil {
				return nil, false, err
			}
			fmt.Printf("%T %v\n", bind["TestJsons"], bind["TestJsons"])
			values := convertBindToRedisValue(bind, schema)
			_, err = cacheRedis.RPush(orm, cacheKey, values...)
			if err != nil {
				return nil, false, err
			}
		}
		return entity, true, nil
	}
	if schema.hasLocalCache {
		schema.localCache.setEntity(orm, id, nil)
	} else if !orm.disabledContextCache {
		orm.cacheEntity(schema, id, nil)
	}
	if hasRedis {
		p := orm.RedisPipeLine(cacheRedis.GetCode())
		p.Del(cacheKey)
		p.RPush(cacheKey, cacheNilValue)
		_, err = p.Exec(orm)
		if err != nil {
			return nil, false, err
		}
	}
	return nil, false, nil
}
