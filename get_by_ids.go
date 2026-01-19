package fluxaorm

import (
	"reflect"
	"strconv"
)

func GetByIDs[E any](ctx Context, ids ...uint64) (EntityIterator[E], error) {
	return getByIDs[E](ctx.(*ormImplementation), ids)
}

func getByIDs[E any](orm *ormImplementation, ids []uint64) (EntityIterator[E], error) {
	schema := getEntitySchema[E](orm)
	if schema == nil {
		return nil, nil
	}
	if len(ids) == 0 {
		return &emptyResultsIterator[E]{}, nil
	}
	if schema.hasLocalCache {
		return &localCacheIDsIterator[E]{orm: orm, schema: schema, ids: ids, index: -1}, nil
	}
	results := &entityIterator[E]{index: -1, ids: ids, schema: schema, orm: orm}
	results.rows = make([]*E, len(ids))
	var missingKeys []int
	cacheRedis, hasRedisCache := schema.GetRedisCache()
	var redisPipeline *RedisPipeLine
	if hasRedisCache {
		redisPipeline = orm.RedisPipeLine(cacheRedis.GetCode())
		l := int64(len(schema.columnNames) + 1)
		foundInContextCache := 0
		for i, id := range ids {
			fromContextCache, inContextCache := orm.getEntityFromCache(schema, id)
			if inContextCache {
				results.rows[i] = fromContextCache.(*E)
				foundInContextCache++
			}
		}
		if foundInContextCache == len(ids) {
			return results, nil
		}
		lRanges := make([]*PipeLineSlice, len(ids)-foundInContextCache)
		k := 0
		for i, id := range ids {
			if results.rows[i] == nil {
				lRanges[k] = redisPipeline.LRange(schema.cacheKey+":"+strconv.FormatUint(id, 10), 0, l)
				k++
			}
		}

		_, err := redisPipeline.Exec(orm)
		if err != nil {
			return nil, err
		}
		k = 0
		for i, id := range ids {
			if results.rows[i] != nil {
				continue
			}
			row, err := lRanges[k].Result()
			if err != nil {
				return nil, err
			}
			k++
			if len(row) > 0 {
				if len(row) == 1 {
					continue
				}
				value := reflect.New(schema.t)
				e := value.Interface().(*E)
				if deserializeFromRedis(row, schema, value.Elem()) {
					orm.cacheEntity(schema, id, e)
				}
				results.rows[i] = e
			} else {
				missingKeys = append(missingKeys, i)
			}
		}
		if len(missingKeys) == 0 {
			return results, nil
		}
	} else {
		for i, id := range ids {
			fromContextCache, inContextCache := orm.getEntityFromCache(schema, id)
			if inContextCache {
				results.rows[i] = fromContextCache.(*E)
			} else {
				missingKeys = append(missingKeys, i)
			}
		}
		if len(missingKeys) == 0 {
			return results, nil
		}
	}
	sql := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE `ID` IN ("
	toSearch := 0
	if len(missingKeys) > 0 {
		for i, key := range missingKeys {
			if i > 0 {
				sql += ","
			}
			sql += strconv.FormatUint(ids[key], 10)
		}
		toSearch = len(missingKeys)
	} else {
		for i, id := range ids {
			if i > 0 {
				sql += ","
			}
			sql += strconv.FormatUint(id, 10)
		}
		toSearch = len(ids)
	}
	sql += ")"
	execRedisPipeline := false
	res, def, err := schema.GetDB().Query(orm, sql)
	if err != nil {
		return nil, err
	}
	defer def()
	foundInDB := 0
	for res.Next() {
		foundInDB++
		pointers := prepareScan(schema)
		err = res.Scan(pointers...)
		if err != nil {
			return nil, err
		}
		value := reflect.New(schema.t)
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		id := *pointers[0].(*uint64)
		for i, originalID := range ids { // TODO too slow
			if id == originalID {
				results.rows[i] = value.Interface().(*E)
			}
		}
		if schema.hasLocalCache {
			schema.localCache.setEntity(orm, id, value.Interface().(*E))
		} else {
			orm.cacheEntity(schema, id, value.Interface())
		}
		if hasRedisCache {
			bind := make(Bind)
			err = fillBindFromOneSource(orm, bind, value.Elem(), schema.fields, "")
			if err != nil {
				return nil, err
			}
			values := convertBindToRedisValue(bind, schema)
			redisPipeline.RPush(schema.getCacheKey()+":"+strconv.FormatUint(id, 10), values...)
			execRedisPipeline = true
		}
	}
	def()
	if foundInDB < toSearch && (schema.hasLocalCache || hasRedisCache) {
		for i, id := range ids {
			if results.rows[i] == nil {
				if schema.hasLocalCache {
					schema.localCache.setEntity(orm, id, nil)
				} else {
					orm.cacheEntity(schema, id, nil)
				}
				if hasRedisCache {
					cacheKey := schema.getCacheKey() + ":" + strconv.FormatUint(id, 10)
					redisPipeline.Del(cacheKey)
					redisPipeline.RPush(cacheKey, cacheNilValue)
					execRedisPipeline = true
				}
			}
		}
	}
	if execRedisPipeline {
		_, err = redisPipeline.Exec(orm)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func warmup(orm *ormImplementation, schema *entitySchema, ids []uint64, references string) error {
	if len(ids) == 0 || orm.disabledContextCache {
		return nil
	}
	iterator, err := schema.GetByIDs(orm, ids...)
	if err != nil {
		return err
	}
	if references != "" {
		err = iterator.LoadReference(references)
		if err != nil {
			return err
		}
	}
	return nil
}
