package fluxaorm

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/puzpuzpuz/xsync/v2"

	jsoniter "github.com/json-iterator/go"
)

type entitySQLOperations map[FlushType][]EntityFlush
type schemaSQLOperations map[*entitySchema]entitySQLOperations
type sqlOperations map[DB]schemaSQLOperations
type dbAction func(db DBBase) error
type PostFlushAction func(ctx Context)

type DuplicateKeyError struct {
	Message string
	Index   string
}

func (e *DuplicateKeyError) Error() string {
	return e.Message
}

func (orm *ormImplementation) Flush() error {
	err := orm.flush(false)
	if err != nil {
		return err
	}
	return orm.flushGenerated(false)
}

func (orm *ormImplementation) FlushAsync() error {
	return orm.flush(true)
}

func (orm *ormImplementation) flushGenerated(async bool) (err error) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedGeneratedEntities == nil || orm.trackedGeneratedEntities.Size() == 0 {
		return nil
	}
	orm.trackedGeneratedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Flushable]) bool {
		value.Range(func(_ uint64, f Flushable) bool {
			err = f.Flush()
			if err != nil {
				return false
			}
			return true
		})
		return true
	})
	if err != nil {
		return err
	}
	for _, dbPipeline := range orm.dbPipeLines {
		err = dbPipeline.Exec(orm)
		if err != nil {
			return err
		}
	}
	orm.trackedGeneratedEntities.Clear()
	return nil
}

func (orm *ormImplementation) flush(async bool) (err error) {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}
	sqlGroup := orm.groupSQLOperations()
	for _, operations := range sqlGroup {
		for schema, queryOperations := range operations {
			deletes, has := queryOperations[Delete]
			if has {
				err = orm.handleDeletes(async, schema, deletes)
				if err != nil {
					return err
				}
			}
			inserts, has := queryOperations[Insert]
			if has {
				err = orm.handleInserts(async, schema, inserts)
				if err != nil {
					return err
				}
			}
			updates, has := queryOperations[Update]
			if has {
				err = orm.handleUpdates(async, schema, updates)
				if err != nil {
					return err
				}
			}
		}
	}
	if !async {
		func() {
			var transactions []DBTransaction
			defer func() {
				for _, tx := range transactions {
					_ = tx.Rollback(orm)
				}
			}()
			for code, actions := range orm.flushDBActions {
				var d DBBase
				d = orm.Engine().DB(code)
				if len(actions) > 1 || len(orm.flushDBActions) > 1 {
					var tx DBTransaction
					tx, err = d.(DB).Begin(orm)
					if err != nil {
						return
					}
					transactions = append(transactions, tx)
					d = tx
				}
				for _, action := range actions {
					err = action(d)
					if err != nil {
						var mysqlErr *mysql.MySQLError
						isMysqlErr := errors.As(err, &mysqlErr)
						if isMysqlErr && mysqlErr.Number == 1062 {
							parts := strings.Split(mysqlErr.Message, ".")
							key := strings.Trim(parts[len(parts)-1], "'")
							err = &DuplicateKeyError{Message: mysqlErr.Message, Index: key}
						}
						return
					}
				}
			}
			for _, tx := range transactions {
				err = tx.Commit(orm)
				if err != nil {
					var mysqlErr *mysql.MySQLError
					isMysqlErr := errors.As(err, &mysqlErr)
					if isMysqlErr && mysqlErr.Number == 1062 {
						parts := strings.Split(mysqlErr.Message, ".")
						key := strings.Trim(parts[len(parts)-1], "'")
						err = &DuplicateKeyError{Message: mysqlErr.Message, Index: key}
					}
					return
				}
			}
		}()
		if err != nil {
			return err
		}
	}
	for _, pipeline := range orm.redisPipeLines {
		results, pipelineErr := pipeline.Exec(orm)
		if pipelineErr != nil {
			for _, result := range results {
				if result.Err() == nil {
					continue
				}
				if result.Name() == "lset" && result.Err().Error() == "ERR no such key" {
					// entity update on key that does not exists
					continue
				}
				return pipelineErr
			}
		}
	}
	for _, action := range orm.flushPostActions {
		action(orm)
	}
	orm.trackedEntities.Clear()
	orm.flushDBActions = nil
	orm.flushPostActions = orm.flushPostActions[0:0]
	orm.redisPipeLines = nil
	return err
}

func (orm *ormImplementation) ClearFlush() {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	orm.trackedEntities.Clear()
	orm.flushDBActions = nil
	orm.flushPostActions = orm.flushPostActions[0:0]
	orm.redisPipeLines = nil
}

func (orm *ormImplementation) handleDeletes(async bool, schema *entitySchema, operations []EntityFlush) error {
	if !schema.virtual {
		var args []any
		if !async {
			args = make([]any, len(operations))
		}
		sql := "DELETE FROM `" + schema.GetTableName() + "` WHERE ID IN ("
		if async {
			for i, operation := range operations {
				if i > 0 {
					sql += ","
				}
				sql += strconv.FormatUint(operation.ID(), 10)
			}
		} else {
			sql += "?" + strings.Repeat(",?", len(operations)-1)
		}
		sql += ")"
		if !async {
			for i, operation := range operations {
				args[i] = operation.ID()
			}
			orm.appendDBAction(schema, func(db DBBase) error {
				_, err := db.Exec(orm, sql, args...)
				return err
			})
		} else {
			r, err := getRedisForStream(orm, LazyChannelName)
			if err != nil {
				return err
			}
			e, err := createEventSlice([]any{sql, schema.mysqlPoolCode}, nil)
			if err != nil {
				return err
			}
			orm.RedisPipeLine(r.GetCode()).XAdd(LazyChannelName, e)
		}
	}

	lc, hasLocalCache := schema.GetLocalCache()
	for _, operation := range operations {
		uniqueIndexes := schema.GetUniqueIndexes()
		var bind Bind
		var err error
		var idAsString string
		deleteFlush := operation.(entityFlushDelete)
		if len(uniqueIndexes) > 0 {
			bind, err = deleteFlush.getOldBind()
			if err != nil {
				return err
			}
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, indexColumns := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey, err := buildUniqueKeyHSetField(schema, indexColumns, bind, nil)
				if err != nil {
					return err
				}
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hField)
					if schema.cacheTTL > 0 {
						orm.RedisPipeLine(cache.GetConfig().GetCode()).Expire(hSetKey, time.Duration(schema.cacheTTL)*time.Second)
					}
				}
			}
		}
		if hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
				lc.setEntity(orm, operation.ID(), nil)
			})
		}
		rc, hasRedisCache := schema.GetRedisCache()
		if hasRedisCache {
			idAsString = strconv.FormatUint(operation.ID(), 10)
			cacheKey := schema.getCacheKey() + ":" + idAsString
			orm.RedisPipeLine(rc.GetCode()).Del(cacheKey)
			orm.RedisPipeLine(rc.GetCode()).LPush(cacheKey, "")
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(rc.GetCode()).Expire(cacheKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
					lc.removeList(orm, cacheAllKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllKey
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		for indexName, def := range schema.cachedIndexes {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				indexAttributes[j] = bind[indexColumn]
			}
			key := indexName
			id, err := hashIndexAttributes(indexAttributes)
			if err != nil {
				return err
			}
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
					lc.removeList(orm, key, id)
				})
			}
			redisSetKey := schema.cacheKey + ":" + key + ":" + strconv.FormatUint(id, 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, strconv.FormatUint(deleteFlush.ID(), 10))
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		rsPoolCode := schema.redisSearchIndexPoolCode
		if rsPoolCode != "" {
			rsPool := orm.RedisPipeLine(rsPoolCode)
			rsKey := schema.redisSearchIndexPrefix + strconv.FormatUint(deleteFlush.ID(), 10)
			rsPool.Del(rsKey)
			if schema.cacheTTL > 0 {
				rsPool.Expire(rsKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}

		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable && !orm.engine.registry.disableLogTables {
			data := make([]any, 7)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`) VALUES(?,?,?,?,?)"
			uuid := logTableSchema.uuid(orm)
			data[1] = strconv.FormatUint(uuid, 10)
			data[2] = strconv.FormatUint(operation.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			data[6] = logTableSchema.mysqlPoolCode
			r, err := getRedisForStream(orm, LogChannelName)
			if err != nil {
				return err
			}
			e, err := createEventSlice(data, nil)
			if err != nil {
				return err
			}
			orm.RedisPipeLine(r.GetCode()).XAdd(LogChannelName, e)
		}
		for _, p := range orm.engine.pluginFlush {
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			after, err := p.EntityFlush(schema, deleteFlush.getValue(), operation.ID(), bind, nil, orm.engine)
			if err != nil {
				return err
			}
			if after != nil {
				orm.flushPostActions = append(orm.flushPostActions, after)
			}
		}
		if len(schema.dirtyDeleted) > 0 {
			rsPool := orm.RedisPipeLine(schema.getForcedRedisCode())
			if idAsString == "" {
				idAsString = strconv.FormatUint(operation.ID(), 10)
			}
			if bind == nil {
				bind, err = deleteFlush.getOldBind()
				if err != nil {
					return err
				}
			}
			e, err := createEventSlice(bind, []string{"action", "delete", "entity", schema.t.String(), "id", idAsString})
			if err != nil {
				return err
			}
			for _, dirtyDef := range schema.dirtyDeleted {
				rsPool.XAdd("dirty_"+dirtyDef.Stream, e)
			}
		}
	}
	return nil
}

func (orm *ormImplementation) handleInserts(async bool, schema *entitySchema, operations []EntityFlush) error {
	columns := schema.GetColumns()
	sql := "INSERT INTO `" + schema.GetTableName() + "`(`ID`"
	for _, column := range columns[1:] {
		sql += ",`" + column + "`"
	}
	sql += ") VALUES"
	var args []any
	if !async {
		args = make([]any, 0, len(operations)*len(columns))
	}
	lc, hasLocalCache := schema.GetLocalCache()
	rc, hasRedisCache := schema.GetRedisCache()
	for i, operation := range operations {
		insert := operation.(entityFlushInsert)
		bind, err := insert.getBind()
		if err != nil {
			return err
		}
		if len(orm.engine.pluginFlush) > 0 {
			elem := insert.getValue().Elem()
			for _, p := range orm.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, insert.ID(), nil, bind, orm.engine)
				if err != nil {
					return err
				}
				if after != nil {
					orm.flushPostActions = append(orm.flushPostActions, after)
				}
			}
		}
		uniqueIndexes := schema.cachedUniqueIndexes
		if len(uniqueIndexes) > 0 {
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, definition := range uniqueIndexes {
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey, err := buildUniqueKeyHSetField(schema, definition.Columns, bind, nil)
				if err != nil {
					return err
				}
				if !hasKey {
					continue
				}
				orm.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(insert.ID(), 10))
				if schema.cacheTTL > 0 {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).Expire(hSetKey, time.Duration(schema.cacheTTL)*time.Second)
				}
			}
		}
		var asyncData []any
		if async {
			asyncData = make([]any, len(columns)+2)
		}

		if i > 0 && !async {
			sql += ","
		}
		if !async || i == 0 {
			sql += "(?"
		}
		if !async {
			args = append(args, bind["ID"])
		} else {
			asyncData[2] = strconv.FormatUint(bind["ID"].(uint64), 10)
		}
		for j, column := range columns[1:] {
			v := bind[column]
			if async {
				vAsUint64, isUint64 := v.(uint64)
				if isUint64 {
					v = strconv.FormatUint(vAsUint64, 10)
				}
			}
			if !async {
				args = append(args, v)
			} else {
				asyncData[j+3] = v
			}
			if !async || i == 0 {
				sql += ",?"
			}
		}
		if !async || i == 0 {
			sql += ")"
		}
		if async && !schema.virtual {
			asyncData[0] = sql
			asyncData[1] = schema.mysqlPoolCode
			r, err := getRedisForStream(orm, LazyChannelName)
			if err != nil {
				return err
			}
			e, err := createEventSlice(asyncData, nil)
			if err != nil {
				return err
			}
			orm.RedisPipeLine(r.GetCode()).XAdd(LazyChannelName, e)
		}
		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable && !orm.engine.registry.disableLogTables {
			data := make([]any, 7)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`After`) VALUES(?,?,?,?,?)"
			uuid := logTableSchema.uuid(orm)
			data[1] = strconv.FormatUint(uuid, 10)
			data[2] = strconv.FormatUint(bind["ID"].(uint64), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(bind)
			data[5] = asJSON
			data[6] = logTableSchema.mysqlPoolCode
			r, err := getRedisForStream(orm, LogChannelName)
			if err != nil {
				return err
			}
			e, err := createEventSlice(data, nil)
			if err != nil {
				return err
			}
			orm.RedisPipeLine(r.GetCode()).XAdd(LogChannelName, e)
		}
		if hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
				lc.setEntity(orm, insert.ID(), insert.getEntity())
			})
		}
		if schema.cacheAll {
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
					lc.removeList(orm, cacheAllKey, 0)
				})
			}
			redisSetKey := schema.cacheKey + ":" + cacheAllKey
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		for indexName, def := range schema.cachedIndexes {
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				indexAttributes[j] = bind[indexColumn]
			}
			key := indexName
			id, err := hashIndexAttributes(indexAttributes)
			if err != nil {
				return err
			}
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
					lc.removeList(orm, key, id)
				})
			}
			redisSetKey := schema.cacheKey + ":" + key + ":" + strconv.FormatUint(id, 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, strconv.FormatUint(insert.ID(), 10))
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		var idAsString string
		if hasRedisCache {
			idAsString = strconv.FormatUint(bind["ID"].(uint64), 10)
			listKey := schema.getCacheKey() + ":" + idAsString
			orm.RedisPipeLine(rc.GetCode()).Del(listKey)
			orm.RedisPipeLine(rc.GetCode()).RPush(listKey, convertBindToRedisValue(bind, schema)...)
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(rc.GetCode()).Expire(listKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		rsPoolCode := schema.redisSearchIndexPoolCode
		if rsPoolCode != "" {
			rsPool := orm.RedisPipeLine(rsPoolCode)
			if idAsString == "" {
				idAsString = strconv.FormatUint(bind["ID"].(uint64), 10)
			}
			values := make([]any, len(schema.redisSearchFields)*2)
			k := 0
			for column, def := range schema.redisSearchFields {
				values[k] = column
				values[k+1] = def.convertBindToHashValue(bind[column])
				k += 2
			}
			rsKey := schema.redisSearchIndexPrefix + idAsString
			rsPool.HSet(rsKey, values...)
			if schema.cacheTTL > 0 {
				rsPool.Expire(rsKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		if len(schema.dirtyAdded) > 0 {
			rsPool := orm.RedisPipeLine(schema.getForcedRedisCode())
			if idAsString == "" {
				idAsString = strconv.FormatUint(bind["ID"].(uint64), 10)
			}
			e, err := createEventSlice(bind, []string{"action", "add", "entity", schema.t.String(), "id", idAsString})
			if err != nil {
				return err
			}
			for _, dirtyDef := range schema.dirtyAdded {
				rsPool.XAdd("dirty_"+dirtyDef.Stream, e)
			}
		}
	}
	if !async && !schema.virtual {
		orm.appendDBAction(schema, func(db DBBase) error {
			_, err := db.Exec(orm, sql, args...)
			return err
		})
	}
	return nil
}

func (orm *ormImplementation) handleUpdates(async bool, schema *entitySchema, operations []EntityFlush) error {
	var queryPrefix string
	for _, operation := range operations {
		update := operation.(entityFlushUpdate)
		newBind, oldBind, forcedNew, forcedOld, err := update.getBind()
		elem := update.getValue().Elem()
		if err != nil {
			return err
		}
		if len(newBind) == 0 {
			continue
		}
		if len(orm.engine.pluginFlush) > 0 {
			for _, p := range orm.engine.pluginFlush {
				after, err := p.EntityFlush(schema, elem, operation.ID(), oldBind, newBind, orm.engine)
				if err != nil {
					return err
				}
				if after != nil {
					orm.flushPostActions = append(orm.flushPostActions, after)
				}
			}
		}
		fakeDeleted := false
		if schema.hasFakeDelete {
			v, has := newBind["FakeDelete"]
			if has {
				b, isBool := v.(bool)
				if isBool {
					if b {
						newBind["FakeDelete"] = update.ID()
						fakeDeleted = true
					} else {
						newBind["FakeDelete"] = uint64(0)
					}
				} else {
					i, isUint := v.(uint64)
					if isUint {
						if i > 0 {
							newBind["FakeDelete"] = update.ID()
							fakeDeleted = true
						} else {
							newBind["FakeDelete"] = uint64(0)
						}
					}
				}

			}
		}
		if len(schema.cachedUniqueIndexes) > 0 {
			cache := orm.Engine().Redis(schema.getForcedRedisCode())
			for indexName, definition := range schema.cachedUniqueIndexes {

				if fakeDeleted {
					hSetKey := schema.getCacheKey() + ":" + indexName
					hField, hasKey, err := buildUniqueKeyHSetField(schema, definition.Columns, newBind, forcedNew)
					if err != nil {
						return err
					}
					if hasKey {
						orm.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hField)
					}
					continue
				}
				indexChanged := false
				for _, column := range definition.Columns {
					_, changed := newBind[column]
					if changed {
						indexChanged = true
						break
					}
				}
				if !indexChanged {
					continue
				}
				hSetKey := schema.getCacheKey() + ":" + indexName
				hField, hasKey, err := buildUniqueKeyHSetField(schema, definition.Columns, newBind, forcedNew)
				if err != nil {
					return err
				}
				expireSet := false
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HSet(hSetKey, hField, strconv.FormatUint(update.ID(), 10))
					if schema.cacheTTL > 0 {
						orm.RedisPipeLine(cache.GetConfig().GetCode()).Expire(hSetKey, time.Duration(schema.cacheTTL)*time.Second)
						expireSet = true
					}
				}
				hFieldOld, hasKey, err := buildUniqueKeyHSetField(schema, definition.Columns, oldBind, forcedOld)
				if err != nil {
					return err
				}
				if hasKey {
					orm.RedisPipeLine(cache.GetConfig().GetCode()).HDel(hSetKey, hFieldOld)
					if schema.cacheTTL > 0 && !expireSet {
						orm.RedisPipeLine(cache.GetConfig().GetCode()).Expire(hSetKey, time.Duration(schema.cacheTTL)*time.Second)
					}
				}
			}
		}

		if queryPrefix == "" {
			queryPrefix = "UPDATE `" + schema.GetTableName() + "` SET "
		}
		sql := queryPrefix
		k := 0
		var args []any
		var asyncArgs []any
		if async {
			asyncArgs = make([]any, len(newBind)+3)
		} else {
			args = make([]any, len(newBind)+1)
		}
		for column, value := range newBind {
			if k > 0 {
				sql += ","
			}
			sql += "`" + column + "`=?"
			if async {
				asUint64, isUint64 := value.(uint64)
				if isUint64 {
					value = strconv.FormatUint(asUint64, 10)
				}
				asyncArgs[k+2] = value
			} else {
				args[k] = value
			}
			k++
		}
		sql += " WHERE ID = ?"
		if async {
			asyncArgs[k+2] = strconv.FormatUint(update.ID(), 10)
		} else {
			args[k] = update.ID()
		}
		if !schema.virtual {
			if async {
				asyncArgs[0] = sql
				asyncArgs[1] = schema.mysqlPoolCode
				r, err := getRedisForStream(orm, LazyChannelName)
				if err != nil {
					return err
				}
				e, err := createEventSlice(asyncArgs, nil)
				if err != nil {
					return err
				}
				orm.RedisPipeLine(r.GetCode()).XAdd(LazyChannelName, e)
			} else {
				orm.appendDBAction(schema, func(db DBBase) error {
					_, err := db.Exec(orm, sql, args...)
					return err
				})
			}
		}
		logTableSchema, hasLogTable := orm.engine.registry.entityLogSchemas[schema.t]
		if hasLogTable && !orm.engine.registry.disableLogTables {
			data := make([]any, 8)
			data[0] = "INSERT INTO `" + logTableSchema.tableName + "`(ID,EntityID,Date,Meta,`Before`,`After`) VALUES(?,?,?,?,?,?)"
			uuid := logTableSchema.uuid(orm)
			data[1] = strconv.FormatUint(uuid, 10)
			data[2] = strconv.FormatUint(update.ID(), 10)
			data[3] = time.Now().Format(time.DateTime)
			if len(orm.meta) > 0 {
				asJSON, _ := jsoniter.ConfigFastest.MarshalToString(orm.meta)
				data[4] = asJSON
			} else {
				data[4] = nil
			}
			asJSON, _ := jsoniter.ConfigFastest.MarshalToString(oldBind)
			data[5] = asJSON
			asJSON, _ = jsoniter.ConfigFastest.MarshalToString(newBind)
			data[6] = asJSON
			data[7] = logTableSchema.mysqlPoolCode
			r, err := getRedisForStream(orm, LogChannelName)
			if err != nil {
				return err
			}
			e, err := createEventSlice(data, nil)
			if err != nil {
				return err
			}
			orm.RedisPipeLine(r.GetCode()).XAdd(LogChannelName, e)
		}

		if update.getEntity() == nil {
			for field, newValue := range newBind {
				fSetter := schema.fieldSetters[field]
				if schema.hasLocalCache {
					func() {
						schema.localCache.mutex.Lock()
						defer schema.localCache.mutex.Unlock()
						fSetter(newValue, elem)
					}()
				} else {
					fSetter(newValue, elem)
				}
			}
		} else if schema.hasLocalCache {
			orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
				sourceValue := update.getSourceValue()
				func() {
					schema.localCache.mutex.Lock()
					defer schema.localCache.mutex.Unlock()
					copyEntity(update.getValue().Elem(), sourceValue.Elem(), schema.fields, true)
				}()
				schema.localCache.setEntity(orm, operation.ID(), update.getEntity())
			})
		}

		var idAsString string
		if schema.hasRedisCache {
			p := orm.RedisPipeLine(schema.redisCache.GetCode())
			idAsString = strconv.FormatUint(update.ID(), 10)
			rKey := schema.getCacheKey() + ":" + idAsString
			for column, val := range newBind {
				index := int64(schema.columnMapping[column] + 1)
				p.LSet(rKey, index, convertBindValueToRedisValue(val))
			}
			if schema.cacheTTL > 0 {
				p.Expire(rKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		rsPoolCode := schema.redisSearchIndexPoolCode
		if rsPoolCode != "" {
			managedByFakeDelete := false
			if schema.hasFakeDelete {
				v, has := newBind["FakeDelete"]
				if has {
					managedByFakeDelete = true
					rsPool := orm.RedisPipeLine(rsPoolCode)
					if v.(uint64) > 0 {
						if idAsString == "" {
							idAsString = strconv.FormatUint(update.ID(), 10)
						}
						rsKey := schema.redisSearchIndexPrefix + idAsString
						rsPool.Del(rsKey)
						if schema.cacheTTL > 0 {
							rsPool.Expire(rsKey, time.Duration(schema.cacheTTL)*time.Second)
						}
					} else {
						values := make([]any, 0)
						bind := make(Bind)
						err = fillBindFromOneSource(orm, bind, elem, schema.fields, "")
						if err != nil {
							return err
						}
						for column, def := range schema.redisSearchFields {
							_, has := bind[column]
							if has {
								values = append(values, column)
								values = append(values, def.convertBindToHashValue(bind[column]))
							}
						}
						if len(values) > 0 {
							if idAsString == "" {
								idAsString = strconv.FormatUint(update.ID(), 10)
							}
							rsKey := schema.redisSearchIndexPrefix + idAsString
							rsPool.HSet(rsKey, values...)
							if schema.cacheTTL > 0 {
								rsPool.Expire(rsKey, time.Duration(schema.cacheTTL)*time.Second)
							}
						}
					}
				}
			}
			if !managedByFakeDelete {
				rsPool := orm.RedisPipeLine(rsPoolCode)
				values := make([]any, 0)
				for column, def := range schema.redisSearchFields {
					_, has := newBind[column]
					if has {
						values = append(values, column)
						values = append(values, def.convertBindToHashValue(newBind[column]))
					}
				}
				if len(values) > 0 {
					if idAsString == "" {
						idAsString = strconv.FormatUint(update.ID(), 10)
					}
					rsKey := schema.redisSearchIndexPrefix + idAsString
					rsPool.HSet(rsKey, values...)
					if schema.cacheTTL > 0 {
						rsPool.Expire(rsKey, time.Duration(schema.cacheTTL)*time.Second)
					}
				}
			}
		}
		for indexName, def := range schema.cachedIndexes {
			indexChanged := false
			for _, indexColumn := range def.Columns {
				_, has := newBind[indexColumn]
				if has {
					indexChanged = true
					break
				}
			}
			if !indexChanged && !fakeDeleted {
				continue
			}
			indexAttributes := make([]any, len(def.Columns))
			for j, indexColumn := range def.Columns {
				newVal, has := newBind[indexColumn]
				if !has {
					newVal, has = forcedNew[indexColumn]
				}
				indexAttributes[j] = newVal
				continue
			}
			key := indexName
			id, err := hashIndexAttributes(indexAttributes)
			if err != nil {
				return err
			}
			if !fakeDeleted {
				if schema.hasLocalCache {
					orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
						schema.localCache.removeList(orm, key, id)
					})
				}
				redisSetKey := schema.cacheKey + ":" + key + ":" + strconv.FormatUint(id, 10)
				idAsString = strconv.FormatUint(update.ID(), 10)
				orm.RedisPipeLine(schema.getForcedRedisCode()).SAdd(redisSetKey, idAsString)
				if schema.cacheTTL > 0 {
					orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
				}
			}

			indexAttributes = indexAttributes[0:len(def.Columns)]
			for j, indexColumn := range def.Columns {
				oldVal, has := oldBind[indexColumn]
				if !has {
					oldVal, has = forcedOld[indexColumn]
				}
				indexAttributes[j] = oldVal
			}
			key2 := indexName
			id2, err := hashIndexAttributes(indexAttributes)
			if err != nil {
				return err
			}
			if schema.hasLocalCache {
				orm.flushPostActions = append(orm.flushPostActions, func(_ Context) {
					schema.localCache.removeList(orm, key2, id2)
				})
			}
			redisSetKey := schema.cacheKey + ":" + key2 + ":" + strconv.FormatUint(id2, 10)
			orm.RedisPipeLine(schema.getForcedRedisCode()).SRem(redisSetKey, idAsString)
			if schema.cacheTTL > 0 {
				orm.RedisPipeLine(schema.getForcedRedisCode()).Expire(redisSetKey, time.Duration(schema.cacheTTL)*time.Second)
			}
		}
		if fakeDeleted {
			if len(schema.dirtyDeleted) > 0 {
				rsPool := orm.RedisPipeLine(schema.getForcedRedisCode())
				if idAsString == "" {
					idAsString = strconv.FormatUint(operation.ID(), 10)
				}
				e, err := createEventSlice(newBind, []string{"action", "delete", "entity", schema.t.String(), "id", idAsString})
				if err != nil {
					return err
				}
				for _, dirtyDef := range schema.dirtyDeleted {
					rsPool.XAdd("dirty_"+dirtyDef.Stream, e)
				}
			}
		}
		if !fakeDeleted && len(schema.dirtyUpdated) > 0 {
			rsPool := orm.RedisPipeLine(schema.getForcedRedisCode())
			if idAsString == "" {
				idAsString = strconv.FormatUint(update.ID(), 10)
			}
			var e []string
			for _, dirtyDef := range schema.dirtyUpdated {
				_, hasID := dirtyDef.Columns["ID"]
				if hasID {
					if e == nil {
						e, err = createEventSlice(newBind, []string{"action", "edit", "entity", schema.t.String(), "id", idAsString})
						if err != nil {
							return err
						}
					}
					rsPool.XAdd("dirty_"+dirtyDef.Stream, e)
					continue
				}
				for key := range newBind {
					_, has := dirtyDef.Columns[key]
					if has {
						if e == nil {
							e, err = createEventSlice(newBind, []string{"action", "edit", "entity", schema.t.String(), "id", idAsString})
							if err != nil {
								return err
							}
						}
						rsPool.XAdd("dirty_"+dirtyDef.Stream, e)
						break
					}
				}
			}
		}
	}
	return nil
}

func (orm *ormImplementation) groupSQLOperations() sqlOperations {
	sqlGroup := make(sqlOperations)
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, EntityFlush]) bool {
		value.Range(func(_ uint64, flush EntityFlush) bool {
			schema := flush.Schema()
			db := orm.engine.DB(schema.mysqlPoolCode)
			poolSQLGroup, has := sqlGroup[db]
			if !has {
				poolSQLGroup = make(schemaSQLOperations)
				sqlGroup[db] = poolSQLGroup
			}
			tableSQLGroup, has := poolSQLGroup[schema]
			if !has {
				tableSQLGroup = make(map[FlushType][]EntityFlush)
				poolSQLGroup[schema] = tableSQLGroup
			}
			tableSQLGroup[flush.flushType()] = append(tableSQLGroup[flush.flushType()], flush)
			return true
		})
		return true
	})
	return sqlGroup
}

func (orm *ormImplementation) appendDBAction(schema EntitySchema, action dbAction) {
	if orm.flushDBActions == nil {
		orm.flushDBActions = make(map[string][]dbAction)
	}
	poolCode := schema.GetDB().GetConfig().GetCode()
	orm.flushDBActions[poolCode] = append(orm.flushDBActions[poolCode], action)
}

func buildUniqueKeyHSetField(schema *entitySchema, indexColumns []string, bind, forced Bind) (string, bool, error) {
	hField := ""
	hasNil := false
	hasInBind := false
	for _, column := range indexColumns {
		bindValue, has := bind[column]
		if !has {
			bindValue, has = forced[column]
		}
		if bindValue == nil {
			hasNil = true
			break
		}
		if has {
			hasInBind = true
		}
		asString, err := schema.columnAttrToStringSetters[column](bindValue, true)
		if err != nil {
			return "", false, err
		}
		hField += asString
	}
	if hasNil || !hasInBind {
		return "", false, nil
	}
	return hashString(hField), true, nil
}
