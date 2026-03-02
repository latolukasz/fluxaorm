package fluxaorm

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/puzpuzpuz/xsync/v2"
)

const (
	AsyncSQLStreamName           = "_fluxa_async_sql"
	AsyncSQLDeadLetterStreamName = "_fluxa_async_sql_failed"
)

// AsyncSQLParam is a typed parameter envelope for serializing SQL params.
type AsyncSQLParam struct {
	Null bool   `msgpack:"n,omitempty"`
	Type string `msgpack:"t,omitempty"` // "s","i","u","f","b","t"
	Val  string `msgpack:"v,omitempty"`
}

// Value converts the parameter back to a Go type.
func (p AsyncSQLParam) Value() any {
	return deconvertChangeValue(p)
}

// AsyncSQLQuery holds a single SQL statement with its typed parameters.
type AsyncSQLQuery struct {
	Q string          `msgpack:"q"`
	P []AsyncSQLParam `msgpack:"p"`
}

// AsyncEntityEvent holds metadata about an entity change for firing hooks in the async consumer.
type AsyncEntityEvent struct {
	CacheIndex uint64                   `msgpack:"ci"`
	EntityID   uint64                   `msgpack:"id"`
	FlushType  uint8                    `msgpack:"ft"` // 1=insert, 2=update, 3=delete
	Changes    map[string]AsyncSQLParam `msgpack:"ch,omitempty"`
}

// AsyncRedisOp represents a recorded Redis command for deferred execution.
type AsyncRedisOp struct {
	Pool string   `msgpack:"p"`
	Cmd  string   `msgpack:"c"`
	Args []string `msgpack:"a,omitempty"`
}

// AsyncDirtyStreamEvent holds a dirty stream event for deferred publishing.
type AsyncDirtyStreamEvent struct {
	Stream     string                      `msgpack:"s"`
	EntityType string                      `msgpack:"et"`
	EntityID   uint64                      `msgpack:"id"`
	FlushType  uint8                       `msgpack:"ft"`
	Changes    map[string]DirtyFieldChange `msgpack:"ch,omitempty"`
}

// AsyncSQLOperation holds one or more SQL queries for a single DB pool.
// Multiple queries are executed in a transaction.
// This type is also used in the dead-letter stream so consumers can inspect
// and replay failed operations.
type AsyncSQLOperation struct {
	Pool              string                  `msgpack:"pool"`
	Queries           []AsyncSQLQuery         `msgpack:"queries"`
	Events            []AsyncEntityEvent      `msgpack:"events,omitempty"`
	RedisOps          []AsyncRedisOp          `msgpack:"redis,omitempty"`
	DirtyStreamEvents []AsyncDirtyStreamEvent `msgpack:"dirty,omitempty"`
}

// convertParam converts a Go SQL parameter value to a typed AsyncSQLParam.
func convertParam(v any) AsyncSQLParam {
	if v == nil {
		return AsyncSQLParam{Null: true}
	}
	switch val := v.(type) {
	case string:
		return AsyncSQLParam{Type: "s", Val: val}
	case int64:
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(val, 10)}
	case uint64:
		return AsyncSQLParam{Type: "u", Val: strconv.FormatUint(val, 10)}
	case float64:
		return AsyncSQLParam{Type: "f", Val: strconv.FormatFloat(val, 'g', -1, 64)}
	case bool:
		if val {
			return AsyncSQLParam{Type: "b", Val: "1"}
		}
		return AsyncSQLParam{Type: "b", Val: "0"}
	case time.Time:
		return AsyncSQLParam{Type: "t", Val: val.UTC().Format(time.RFC3339)}
	case sql.NullString:
		if !val.Valid {
			return AsyncSQLParam{Null: true}
		}
		return AsyncSQLParam{Type: "s", Val: val.String}
	case sql.NullInt64:
		if !val.Valid {
			return AsyncSQLParam{Null: true}
		}
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(val.Int64, 10)}
	case sql.NullFloat64:
		if !val.Valid {
			return AsyncSQLParam{Null: true}
		}
		return AsyncSQLParam{Type: "f", Val: strconv.FormatFloat(val.Float64, 'g', -1, 64)}
	case sql.NullBool:
		if !val.Valid {
			return AsyncSQLParam{Null: true}
		}
		if val.Bool {
			return AsyncSQLParam{Type: "b", Val: "1"}
		}
		return AsyncSQLParam{Type: "b", Val: "0"}
	case sql.NullTime:
		if !val.Valid {
			return AsyncSQLParam{Null: true}
		}
		return AsyncSQLParam{Type: "t", Val: val.Time.UTC().Format(time.RFC3339)}
	}
	return AsyncSQLParam{Type: "s", Val: fmt.Sprintf("%v", v)}
}

// deconvertParam converts an AsyncSQLParam back to a Go value accepted by the MySQL driver.
func deconvertParam(p AsyncSQLParam) any {
	if p.Null {
		return nil
	}
	switch p.Type {
	case "s":
		return p.Val
	case "i":
		v, _ := strconv.ParseInt(p.Val, 10, 64)
		return v
	case "u":
		v, _ := strconv.ParseUint(p.Val, 10, 64)
		return v
	case "f":
		v, _ := strconv.ParseFloat(p.Val, 64)
		return v
	case "b":
		return p.Val == "1"
	case "t":
		v, _ := time.Parse(time.RFC3339, p.Val)
		return v
	}
	return p.Val
}

// convertChangeValue converts a Go value from the changes map to an AsyncSQLParam.
// Unlike convertParam, this handles the unwrapped types returned by privateGetOriginalColumnValue
// (e.g. uint32, int8) rather than sql.Null* types.
func convertChangeValue(v any) AsyncSQLParam {
	if v == nil {
		return AsyncSQLParam{Null: true}
	}
	switch val := v.(type) {
	case string:
		return AsyncSQLParam{Type: "s", Val: val}
	case uint64:
		return AsyncSQLParam{Type: "u", Val: strconv.FormatUint(val, 10)}
	case uint32:
		return AsyncSQLParam{Type: "u", Val: strconv.FormatUint(uint64(val), 10)}
	case uint16:
		return AsyncSQLParam{Type: "u", Val: strconv.FormatUint(uint64(val), 10)}
	case uint8:
		return AsyncSQLParam{Type: "u", Val: strconv.FormatUint(uint64(val), 10)}
	case int64:
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(val, 10)}
	case int32:
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(int64(val), 10)}
	case int16:
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(int64(val), 10)}
	case int8:
		return AsyncSQLParam{Type: "i", Val: strconv.FormatInt(int64(val), 10)}
	case float64:
		return AsyncSQLParam{Type: "f", Val: strconv.FormatFloat(val, 'g', -1, 64)}
	case float32:
		return AsyncSQLParam{Type: "f", Val: strconv.FormatFloat(float64(val), 'g', -1, 64)}
	case bool:
		if val {
			return AsyncSQLParam{Type: "b", Val: "1"}
		}
		return AsyncSQLParam{Type: "b", Val: "0"}
	case time.Time:
		return AsyncSQLParam{Type: "t", Val: val.UTC().Format(time.RFC3339)}
	}
	return AsyncSQLParam{Type: "s", Val: fmt.Sprintf("%v", v)}
}

// deconvertChangeValue converts an AsyncSQLParam back to a simple Go type for the changes map.
func deconvertChangeValue(p AsyncSQLParam) any {
	if p.Null {
		return nil
	}
	switch p.Type {
	case "s":
		return p.Val
	case "u":
		v, _ := strconv.ParseUint(p.Val, 10, 64)
		return v
	case "i":
		v, _ := strconv.ParseInt(p.Val, 10, 64)
		return v
	case "f":
		v, _ := strconv.ParseFloat(p.Val, 64)
		return v
	case "b":
		return p.Val == "1"
	case "t":
		v, _ := time.Parse(time.RFC3339, p.Val)
		return v
	}
	return p.Val
}

func convertChanges(m map[string]any) map[string]AsyncSQLParam {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]AsyncSQLParam, len(m))
	for k, v := range m {
		result[k] = convertChangeValue(v)
	}
	return result
}

func deconvertChanges(m map[string]AsyncSQLParam) map[string]any {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = deconvertChangeValue(v)
	}
	return result
}

// AsyncSQLConsumer reads async SQL events from the stream and executes them against MySQL.
type AsyncSQLConsumer interface {
	Consume(count int, blockTime time.Duration) error
	AutoClaim(count int, minIdle time.Duration) error
}

// FlushAsync is like Flush() but instead of executing SQL directly in MySQL,
// it publishes the SQL queries to a Redis Stream. When immediateRedisUpdates
// is true, Redis cache and search indexes are updated immediately (optimistic
// update). When false, Redis operations are serialized into the stream and
// executed by the consumer after SQL. Call GetAsyncSQLConsumer() to process
// the queued SQL operations.
func (orm *ormImplementation) FlushAsync(immediateRedisUpdates bool) error {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}

	// Enable recording mode when deferring Redis updates
	if !immediateRedisUpdates {
		orm.redisRecordMode = true
	}

	// Step 1: call PrivateFlush on all tracked entities → populates dbPipeLines and redisPipeLines
	var flushErr error
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Entity]) bool {
		value.Range(func(_ uint64, e Entity) bool {
			flushErr = e.PrivateFlush()
			return flushErr == nil
		})
		return flushErr == nil
	})
	if flushErr != nil {
		if !immediateRedisUpdates {
			orm.redisRecordMode = false
		}
		return flushErr
	}

	// Step 2: collect entity events for async hook firing (if any handlers are registered)
	var entityEventsByPool map[string][]AsyncEntityEvent
	if orm.engine.entityLoaders != nil {
		entityEventsByPool = make(map[string][]AsyncEntityEvent)
		orm.trackedEntities.Range(func(cacheIndex uint64, value *xsync.MapOf[uint64, Entity]) bool {
			pool, hasPool := orm.engine.entityDBPools[cacheIndex]
			if !hasPool {
				return true
			}
			hasInsert := orm.engine.afterInsertHandlers != nil && orm.engine.afterInsertHandlers[cacheIndex] != nil
			hasUpdate := orm.engine.afterUpdateHandlers != nil && orm.engine.afterUpdateHandlers[cacheIndex] != nil
			hasDelete := orm.engine.afterDeleteHandlers != nil && orm.engine.afterDeleteHandlers[cacheIndex] != nil
			if !hasInsert && !hasUpdate && !hasDelete {
				return true
			}
			value.Range(func(_ uint64, e Entity) bool {
				eventType, changes := e.PrivateFlushEvent()
				if eventType == 0 {
					return true
				}
				if (eventType == 1 && !hasInsert) || (eventType == 2 && !hasUpdate) || (eventType == 3 && !hasDelete) {
					return true
				}
				ev := AsyncEntityEvent{CacheIndex: cacheIndex, EntityID: e.GetID(), FlushType: eventType}
				if eventType == 2 && changes != nil {
					ev.Changes = convertChanges(changes)
				}
				entityEventsByPool[pool] = append(entityEventsByPool[pool], ev)
				return true
			})
			return true
		})
	}

	// Step 2b: collect dirty stream events
	var dirtyStreamEventsByPool map[string][]AsyncDirtyStreamEvent
	for _, schema := range orm.engine.registry.entitySchemas {
		if !schema.hasDirtyStreams {
			continue
		}
		if orm.trackedEntities == nil {
			break
		}
		entities, ok := orm.trackedEntities.Load(schema.index)
		if !ok {
			continue
		}
		entities.Range(func(_ uint64, e Entity) bool {
			flushType, changes := e.PrivateFlushEvent()
			if flushType == 0 {
				return true
			}
			databaseBind := e.PrivateGetDatabaseBind()
			pool, hasPool := orm.engine.entityDBPools[schema.index]
			if !hasPool {
				pool = schema.mysqlPoolCode
			}
			for _, ds := range schema.dirtyStreams {
				publish := false
				switch flushType {
				case 1:
					publish = ds.onInsert
				case 2:
					if ds.onUpdate {
						publish = true
					} else if len(ds.fieldTriggers) > 0 && changes != nil {
						for _, ft := range ds.fieldTriggers {
							if _, ok := changes[ft]; ok {
								publish = true
								break
							}
						}
					}
				case 3:
					publish = ds.onDelete
				}
				if !publish {
					continue
				}
				ev := AsyncDirtyStreamEvent{
					Stream:     ds.streamName,
					EntityType: schema.tableName,
					EntityID:   e.GetID(),
					FlushType:  flushType,
				}
				if flushType == 2 && changes != nil {
					ev.Changes = make(map[string]DirtyFieldChange, len(changes))
					for field, oldVal := range changes {
						newVal := databaseBind[field]
						ev.Changes[field] = DirtyFieldChange{
							Old: convertChangeValue(oldVal),
							New: convertChangeValue(newVal),
						}
					}
				}
				if dirtyStreamEventsByPool == nil {
					dirtyStreamEventsByPool = make(map[string][]AsyncDirtyStreamEvent)
				}
				dirtyStreamEventsByPool[pool] = append(dirtyStreamEventsByPool[pool], ev)
			}
			return true
		})
	}

	// Step 3: serialize each non-empty DatabasePipeline into AsyncSQLOperation events
	broker := orm.GetEventBroker()
	eventFlusher := broker.NewFlusher()
	firstPool := true
	for pool, dbPipeline := range orm.dbPipeLines {
		if len(dbPipeline.queries) == 0 {
			continue
		}
		queries := make([]AsyncSQLQuery, len(dbPipeline.queries))
		for i, q := range dbPipeline.queries {
			params := make([]AsyncSQLParam, len(dbPipeline.parameters[i]))
			for j, p := range dbPipeline.parameters[i] {
				params[j] = convertParam(p)
			}
			queries[i] = AsyncSQLQuery{Q: q, P: params}
		}
		op := AsyncSQLOperation{Pool: pool, Queries: queries, Events: entityEventsByPool[pool], DirtyStreamEvents: dirtyStreamEventsByPool[pool]}

		// Attach deferred Redis ops to the first operation
		if !immediateRedisUpdates && firstPool {
			var allRedisOps []AsyncRedisOp
			for _, redisPipeline := range orm.redisPipeLines {
				allRedisOps = append(allRedisOps, redisPipeline.GetRecordedOps()...)
			}
			if len(allRedisOps) > 0 {
				op.RedisOps = allRedisOps
			}
			firstPool = false
		}

		if err := eventFlusher.Publish(AsyncSQLStreamName, op); err != nil {
			if !immediateRedisUpdates {
				orm.redisRecordMode = false
			}
			return err
		}
	}

	// Step 4: clear DB pipelines — queries will go to the stream, not MySQL directly
	orm.dbPipeLines = nil

	// Step 5: execute Redis pipelines (only in immediate mode)
	if immediateRedisUpdates {
		for _, redisPipeline := range orm.redisPipeLines {
			if _, err := redisPipeline.Exec(orm); err != nil {
				return err
			}
		}
	} else {
		orm.redisRecordMode = false
		orm.redisPipeLines = nil
	}

	// Step 6: publish the serialized SQL events to the async stream
	if err := eventFlusher.Flush(); err != nil {
		return err
	}

	// Step 7: mark entities as flushed and clear tracked set
	orm.trackedEntities.Range(func(_ uint64, value *xsync.MapOf[uint64, Entity]) bool {
		value.Range(func(_ uint64, e Entity) bool {
			e.PrivateFlushed()
			return true
		})
		return true
	})
	orm.trackedEntities.Clear()
	return nil
}

// GetAsyncSQLConsumer returns a consumer that reads SQL events from the async stream
// and executes them against MySQL. Permanent MySQL errors are moved to the dead-letter
// stream. Transient errors stop processing and are returned to the caller.
func (orm *ormImplementation) GetAsyncSQLConsumer() (AsyncSQLConsumer, error) {
	broker := orm.GetEventBroker()
	consumer, err := broker.ConsumerSingle(orm, AsyncSQLStreamName)
	if err != nil {
		return nil, err
	}
	return &asyncSQLConsumerImpl{inner: consumer, ctx: orm}, nil
}

type asyncSQLConsumerImpl struct {
	inner EventsConsumer
	ctx   *ormImplementation
}

func (c *asyncSQLConsumerImpl) Consume(count int, blockTime time.Duration) error {
	return c.inner.Consume(count, blockTime, c.handleEvents)
}

func (c *asyncSQLConsumerImpl) AutoClaim(count int, minIdle time.Duration) error {
	return c.inner.AutoClaim(count, minIdle, c.handleEvents)
}

func (c *asyncSQLConsumerImpl) executeRedisOps(ops []AsyncRedisOp) error {
	byPool := make(map[string][]AsyncRedisOp)
	for _, op := range ops {
		byPool[op.Pool] = append(byPool[op.Pool], op)
	}
	for pool, poolOps := range byPool {
		redisPipeline := c.ctx.RedisPipeLine(pool)
		for _, op := range poolOps {
			switch op.Cmd {
			case "rpush":
				if len(op.Args) < 2 {
					continue
				}
				values := make([]any, len(op.Args)-1)
				for i, v := range op.Args[1:] {
					values[i] = v
				}
				redisPipeline.RPush(op.Args[0], values...)
			case "lset":
				if len(op.Args) < 3 {
					continue
				}
				index, _ := strconv.ParseInt(op.Args[1], 10, 64)
				redisPipeline.LSet(op.Args[0], index, op.Args[2])
			case "del":
				redisPipeline.Del(op.Args...)
			case "hset":
				if len(op.Args) < 3 {
					continue
				}
				values := make([]any, len(op.Args)-1)
				for i, v := range op.Args[1:] {
					values[i] = v
				}
				redisPipeline.HSet(op.Args[0], values...)
			case "set":
				if len(op.Args) < 3 {
					continue
				}
				expSec, _ := strconv.ParseInt(op.Args[2], 10, 64)
				redisPipeline.Set(op.Args[0], op.Args[1], time.Duration(expSec)*time.Second)
			}
		}
		if _, err := redisPipeline.Exec(c.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *asyncSQLConsumerImpl) handleEvents(events []Event) error {
	broker := c.ctx.GetEventBroker()
	for _, ev := range events {
		var op AsyncSQLOperation
		if err := ev.Unserialize(&op); err != nil {
			// Bad format: move to dead-letter and acknowledge
			_, _ = broker.Publish(AsyncSQLDeadLetterStreamName, op, "error", err.Error())
			_ = ev.Ack()
			continue
		}

		// Pre-load entities for DELETE before SQL execution (they'll be gone after hard delete)
		var preLoadedDeleteEntities map[uint64]Entity
		if len(op.Events) > 0 && c.ctx.engine.entityLoaders != nil {
			for _, entityEvent := range op.Events {
				if entityEvent.FlushType != 3 {
					continue
				}
				loader, hasLoader := c.ctx.engine.entityLoaders[entityEvent.CacheIndex]
				if !hasLoader {
					continue
				}
				loadCtx := c.ctx.engine.NewContext(c.ctx.context)
				entity, found, err := loader(loadCtx, entityEvent.EntityID)
				if err != nil || !found {
					continue
				}
				if preLoadedDeleteEntities == nil {
					preLoadedDeleteEntities = make(map[uint64]Entity)
				}
				preLoadedDeleteEntities[entityEvent.EntityID] = entity
			}
		}

		if err := c.executeOperation(op); err != nil {
			if isPermanentMySQLError(err) {
				_, _ = broker.Publish(AsyncSQLDeadLetterStreamName, op, "error", err.Error())
				_ = ev.Ack()
				continue
			}
			// Transient error: stop processing, leave event in pending
			return err
		}
		_ = ev.Ack()

		// Execute deferred Redis ops after SQL success
		if len(op.RedisOps) > 0 {
			if err := c.executeRedisOps(op.RedisOps); err != nil {
				return err
			}
		}

		// Fire hooks after SQL execution and ACK
		if len(op.Events) > 0 {
			if err := c.fireEntityHooks(op.Events, preLoadedDeleteEntities); err != nil {
				return err
			}
		}

		// Publish dirty stream events after SQL execution
		if len(op.DirtyStreamEvents) > 0 {
			if err := c.publishDirtyStreamEvents(op.DirtyStreamEvents); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *asyncSQLConsumerImpl) fireEntityHooks(events []AsyncEntityEvent, preLoadedDeleteEntities map[uint64]Entity) error {
	for _, entityEvent := range events {
		switch entityEvent.FlushType {
		case 1: // INSERT
			handler, ok := c.ctx.engine.afterInsertHandlers[entityEvent.CacheIndex]
			if !ok {
				continue
			}
			loader, hasLoader := c.ctx.engine.entityLoaders[entityEvent.CacheIndex]
			if !hasLoader {
				continue
			}
			loadCtx := c.ctx.engine.NewContext(c.ctx.context)
			entity, found, err := loader(loadCtx, entityEvent.EntityID)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if err = handler(loadCtx, entity); err != nil {
				return err
			}
		case 2: // UPDATE
			handler, ok := c.ctx.engine.afterUpdateHandlers[entityEvent.CacheIndex]
			if !ok {
				continue
			}
			loader, hasLoader := c.ctx.engine.entityLoaders[entityEvent.CacheIndex]
			if !hasLoader {
				continue
			}
			loadCtx := c.ctx.engine.NewContext(c.ctx.context)
			entity, found, err := loader(loadCtx, entityEvent.EntityID)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			changes := deconvertChanges(entityEvent.Changes)
			if err = handler(loadCtx, entity, changes); err != nil {
				return err
			}
		case 3: // DELETE
			handler, ok := c.ctx.engine.afterDeleteHandlers[entityEvent.CacheIndex]
			if !ok {
				continue
			}
			// Try loading from DB first (works for FakeDelete/soft-delete, entity still exists)
			loader, hasLoader := c.ctx.engine.entityLoaders[entityEvent.CacheIndex]
			if !hasLoader {
				continue
			}
			loadCtx := c.ctx.engine.NewContext(c.ctx.context)
			entity, found, err := loader(loadCtx, entityEvent.EntityID)
			if err != nil {
				return err
			}
			if !found {
				// Hard delete: use pre-loaded entity
				entity = preLoadedDeleteEntities[entityEvent.EntityID]
				if entity == nil {
					continue
				}
			}
			if err = handler(loadCtx, entity); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *asyncSQLConsumerImpl) publishDirtyStreamEvents(events []AsyncDirtyStreamEvent) error {
	broker := c.ctx.GetEventBroker()
	flusher := broker.NewFlusher()
	for _, ev := range events {
		dsEvent := DirtyStreamEvent{
			EntityType: ev.EntityType,
			EntityID:   ev.EntityID,
			FlushType:  ev.FlushType,
			Changes:    ev.Changes,
		}
		if err := flusher.Publish(ev.Stream, dsEvent); err != nil {
			return err
		}
	}
	return flusher.Flush()
}

func (c *asyncSQLConsumerImpl) executeOperation(op AsyncSQLOperation) error {
	db := c.ctx.engine.DB(op.Pool)
	if db == nil {
		return fmt.Errorf("unknown DB pool: %s", op.Pool)
	}
	if len(op.Queries) == 1 {
		params := make([]any, len(op.Queries[0].P))
		for i, p := range op.Queries[0].P {
			params[i] = deconvertParam(p)
		}
		_, err := db.Exec(c.ctx, op.Queries[0].Q, params...)
		return err
	}
	tr, err := db.Begin(c.ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tr.Rollback(c.ctx)
	}()
	for _, q := range op.Queries {
		params := make([]any, len(q.P))
		for i, p := range q.P {
			params[i] = deconvertParam(p)
		}
		if _, err = tr.Exec(c.ctx, q.Q, params...); err != nil {
			return err
		}
	}
	return tr.Commit(c.ctx)
}

// isPermanentMySQLError returns true for MySQL errors that are unlikely to succeed on retry
// (e.g. duplicate key, missing table, parse error) vs. transient errors (e.g. connection refused).
func isPermanentMySQLError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1062, // ER_DUP_ENTRY
		1146, // ER_NO_SUCH_TABLE
		1054, // ER_BAD_FIELD_ERROR
		1064, // ER_PARSE_ERROR
		1406, // ER_DATA_TOO_LONG
		1048, // ER_BAD_NULL_ERROR
		1292, // ER_TRUNCATED_WRONG_VALUE
		1366, // ER_WARN_DATA_OUT_OF_RANGE
		1452, // ER_NO_REFERENCED_ROW_2
		1216, // ER_NO_REFERENCED_ROW
		1217: // ER_ROW_IS_REFERENCED
		return true
	}
	return false
}
