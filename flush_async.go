package fluxaorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/puzpuzpuz/xsync/v2"
	"github.com/shamaton/msgpack"
)

const (
	AsyncSQLTopicName           = "_fluxa_async_sql"
	AsyncSQLDeadLetterTopicName = "_fluxa_async_sql_failed"
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

// AsyncSQLOperation holds one or more SQL queries for a single DB pool and table.
// Multiple queries are executed in a transaction.
// This type is also used in the dead-letter topic so consumers can inspect
// and replay failed operations.
type AsyncSQLOperation struct {
	Pool     string             `msgpack:"pool"`
	Queries  []AsyncSQLQuery    `msgpack:"queries"`
	Events   []AsyncEntityEvent `msgpack:"events,omitempty"`
	RedisOps []AsyncRedisOp     `msgpack:"redis,omitempty"`
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

// AsyncSQLConsumer reads async SQL events from Kafka and executes them against MySQL.
type AsyncSQLConsumer interface {
	Consume(count int, blockTime time.Duration) error
	Close()
}

// FlushAsync is like Flush() but instead of executing SQL directly in MySQL,
// it publishes the SQL queries to a Kafka topic. When immediateRedisUpdates
// is true, Redis cache and search indexes are updated immediately (optimistic
// update). When false, Redis operations are serialized into the Kafka record and
// executed by the consumer after SQL. Call GetAsyncSQLConsumer() to process
// the queued SQL operations.
func (orm *ormImplementation) FlushAsync(immediateRedisUpdates bool) error {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}

	kafkaPoolCode := orm.engine.registry.asyncFlushKafkaPool
	if kafkaPoolCode == "" {
		return fmt.Errorf("async flush not configured: call RegisterAsyncFlush() during registry setup")
	}
	kafkaPool := orm.engine.Kafka(kafkaPoolCode)
	if kafkaPool == nil {
		return fmt.Errorf("kafka pool '%s' not found for async flush", kafkaPoolCode)
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

	// Step 3: collect deferred Redis ops (if not immediate mode)
	var allRedisOps []AsyncRedisOp
	if !immediateRedisUpdates {
		for _, redisPipeline := range orm.redisPipeLines {
			allRedisOps = append(allRedisOps, redisPipeline.GetRecordedOps()...)
		}
	}

	// Step 4: group queries by (pool, tableName) and produce Kafka records
	type tableGroup struct {
		queryIndexes []int
	}
	var records []*KafkaRecord
	firstRecord := true
	for pool, dbPipeline := range orm.dbPipeLines {
		if len(dbPipeline.queries) == 0 {
			continue
		}
		// Group queries by table name
		groups := make(map[string]*tableGroup)
		for i, table := range dbPipeline.tables {
			if table == "" {
				table = "_default"
			}
			g, ok := groups[table]
			if !ok {
				g = &tableGroup{}
				groups[table] = g
			}
			g.queryIndexes = append(g.queryIndexes, i)
		}
		for table, group := range groups {
			queries := make([]AsyncSQLQuery, len(group.queryIndexes))
			for qi, idx := range group.queryIndexes {
				q := dbPipeline.queries[idx]
				params := make([]AsyncSQLParam, len(dbPipeline.parameters[idx]))
				for j, p := range dbPipeline.parameters[idx] {
					params[j] = convertParam(p)
				}
				queries[qi] = AsyncSQLQuery{Q: q, P: params}
			}
			op := AsyncSQLOperation{Pool: pool, Queries: queries, Events: entityEventsByPool[pool]}

			// Attach deferred Redis ops to the first record
			if firstRecord && len(allRedisOps) > 0 {
				op.RedisOps = allRedisOps
				allRedisOps = nil
				firstRecord = false
			}

			value, err := msgpack.Marshal(op)
			if err != nil {
				if !immediateRedisUpdates {
					orm.redisRecordMode = false
				}
				return err
			}
			key := []byte(table)
			if table == "_default" {
				key = []byte(pool)
			}
			records = append(records, &KafkaRecord{
				Topic: AsyncSQLTopicName,
				Key:   key,
				Value: value,
			})
		}
	}

	// Step 5: clear DB pipelines — queries will go to Kafka, not MySQL directly
	orm.dbPipeLines = nil

	// Step 6: execute Redis pipelines (only in immediate mode)
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

	// Step 7: produce records to Kafka
	if len(records) > 0 {
		if err := kafkaPool.ProduceSync(orm, records...); err != nil {
			return err
		}
	}

	// Step 8: mark entities as flushed and clear tracked set
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

// GetAsyncSQLConsumer returns a consumer that reads SQL events from Kafka
// and executes them against MySQL. Permanent MySQL errors are moved to the dead-letter
// topic. Transient errors stop processing and are returned to the caller.
func (orm *ormImplementation) GetAsyncSQLConsumer() (AsyncSQLConsumer, error) {
	kafkaPoolCode := orm.engine.registry.asyncFlushKafkaPool
	if kafkaPoolCode == "" {
		return nil, fmt.Errorf("async flush not configured: call RegisterAsyncFlush() during registry setup")
	}
	kafkaPool := orm.engine.Kafka(kafkaPoolCode)
	if kafkaPool == nil {
		return nil, fmt.Errorf("kafka pool '%s' not found for async flush", kafkaPoolCode)
	}
	cg, err := kafkaPool.ConsumerGroup(AsyncSQLTopicName)
	if err != nil {
		return nil, err
	}
	return &asyncSQLConsumerImpl{cg: cg, kafkaPool: kafkaPool, ctx: orm}, nil
}

type asyncSQLConsumerImpl struct {
	cg        KafkaConsumerGroup
	kafkaPool Kafka
	ctx       *ormImplementation
}

func (c *asyncSQLConsumerImpl) Consume(count int, blockTime time.Duration) error {
	ctx, cancel := context.WithTimeout(c.ctx.context, blockTime)
	defer cancel()
	consumeCtx := c.ctx.engine.NewContext(ctx)
	fetches := c.cg.PollFetches(consumeCtx)
	if fetches.IsEmpty() {
		return nil
	}
	processed := 0
	var processErr error
	fetches.EachRecord(func(record *KafkaRecord) {
		if processErr != nil || (count > 0 && processed >= count) {
			return
		}
		var op AsyncSQLOperation
		if err := msgpack.Unmarshal(record.Value, &op); err != nil {
			// Bad format: move to dead-letter
			c.deadLetter(record, err.Error())
			processed++
			return
		}

		// Pre-load entities for DELETE before SQL execution
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
				c.deadLetter(record, err.Error())
				processed++
				return
			}
			processErr = err
			return
		}

		// Execute deferred Redis ops after SQL success
		if len(op.RedisOps) > 0 {
			if err := c.executeRedisOps(op.RedisOps); err != nil {
				processErr = err
				return
			}
		}

		// Fire hooks after SQL execution
		if len(op.Events) > 0 {
			if err := c.fireEntityHooks(op.Events, preLoadedDeleteEntities); err != nil {
				processErr = err
				return
			}
		}

		processed++
	})
	if processErr != nil {
		return processErr
	}
	return c.cg.CommitUncommittedOffsets(c.ctx)
}

func (c *asyncSQLConsumerImpl) Close() {
	c.cg.Close()
}

func (c *asyncSQLConsumerImpl) deadLetter(record *KafkaRecord, errMsg string) {
	_ = c.kafkaPool.ProduceSync(c.ctx, &KafkaRecord{
		Topic: AsyncSQLDeadLetterTopicName,
		Key:   record.Key,
		Value: record.Value,
		Headers: []KafkaRecordHeader{
			{Key: "error", Value: []byte(errMsg)},
		},
	})
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
			case "hdel":
				if len(op.Args) < 2 {
					continue
				}
				redisPipeline.HDel(op.Args[0], op.Args[1:]...)
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
