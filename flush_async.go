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

// AsyncSQLQuery holds a single SQL statement with its typed parameters.
type AsyncSQLQuery struct {
	Q string          `msgpack:"q"`
	P []AsyncSQLParam `msgpack:"p"`
}

// AsyncSQLOperation holds one or more SQL queries for a single DB pool.
// Multiple queries are executed in a transaction.
// This type is also used in the dead-letter stream so consumers can inspect
// and replay failed operations.
type AsyncSQLOperation struct {
	Pool    string          `msgpack:"pool"`
	Queries []AsyncSQLQuery `msgpack:"queries"`
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

// AsyncSQLConsumer reads async SQL events from the stream and executes them against MySQL.
type AsyncSQLConsumer interface {
	Consume(count int, blockTime time.Duration) error
	AutoClaim(count int, minIdle time.Duration) error
}

// FlushAsync is like Flush() but instead of executing SQL directly in MySQL,
// it publishes the SQL queries to a Redis Stream. Redis cache and search indexes
// are updated immediately (optimistic update). Call GetAsyncSQLConsumer() to
// process the queued SQL operations.
func (orm *ormImplementation) FlushAsync() error {
	orm.mutexFlush.Lock()
	defer orm.mutexFlush.Unlock()
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
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
		return flushErr
	}

	// Step 2: serialize each non-empty DatabasePipeline into AsyncSQLOperation events
	broker := orm.GetEventBroker()
	eventFlusher := broker.NewFlusher()
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
		op := AsyncSQLOperation{Pool: pool, Queries: queries}
		if err := eventFlusher.Publish(AsyncSQLStreamName, op); err != nil {
			return err
		}
	}

	// Step 3: clear DB pipelines — queries will go to the stream, not MySQL directly
	orm.dbPipeLines = nil

	// Step 4: execute Redis pipelines (cache + search indexes updated immediately)
	for _, redisPipeline := range orm.redisPipeLines {
		if _, err := redisPipeline.Exec(orm); err != nil {
			return err
		}
	}

	// Step 5: publish the serialized SQL events to the async stream
	if err := eventFlusher.Flush(); err != nil {
		return err
	}

	// Step 6: mark entities as flushed and clear tracked set
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
