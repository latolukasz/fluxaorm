# Plan: FlushAsync() Implementation

## Context

Currently `Flush()` writes SQL changes (INSERT/UPDATE/DELETE) synchronously to MySQL, then updates Redis cache/search indexes. The goal is a `FlushAsync()` method that:
- Publishes SQL queries to a Redis Stream instead of executing them immediately
- Updates Redis cache and search indexes immediately (optimistic update)
- Provides a consumer helper that reads events from the stream, executes SQL, and handles errors automatically — permanent MySQL errors go to a dead-letter stream, transient errors stop processing and return an error

---

## Files to Modify / Create

| File | Action |
|------|--------|
| `orm.go` | Add `FlushAsync()` and `GetAsyncSQLConsumer()` to `Context` interface |
| `registry.go` | Add `RegisterAsyncSQLStream()` to `Registry` interface + implementation; auto-register in `Validate()` |
| `flush_async.go` | **New** – all async SQL logic |

---

## Detailed Design

### 1. Constants and Stream Names (`flush_async.go`)

```go
const (
    AsyncSQLStreamName          = "_fluxa_async_sql"
    AsyncSQLDeadLetterStreamName = "_fluxa_async_sql_failed"
)
```

Both streams live on the same Redis pool (configured via `RegisterAsyncSQLStream`).

---

### 2. SQL Parameter Serialization (`flush_async.go`)

SQL parameters contain `uint64`, `int64`, `float64`, `bool`, `string`, `time.Time`, `sql.NullInt64`, `sql.NullFloat64`, `sql.NullBool`, `sql.NullString`, `sql.NullTime`. Msgpack cannot round-trip these as `[]any` reliably (struct types like `sql.NullInt64` become maps on deserialization).

**Solution**: typed parameter envelope that serializes each param as a string + type tag:

```go
type asyncSQLParam struct {
    Null bool   `msgpack:"n,omitempty"`
    Type string `msgpack:"t,omitempty"` // "s","i","u","f","b","t"
    Val  string `msgpack:"v,omitempty"`
}
```

`convertParam(v any) asyncSQLParam` converts from any Go SQL param to this struct.
`deconvertParam(p asyncSQLParam) any` converts back to a type MySQL driver accepts (`nil`, `string`, `int64`, `uint64`, `float64`, `bool`, `time.Time`).

Type codes:
- `"s"` → string
- `"i"` → int64
- `"u"` → uint64
- `"f"` → float64
- `"b"` → bool (`"1"`/`"0"`)
- `"t"` → time.Time (RFC3339 format)
- `Null: true` → nil (SQL NULL) — covers all `sql.Null*` types when `Valid=false`

---

### 3. Event Struct (`flush_async.go`)

One event per DB pipeline (= one event per DB pool per flush, maintaining transaction semantics):

```go
type asyncSQLQuery struct {
    Q string          `msgpack:"q"` // SQL template with ? placeholders
    P []asyncSQLParam `msgpack:"p"` // typed parameters
}

type asyncSQLOperation struct {
    Pool    string         `msgpack:"pool"`
    Queries []asyncSQLQuery `msgpack:"queries"`
}
```

---

### 4. `FlushAsync()` (`flush_async.go`, added to Context interface in `orm.go`)

Steps (mirrors `flush()` in `flush.go`):

1. Lock `mutexFlush`, return early if nothing tracked
2. Call `PrivateFlush()` on all tracked entities → populates `dbPipeLines` and `redisPipeLines`
3. **Before executing anything**, prepare `eventFlusher` and serialize each non-empty `DatabasePipeline` into an `asyncSQLOperation` event (do NOT call `dbPipeline.Exec()`)
4. Reset `dbPipeLines = nil` (queries will not go to MySQL directly)
5. Execute `redisPipeline.Exec()` for each pool (cache + search updates happen immediately)
6. Call `eventFlusher.Flush()` to publish SQL events to `AsyncSQLStreamName`
7. Call `PrivateFlushed()` on all entities, clear `trackedEntities`

**Ordering rationale**: Redis pipeline runs before stream publish. If stream publish fails, Redis is already updated — this is acceptable because a stale cache is less harmful than having MySQL updated but cache stale. On error in step 5 or 6, tracked entities remain (not flushed); user can retry.

---

### 5. Registry Changes (`registry.go`)

Add to `Registry` interface:
```go
RegisterAsyncSQLStream(redisPool string)
```

Implementation: calls `RegisterRedisStream` for both stream names on the given pool.

In `Validate()`, after building entity schemas (around line 192), add auto-registration:
```go
if _, hasAsyncSQL := r.redisStreamPools[AsyncSQLStreamName]; !hasAsyncSQL {
    if _, hasDefault := r.redisPools[DefaultPoolCode]; hasDefault {
        r.RegisterRedisStream(AsyncSQLStreamName, DefaultPoolCode)
        r.RegisterRedisStream(AsyncSQLDeadLetterStreamName, DefaultPoolCode)
    }
}
```

This means `FlushAsync()` works out of the box if a default Redis pool is registered. Call `RegisterAsyncSQLStream("my-pool")` before `Validate()` to use a different pool.

---

### 6. `AsyncSQLConsumer` Interface and Implementation (`flush_async.go`)

```go
type AsyncSQLConsumer interface {
    Consume(count int, blockTime time.Duration) error
    AutoClaim(count int, minIdle time.Duration) error
}
```

Added to Context interface (`orm.go`):
```go
GetAsyncSQLConsumer() (AsyncSQLConsumer, error)
```

`GetAsyncSQLConsumer()` implementation:
- Gets `EventBroker` from current context
- Calls `broker.ConsumerSingle(ctx, AsyncSQLStreamName)` → gets `EventsConsumer`
- Returns `asyncSQLConsumerImpl{inner: consumer, ctx: orm}`

`asyncSQLConsumerImpl`:
```go
type asyncSQLConsumerImpl struct {
    inner  EventsConsumer
    ctx    *ormImplementation
}
```

`Consume()` delegates to `inner.Consume(count, blockTime, c.handleEvents)`.
`AutoClaim()` delegates to `inner.AutoClaim(count, minIdle, c.handleEvents)`.

---

### 7. Event Handler Logic (`flush_async.go`)

`handleEvents(events []Event) error`:

For each event:
1. `ev.Unserialize(&op)` — deserialize `asyncSQLOperation`
   - On failure (bad format): publish to dead-letter, `ev.Ack()`, continue
2. `executeOperation(op)` — run SQL via DB pipeline
   - **Success**: `ev.Ack()`, continue
   - **Permanent MySQL error**: publish to dead-letter, `ev.Ack()`, continue
   - **Transient error**: return error immediately (unprocessed events remain in pending; consumer can retry)

**Permanent MySQL error detection** (`isPermanentMySQLError`):
```go
import "github.com/go-sql-driver/mysql"

var mysqlErr *mysql.MySQLError
if !errors.As(err, &mysqlErr) {
    return false // not a MySQL error → transient
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
```

**Dead-letter publish**: Uses EventBroker with original `asyncSQLOperation` as body and `"error", err.Error()` as metadata tags. The dead-letter event has the same structure, queryable later.

**`executeOperation(op asyncSQLOperation)`**:
- Gets `db := c.ctx.engine.DB(op.Pool)`
- If single query: `db.Exec(c.ctx, q.Q, deconvertedParams...)`
- If multiple queries: begin transaction, exec each query in order, commit. Any error rolls back and is returned (checked for permanent/transient by caller).

---

## Verification

```bash
# Run all tests
make test

# Run single test (once test is written)
go test -race -p 1 -run TestFlushAsync ./...
```

Test scenario to cover:
1. Create entities, call `FlushAsync()` → entities NOT in MySQL, ARE in Redis cache, SQL events in stream
2. `GetAsyncSQLConsumer().Consume(...)` → entities appear in MySQL, events removed from stream
3. Insert entity with duplicate key → event moves to dead-letter stream, original consumed
4. Simulate connection error → consumer returns error, event remains in pending
5. `AutoClaim()` reclaims abandoned events and retries

Key test files to reference: `event_broker_test.go` (for consumer setup pattern), `test_generate/generate_test.go` (for entity setup pattern).
