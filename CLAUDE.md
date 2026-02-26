# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # Run all tests (go test -race -p 1 ./...)
make check         # Run linting (revive, gocyclo, format check, no fmt.Print*/spew.Dump)
make format        # Format code with goimports
make cover         # Run tests with coverage → resources/cover/cover.out
make cover-html    # Generate and open HTML coverage report
make tidy          # Run go mod tidy
```

To run a single test:
```bash
go test -race -p 1 -run TestName ./...
```

Tests require running MySQL and Redis services. Use `docker/docker-compose.yml` for setup (requires `LOCAL_IP`, `MYSQL_PORT`, `REDIS_PORT` env vars). MySQL 8.0 on port 3306 and Redis 8.2.2 on port 6379.

Linting is configured in `revive.toml`. Cyclomatic complexity threshold is 100. `fmt.Print*` and `spew.Dump` calls are banned by `make check`.

## Architecture

FLUXA ORM is a **code-generation-based** Go ORM targeting MySQL + Redis 8.0. The main package is everything at the root level (`github.com/latolukasz/fluxaorm/v2`).

### Core Flow

1. **Registry** (`registry.go`) — configure entity types, connection pools (MySQL, Redis, Local Cache), and plugins via `NewRegistry()`
2. **Engine** (`engine.go`) — immutable runtime object created via `registry.Validate()`; holds connection pools and all entity schemas
3. **Context** (`orm.go`) — created from Engine per-request (`Context` interface / `ormImplementation`); the main API surface for ORM operations

### Entity Definition

Entities are plain Go structs with an `ID` field and optional `orm:` struct tags. Field types and caching are declared on the struct; schema metadata is extracted at `Validate()` time via reflection.

Key struct tags:
- `orm:"redisCache"` — enable Redis List cache for this entity
- `orm:"required"` — NOT NULL / required field
- `orm:"unique=IndexName"` / `orm:"unique=IndexName:2"` — composite unique index (colon suffix = column order)
- `orm:"enum=a,b,c"` / `orm:"set=a,b,c"` — MySQL ENUM/SET column
- `orm:"enumName=TypeName"` — share an enum type across fields
- `orm:"time"` — store as DATETIME (default is DATE for `time.Time` fields)
- `orm:"redisSearch=poolName"` — enable Redis Search indexing for this entity (on ID field only; uses `DefaultPoolCode` if omitted)
- `orm:"searchable"` — include field in Redis Search index
- `orm:"sortable"` — add SORTABLE flag to Redis Search index field (requires `searchable`)

Entity registration: `registry.RegisterEntity(&MyEntity{})`, then call `registry.Validate()`.

### Code Generation

`Generate(engine Engine, outputDirectory string)` produces one `.go` file per entity in the given directory. The generator is split across:

- `generate.go` — entry point, file I/O, `codeGenerator` struct
- `generate_entity.go` — `XxxEntity` struct body
- `generate_entity_struct.go` — entity struct scaffolding
- `generate_fields.go` — per-field SQL row and Redis serialisation helpers
- `generate_getters.go` / `generate_getters_nullable.go` — typed getters & setters with dirty tracking
- `generate_provider.go` — `XxxProvider` singleton, `XxxSQLRow`, `redisValues()`
- `generate_query.go` — `GetByID`, `GetByIDs`, `Search*`, `SearchIDs*`, `New`, `Delete`, etc.

**Generated output structure (per entity):**

- **`XxxSQLRow` struct** — flat struct with fields `F0`, `F1`, `F2`... for reflection-free `Scan()`
- **`XxxProvider` singleton** — holds static metadata (tableName, dbCode, redisCode, cacheIndex, redisCachePrefix, stamp, TTL) and exposes all query methods
- **`XxxEntity` struct** — user-facing entity; holds `ctx`, `id`, `new`, `deleted`, `originDatabaseValues` (SQLRow), lazy `databaseBind` / `redisBind` maps for dirty tracking

### Caching (Three Tiers)

1. **Context cache** — per-request in-memory map inside `ormImplementation`; **enabled by default** with a 1-second TTL. Populated only by `GetByID` and `GetByIDs` (not by `Search*`). Disable with `ctx.DisableContextCache()`; change TTL with `ctx.SetContextCacheTTL(d time.Duration)`. When the TTL expires, the entire map is cleared on the next cache read. Low-level hooks `GetFromContextCache` / `SetInContextCache` are exported on the `Context` interface so generated code can use them across packages.
2. **Local cache** (`local_cache.go`) — per-process LRU; size configured per entity via `registry.RegisterLocalCache(code, limit)`
3. **Redis cache** (`redis_cache.go`) — entity stored as a Redis List; index 0 = struct hash stamp, indices 1..N = serialised field values

### Dirty Tracking in Generated Code

- Getters read from (priority): `databaseBind` → `originRedisValues` → `originDatabaseValues`
- Setters compare new value against current origin; no-op if unchanged; otherwise update bind maps
- `PrivateFlush()` enqueues INSERT/UPDATE/DELETE on the DB pipeline and `LSet` on the Redis pipeline

### UUID Generation

Uses Redis `INCR` on a per-entity key. On first use (counter == 1), acquires a distributed lock and initialises the counter from `MAX(ID)` in MySQL (`initUUID`).

### Redis Search

Entities opt in to Redis Search (FT.SEARCH) indexing via struct tags on the ID field and individual fields. The ORM manages index lifecycle and hash document writes automatically.

- `redis_search.go` — `RedisSearchWhere` query builder, `RedisSearchAlter`, `GetRedisSearchAlters(ctx)` (returns pending FT.CREATE operations, similar to `GetAlters`)
- Index name = `<tableName>_<8-char FNV hash of field definitions>` — index is recreated when schema changes
- Hash key prefix = `<5-char FNV hash of "<tableName>:search">:h:` + entity ID
- `PrivateFlush()` maintains Redis hashes: Del+HSet on INSERT/UPDATE, Del on DELETE; soft-deleted entities are removed from the index
- Generated query methods: `SearchInRedis`, `SearchOneInRedis`, `SearchInRedisWithCount`, `SearchIDsInRedis`, `SearchIDsInRedisWithCount`
- Type mapping: numeric types + bool + time.Time + Reference → NUMERIC; string → TEXT; enum/set → TAG

### Key Supporting Files

- `entity_schema.go` — `entitySchema` struct; all per-entity metadata (columns, indexes, caching, enums, references, struct hash)
- `flush.go` — batches entity inserts/updates via dirty tracking; `ctx.Flush()`
- `db.go` — MySQL abstraction (`DB` interface, `DBTransaction`, metrics)
- `schema.go` — DDL operations (CREATE/ALTER TABLE, index management)
- `event_broker.go` — Redis Stream-based pub/sub for entity change events
- `locker.go` — distributed locking via `bsm/redislock`
- `metrics.go` — Prometheus metrics for queries, cache hits/misses
- `where.go` — typed WHERE clause builder
- `test.go` — test utilities (`PrepareTablesBeta`, mock structures)

### Test Fixtures

`test_generate/` contains:
- `generate_test.go` — entity struct definitions used as generator input + `TestGenerate` which calls `Generate()` and then exercises the output
- `entities/` — committed generated output (must be kept up-to-date by re-running `TestGenerate`)
- `entities/enums/` — generated enum types

### Concurrency

Uses `puzpuzpuz/xsync/v2` concurrent maps for thread-safe metadata in Engine and EntitySchema. All tests run with `-race`.
