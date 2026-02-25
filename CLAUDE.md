# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # Run all tests (go test -race -p 1 ./...)
make check         # Run linting (revive, gocyclo, format check)
make format        # Format code with goimports
make cover         # Run tests with coverage → resources/cover/cover.out
make cover-html    # Generate and open HTML coverage report
make tidy          # Run go mod tidy
```

To run a single test:
```bash
go test -race -p 1 -run TestName ./...
```

Tests require running MySQL and Redis services. See `docker/` for setup.

Linting is configured in `revive.toml`. Cyclomatic complexity threshold is 100.

## Architecture

FLUXA ORM is a Go ORM targeting MySQL + Redis 8.0 with Redis Search. The main package is everything at the root level.

### Core Flow

1. **Registry** (`registry.go`) — configure entity types, connection pools (MySQL, Redis, Local Cache), and plugins
2. **Engine** (`engine.go`) — runtime object created from Registry; holds connection pools
3. **Context** (`orm.go`) — created from Engine per-request; the main API surface for all ORM operations

### Entity Lifecycle

- **EntitySchema** (`entity_schema.go`) — defines table structure, fields, indexes, caching strategy, references, and enums per entity type. Schema is registered on Registry, validated and stored in Engine.
- **Flush** (`flush.go`) — batches entity inserts/updates via dirty tracking. Supports sync and async modes.

### Caching (Three Tiers)

1. **Context cache** — per-request entity cache in `ormImplementation`; disabled with `DisableContextCache()`
2. **Local cache** (`local_cache.go`) — per-process LRU cache; size configured per entity
3. **Redis cache** (`redis_cache.go`) — full Redis 8.0 API including Streams, Search, Sorted Sets, Hashes, etc.; pipelining via `redis_pipeline.go`

### Code Generation

`generate.go` — generates fully-typed, zero-reflection Go code for each registered entity.

**Design goals:** developer-friendly API + maximum performance (minimal DB/Redis queries, minimal memory allocations, no reflection at runtime).

#### Generated output structure (per entity)

- **`XxxSQLRow` struct** — flat struct with fields `F0`, `F1`, `F2`... mapped 1:1 to DB columns; used for direct `Scan()` without reflection.
- **`xxxProvider` / `XxxProvider` singleton** — holds static metadata (tableName, dbCode, redisCode, cacheIndex, redisCachePrefix, stamp, TTL). Exposes all query methods.
- **`XxxEntity` struct** — the user-facing entity; holds `ctx`, `id`, `new`, `deleted`, `originDatabaseValues` (SQLRow), `databaseBind` (lazy map of changed columns), and optionally `redisBind` + `originRedisValues` for entities with Redis cache.

#### Caching strategy in generated code

- Redis cache stores entity as a **Redis List** (`RPush`). Index 0 = struct hash stamp; indices 1..N = field values serialised to strings (ints as decimal, floats as `f8`, booleans as `"1"`/`"0"`, times as Unix seconds, NULL as `""`).
- `GetByID`: checks Redis List first → validates stamp → returns entity backed by `originRedisValues`; on cache miss queries MySQL and caches result; marks not-found with empty marker list `[""]`.
- `GetByIDs`: batch MySQL `IN (...)` query, no Redis cache.

#### Dirty tracking in generated code

- Getters read from (in priority order): `databaseBind` (pending change) → `originRedisValues` (Redis cache) → `originDatabaseValues` (MySQL row).
- Setters compare new value against current origin; if unchanged they `delete()` from the bind maps (idempotent/no-op); if changed they call `addToDatabaseBind` + `addToRedisBind`.
- `PrivateFlush()` builds and enqueues INSERT/UPDATE/DELETE on the DB pipeline and LSet changes on the Redis pipeline — both via pipelining to minimise round-trips.

#### UUID generation in generated code

- Uses a Redis counter (`INCR` on a per-entity key).
- On first use (counter == 1) acquires a distributed lock and initialises the counter from `MAX(ID)` in MySQL (`initUUID`).

#### Field types supported by the generator

`uint64`, `int64`, `bool`, `float64` (with precision), `time.Time` (datetime or date-only), `string`, `[]uint8` (byte), enums (typed string aliases), sets (`[]EnumType`, stored comma-separated sorted), nullable variants of all the above, and `fluxaorm.Reference[T]` (required and optional).

#### Generated provider methods

- `GetByID` — checks Redis cache (if configured) → falls back to MySQL; marks not-found with empty marker
- `GetByIDs` — batch MySQL `IN (...)` query; for cached entities checks Redis pipeline first
- `New` / `NewWithID` — allocates entity and tracks it; UUID from Redis `INCR`
- `PrivateFlush` / `PrivateFlushed` — enqueues INSERT/UPDATE/DELETE on DB pipeline; clears dirty state after commit
- `Delete` / `ForceDelete` — soft-delete (sets `FakeDelete`) or hard-delete
- Typed getters & setters for all field types
- `GetByIndex<Name>` — unique index lookup (stub, returns nil)
- `Search` — `SELECT all columns WHERE … [LIMIT …]`; FakeDelete-aware
- `SearchWithCount` — `SELECT COUNT(*) + SELECT all columns WHERE … [LIMIT …]`; FakeDelete-aware
- `SearchOne` — `SELECT all columns WHERE … LIMIT 1`; FakeDelete-aware
- `SearchIDs` — `SELECT ID WHERE … [LIMIT …]`; FakeDelete-aware
- `SearchIDsWithCount` — `SELECT COUNT(*) + SELECT ID WHERE … LIMIT …`; FakeDelete-aware

See `test_generate/` for entity definitions used to test generation and `test_generate/entities/` for example generated output.

### Event System

`event_broker.go` — Redis Stream-based pub/sub for entity change events. Consumers use consumer groups for reliable delivery.

### Key Supporting Files

- `db.go` — MySQL abstraction (`DB` interface, `DBTransaction`, metrics)
- `schema.go` — DDL operations (CREATE/ALTER TABLE, index management)
- `locker.go` — distributed locking via `bsm/redislock`
- `metrics.go` — Prometheus metrics for queries, cache hits/misses
- `query_logger.go` — configurable query logging with handlers
- `test.go` — test utilities (PrepareTables, mock structures)

### Concurrency

Uses `puzpuzpuz/xsync/v2` concurrent maps for thread-safe metadata storage in Engine and EntitySchema. Tests run with `-race` flag.
