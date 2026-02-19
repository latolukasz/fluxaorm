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

Tests require running MySQL and Redis (Dragonfly) services. See `docker/` for setup.

Linting is configured in `revive.toml`. Cyclomatic complexity threshold is 100.

## Architecture

FLUXA ORM is a Go ORM targeting MySQL + Redis 8.0 with Redis Search. The main package is everything at the root level.

### Core Flow

1. **Registry** (`registry.go`) — configure entity types, connection pools (MySQL, Redis, Local Cache), and plugins
2. **Engine** (`engine.go`) — runtime object created from Registry; holds connection pools
3. **Context** (`orm.go`) — created from Engine per-request; the main API surface for all ORM operations

### Entity Lifecycle

- **EntitySchema** (`entity_schema.go`) — the largest file (75KB); defines table structure, fields, indexes, caching strategy, references, and enums per entity type. Schema is registered on Registry, validated and stored in Engine.
- **Flush** (`flush.go`) — batches entity inserts/updates; uses dirty tracking to detect changes. Supports sync and async modes.
- **Bind** (`bind.go`, 43KB) — maps MySQL column values to Go struct fields; handles all type conversions.
- **Column setters** (`column_setter.go`) and **field editors** (`edit_entity_field.go`) — typed setters for entity fields.

### Caching (Three Tiers)

1. **Context cache** — per-request entity cache in `ormImplementation`; disabled with `DisableContextCache()`
2. **Local cache** (`local_cache.go`) — per-process LRU cache; size configured per entity
3. **Redis cache** (`redis_cache.go`, 51KB) — full Redis 8.0 API including Streams, Search, Sorted Sets, Hashes, etc.; pipelining via `redis_pipeline.go`

### Search & Retrieval

- `get_by_id.go`, `get_by_ids.go` — primary key lookups (cache-aware)
- `get_by_index.go`, `get_by_unique_index.go` — index-based lookups
- `search.go` — SQL `WHERE` clause builder using `Where` type from `where.go`
- `search_by_redis_index.go` — Redis Search index queries
- `pager.go` — pagination support for search results

### Code Generation

`generate.go` (83KB) — generates typed provider code for registered entities. Run after modifying entity definitions. Output goes to a configurable directory. See `test_generate/` for examples of generated code and entities.

### Event System

`event_broker.go` + `dirty_stream_consumer.go`/`dirty_stream_push.go` — Redis Stream-based pub/sub for entity change events. Consumers use consumer groups for reliable delivery.

### Plugin System

`plugin.go` defines the plugin interface for entity lifecycle hooks. See `plugins/modified/` for an example (auto-timestamps on update).

### Key Supporting Files

- `db.go` — MySQL abstraction (`DB` interface, `DBTransaction`, metrics)
- `schema.go` — DDL operations (CREATE/ALTER TABLE, index management)
- `locker.go` — distributed locking via `bsm/redislock`
- `metrics.go` — Prometheus metrics for queries, cache hits/misses
- `log_table.go` — audit log table support
- `query_logger.go` — configurable query logging with handlers
- `test.go` — test utilities (PrepareTables, mock structures)

### Concurrency

Uses `puzpuzpuz/xsync/v2` concurrent maps for thread-safe metadata storage in Engine and EntitySchema. Tests run with `-race` flag.
