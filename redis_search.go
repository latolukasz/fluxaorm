package fluxaorm

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisSearchWhere builds FT.SEARCH query strings.
type RedisSearchWhere struct {
	parts   []string
	sortBy  string
	sortAsc bool
}

// NewRedisSearchWhere creates a new empty where builder.
func NewRedisSearchWhere() *RedisSearchWhere {
	return &RedisSearchWhere{}
}

// NumericRange adds @field:[min max] condition.
func (w *RedisSearchWhere) NumericRange(field string, min, max float64) *RedisSearchWhere {
	w.parts = append(w.parts, fmt.Sprintf("@%s:[%g %g]", field, min, max))
	return w
}

// NumericMin adds @field:[min +inf] condition.
func (w *RedisSearchWhere) NumericMin(field string, min float64) *RedisSearchWhere {
	w.parts = append(w.parts, fmt.Sprintf("@%s:[%g +inf]", field, min))
	return w
}

// NumericMax adds @field:[-inf max] condition.
func (w *RedisSearchWhere) NumericMax(field string, max float64) *RedisSearchWhere {
	w.parts = append(w.parts, fmt.Sprintf("@%s:[-inf %g]", field, max))
	return w
}

// NumericEqual adds @field:[val val] (exact match) condition.
func (w *RedisSearchWhere) NumericEqual(field string, value float64) *RedisSearchWhere {
	w.parts = append(w.parts, fmt.Sprintf("@%s:[%g %g]", field, value, value))
	return w
}

// Tag adds @field:{v1|v2} condition.
func (w *RedisSearchWhere) Tag(field string, values ...string) *RedisSearchWhere {
	escaped := make([]string, len(values))
	for i, v := range values {
		escaped[i] = strings.ReplaceAll(v, "|", "\\|")
	}
	w.parts = append(w.parts, fmt.Sprintf("@%s:{%s}", field, strings.Join(escaped, "|")))
	return w
}

// Text adds @field:text condition (full-text search).
func (w *RedisSearchWhere) Text(field string, text string) *RedisSearchWhere {
	w.parts = append(w.parts, fmt.Sprintf("@%s:%s", field, text))
	return w
}

// Bool adds @field:[1 1] or @field:[0 0] condition.
func (w *RedisSearchWhere) Bool(field string, value bool) *RedisSearchWhere {
	if value {
		w.parts = append(w.parts, fmt.Sprintf("@%s:[1 1]", field))
	} else {
		w.parts = append(w.parts, fmt.Sprintf("@%s:[0 0]", field))
	}
	return w
}

// SortBy sets the sort field and direction (not part of the query string).
func (w *RedisSearchWhere) SortBy(field string, ascending bool) *RedisSearchWhere {
	w.sortBy = field
	w.sortAsc = ascending
	return w
}

// String returns the assembled query (space-separated = AND). Returns "*" if empty.
func (w *RedisSearchWhere) String() string {
	if w == nil || len(w.parts) == 0 {
		return "*"
	}
	return strings.Join(w.parts, " ")
}

// GetSearchOptions returns FTSearchOptions with NOCONTENT, LIMIT offset/count, and optional SORTBY.
func (w *RedisSearchWhere) GetSearchOptions(offset, count int) *redis.FTSearchOptions {
	opts := &redis.FTSearchOptions{
		NoContent:   true,
		LimitOffset: offset,
		Limit:       count,
	}
	if w != nil && w.sortBy != "" {
		opts.SortBy = []redis.FTSearchSortBy{{FieldName: w.sortBy, Asc: w.sortAsc}}
	}
	return opts
}

// RedisSearchAlter holds a pending FT.CREATE operation.
type RedisSearchAlter struct {
	IndexName string
	RedisPool string
	options   *redis.FTCreateOptions
	schema    []*redis.FieldSchema
}

// Exec executes the FT.CREATE command.
func (a RedisSearchAlter) Exec(ctx Context) error {
	return ctx.Engine().Redis(a.RedisPool).FTCreate(ctx, a.IndexName, a.options, a.schema...)
}

// GetRedisSearchAlters returns pending FT.CREATE operations for all entities with Redis Search enabled.
// Only indexes that do not yet exist are returned. Old indexes with a different hash are not dropped.
func GetRedisSearchAlters(ctx Context) ([]RedisSearchAlter, error) {
	poolSchemas := make(map[string][]*entitySchema)
	for _, schema := range ctx.Engine().Registry().(*engineRegistryImplementation).entitySchemas {
		if schema.hasRedisSearch {
			poolSchemas[schema.redisSearchPoolCode] = append(poolSchemas[schema.redisSearchPoolCode], schema)
		}
	}

	var alters []RedisSearchAlter
	for poolCode, schemas := range poolSchemas {
		existingIndexes, err := ctx.Engine().Redis(poolCode).FTList(ctx)
		if err != nil {
			return nil, err
		}
		existingSet := make(map[string]bool, len(existingIndexes))
		for _, idx := range existingIndexes {
			existingSet[idx] = true
		}

		for _, schema := range schemas {
			if existingSet[schema.redisSearchIndex] {
				continue
			}
			opts := &redis.FTCreateOptions{
				OnHash: true,
				Prefix: []interface{}{schema.redisSearchPrefix},
			}
			fieldSchemas := make([]*redis.FieldSchema, len(schema.searchableFields))
			for i, f := range schema.searchableFields {
				fs := &redis.FieldSchema{
					FieldName: f.columnName,
					FieldType: redisSearchFieldType(f.redisType),
				}
				if f.sortable {
					fs.Sortable = true
				}
				fieldSchemas[i] = fs
			}
			alters = append(alters, RedisSearchAlter{
				IndexName: schema.redisSearchIndex,
				RedisPool: poolCode,
				options:   opts,
				schema:    fieldSchemas,
			})
		}
	}
	return alters, nil
}

func redisSearchFieldType(t string) redis.SearchFieldType {
	switch t {
	case "NUMERIC":
		return redis.SearchFieldTypeNumeric
	case "TAG":
		return redis.SearchFieldTypeTag
	default:
		return redis.SearchFieldTypeText
	}
}
