package fluxaorm

import (
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisSearchQuery is a type-safe query builder for Redis FT.SEARCH queries.
type RedisSearchQuery struct {
	pager      *Pager
	sortField  string
	sortAsc    bool
	conditions []RedisSearchCondition
}

// NewRedisSearchQuery creates a new empty RedisSearchQuery.
func NewRedisSearchQuery() *RedisSearchQuery {
	return &RedisSearchQuery{}
}

// Pager sets the pagination for the query.
func (q *RedisSearchQuery) Pager(p *Pager) *RedisSearchQuery {
	q.pager = p
	return q
}

// SortByASC sets ascending sort on the given field.
func (q *RedisSearchQuery) SortByASC(f RedisSearchField) *RedisSearchQuery {
	q.sortField = f.ColumnName()
	q.sortAsc = true
	return q
}

// SortByDESC sets descending sort on the given field.
func (q *RedisSearchQuery) SortByDESC(f RedisSearchField) *RedisSearchQuery {
	q.sortField = f.ColumnName()
	q.sortAsc = false
	return q
}

// Filter adds conditions to the query.
func (q *RedisSearchQuery) Filter(conditions ...RedisSearchCondition) *RedisSearchQuery {
	q.conditions = append(q.conditions, conditions...)
	return q
}

// GetPagerOffsetCount returns the offset and count for pagination.
// If no pager is set, returns offset=0 and count=10000.
func (q *RedisSearchQuery) GetPagerOffsetCount() (offset int, count int) {
	if q.pager == nil {
		return 0, 10000
	}
	return (q.pager.CurrentPage - 1) * q.pager.PageSize, q.pager.PageSize
}

// BuildQueryString returns the FT.SEARCH query string. Returns "*" if no conditions.
func (q *RedisSearchQuery) BuildQueryString() string {
	if len(q.conditions) == 0 {
		return "*"
	}
	parts := make([]string, len(q.conditions))
	for i, c := range q.conditions {
		parts[i] = c.ToFTSearch()
	}
	return strings.Join(parts, " ")
}

// BuildSearchOptions returns FTSearchOptions for go-redis.
func (q *RedisSearchQuery) BuildSearchOptions(offset, count int) *redis.FTSearchOptions {
	opts := &redis.FTSearchOptions{
		NoContent:   true,
		LimitOffset: offset,
		Limit:       count,
	}
	if q.sortField != "" {
		if q.sortAsc {
			opts.SortBy = []redis.FTSearchSortBy{{FieldName: q.sortField, Asc: true}}
		} else {
			opts.SortBy = []redis.FTSearchSortBy{{FieldName: q.sortField, Desc: true}}
		}
	}
	return opts
}
