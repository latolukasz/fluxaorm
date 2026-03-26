package fluxaorm

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisSearchQueryEmpty(t *testing.T) {
	q := NewRedisSearchQuery()
	assert.Equal(t, "*", q.BuildQueryString())
}

func TestRedisSearchQueryWithConditions(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	tag := RedisSearchTagField{Column: "Status"}

	q := NewRedisSearchQuery().Filter(age.Gte(18), tag.In("active"))
	assert.Equal(t, "@Age:[18 +inf] @Status:{active}", q.BuildQueryString())
}

func TestRedisSearchQueryMultipleFilterCalls(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	name := RedisSearchTextField{Column: "Name"}

	q := NewRedisSearchQuery().
		Filter(age.Gte(18)).
		Filter(name.Match("John"))
	assert.Equal(t, "@Age:[18 +inf] @Name:John", q.BuildQueryString())
}

func TestRedisSearchQueryBuildSearchOptionsNoSort(t *testing.T) {
	q := NewRedisSearchQuery()
	opts := q.BuildSearchOptions(0, 10)

	assert.True(t, opts.NoContent)
	assert.Equal(t, 0, opts.LimitOffset)
	assert.Equal(t, 10, opts.Limit)
	assert.Nil(t, opts.SortBy)
}

func TestRedisSearchQueryBuildSearchOptionsSortASC(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	q := NewRedisSearchQuery().SortByASC(age)
	opts := q.BuildSearchOptions(0, 10)

	assert.Equal(t, []redis.FTSearchSortBy{{FieldName: "Age", Asc: true}}, opts.SortBy)
}

func TestRedisSearchQueryBuildSearchOptionsSortDESC(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	q := NewRedisSearchQuery().SortByDESC(age)
	opts := q.BuildSearchOptions(5, 20)

	assert.Equal(t, []redis.FTSearchSortBy{{FieldName: "Age", Desc: true}}, opts.SortBy)
	assert.Equal(t, 5, opts.LimitOffset)
	assert.Equal(t, 20, opts.Limit)
}

func TestRedisSearchQuerySortDESCOverridesASC(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	name := RedisSearchNumericField{Column: "Name"}

	q := NewRedisSearchQuery().SortByASC(age).SortByDESC(name)
	opts := q.BuildSearchOptions(0, 10)

	assert.Equal(t, []redis.FTSearchSortBy{{FieldName: "Name", Desc: true}}, opts.SortBy)
}

func TestRedisSearchQueryPager(t *testing.T) {
	p := NewPager(1, 10)
	q := NewRedisSearchQuery().Pager(p)
	assert.NotNil(t, q.pager)
	assert.Equal(t, 10, q.pager.PageSize)
}

func TestRedisSearchQueryChaining(t *testing.T) {
	age := RedisSearchNumericField{Column: "Age"}
	tag := RedisSearchTagField{Column: "Status"}
	p := NewPager(1, 10)

	q := NewRedisSearchQuery().
		Filter(age.Between(18, 65)).
		Filter(tag.In("active", "pending")).
		SortByDESC(age).
		Pager(p)

	assert.Equal(t, "@Age:[18 65] @Status:{active|pending}", q.BuildQueryString())

	opts := q.BuildSearchOptions(0, 10)
	assert.Equal(t, []redis.FTSearchSortBy{{FieldName: "Age", Desc: true}}, opts.SortBy)
}
