package fluxaorm

import (
	"fmt"
	"strconv"
	"strings"
)

// RedisSearchCondition represents a single FT.SEARCH predicate.
type RedisSearchCondition interface {
	ToFTSearch() string
}

// RedisSearchField is implemented by all Redis Search field types.
type RedisSearchField interface {
	ColumnName() string
}

type rsNumericRangeCondition struct {
	column string
	min    string
	max    string
}

func (c rsNumericRangeCondition) ToFTSearch() string {
	return fmt.Sprintf("@%s:[%s %s]", c.column, c.min, c.max)
}

type rsTagCondition struct {
	column string
	values []string
}

func (c rsTagCondition) ToFTSearch() string {
	escaped := make([]string, len(c.values))
	for i, v := range c.values {
		escaped[i] = strings.ReplaceAll(v, "|", "\\|")
	}
	return fmt.Sprintf("@%s:{%s}", c.column, strings.Join(escaped, "|"))
}

type rsTextCondition struct {
	column string
	text   string
}

func (c rsTextCondition) ToFTSearch() string {
	return fmt.Sprintf("@%s:%s", c.column, c.text)
}

// RedisSearchNumericField represents a numeric field in a Redis Search index.
type RedisSearchNumericField struct{ Column string }

func (f RedisSearchNumericField) ColumnName() string { return f.Column }

func (f RedisSearchNumericField) Eq(v float64) RedisSearchCondition {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	return rsNumericRangeCondition{column: f.Column, min: s, max: s}
}

func (f RedisSearchNumericField) Gte(v float64) RedisSearchCondition {
	return rsNumericRangeCondition{column: f.Column, min: strconv.FormatFloat(v, 'f', -1, 64), max: "+inf"}
}

func (f RedisSearchNumericField) Lte(v float64) RedisSearchCondition {
	return rsNumericRangeCondition{column: f.Column, min: "-inf", max: strconv.FormatFloat(v, 'f', -1, 64)}
}

func (f RedisSearchNumericField) Gt(v float64) RedisSearchCondition {
	return rsNumericRangeCondition{column: f.Column, min: "(" + strconv.FormatFloat(v, 'f', -1, 64), max: "+inf"}
}

func (f RedisSearchNumericField) Lt(v float64) RedisSearchCondition {
	return rsNumericRangeCondition{column: f.Column, min: "-inf", max: "(" + strconv.FormatFloat(v, 'f', -1, 64)}
}

func (f RedisSearchNumericField) Between(min, max float64) RedisSearchCondition {
	return rsNumericRangeCondition{
		column: f.Column,
		min:    strconv.FormatFloat(min, 'f', -1, 64),
		max:    strconv.FormatFloat(max, 'f', -1, 64),
	}
}

// RedisSearchTextField represents a text field in a Redis Search index.
type RedisSearchTextField struct{ Column string }

func (f RedisSearchTextField) ColumnName() string { return f.Column }

func (f RedisSearchTextField) Match(text string) RedisSearchCondition {
	return rsTextCondition{column: f.Column, text: text}
}

// RedisSearchTagField represents a tag field in a Redis Search index.
type RedisSearchTagField struct{ Column string }

func (f RedisSearchTagField) ColumnName() string { return f.Column }

func (f RedisSearchTagField) In(values ...string) RedisSearchCondition {
	return rsTagCondition{column: f.Column, values: values}
}
