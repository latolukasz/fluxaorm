package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearchNumericFieldEq(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "Age", f.ColumnName())
	assert.Equal(t, "@Age:[42 42]", f.Eq(42).ToFTSearch())
}

func TestRedisSearchNumericFieldGte(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "@Age:[10 +inf]", f.Gte(10).ToFTSearch())
}

func TestRedisSearchNumericFieldLte(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "@Age:[-inf 10]", f.Lte(10).ToFTSearch())
}

func TestRedisSearchNumericFieldGt(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "@Age:[(5 +inf]", f.Gt(5).ToFTSearch())
}

func TestRedisSearchNumericFieldLt(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "@Age:[-inf (5]", f.Lt(5).ToFTSearch())
}

func TestRedisSearchNumericFieldBetween(t *testing.T) {
	f := RedisSearchNumericField{Column: "Age"}
	assert.Equal(t, "@Age:[1 10]", f.Between(1, 10).ToFTSearch())
}

func TestRedisSearchNumericFieldFloatValues(t *testing.T) {
	f := RedisSearchNumericField{Column: "Price"}
	assert.Equal(t, "@Price:[9.99 9.99]", f.Eq(9.99).ToFTSearch())
	assert.Equal(t, "@Price:[0.5 +inf]", f.Gte(0.5).ToFTSearch())
}

func TestRedisSearchTextFieldMatch(t *testing.T) {
	f := RedisSearchTextField{Column: "Name"}
	assert.Equal(t, "Name", f.ColumnName())
	assert.Equal(t, "@Name:hello", f.Match("hello").ToFTSearch())
}

func TestRedisSearchTextFieldMatchPhrase(t *testing.T) {
	f := RedisSearchTextField{Column: "Description"}
	assert.Equal(t, "@Description:hello world", f.Match("hello world").ToFTSearch())
}

func TestRedisSearchTagFieldIn(t *testing.T) {
	f := RedisSearchTagField{Column: "Type"}
	assert.Equal(t, "Type", f.ColumnName())
	assert.Equal(t, "@Type:{a|b}", f.In("a", "b").ToFTSearch())
}

func TestRedisSearchTagFieldInSingle(t *testing.T) {
	f := RedisSearchTagField{Column: "Type"}
	assert.Equal(t, "@Type:{active}", f.In("active").ToFTSearch())
}

func TestRedisSearchTagFieldEscapePipe(t *testing.T) {
	f := RedisSearchTagField{Column: "Tags"}
	assert.Equal(t, "@Tags:{a\\|b|c}", f.In("a|b", "c").ToFTSearch())
}

func TestRedisSearchUintFieldEq(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "Count", f.ColumnName())
	assert.Equal(t, "@Count:[42 42]", f.Eq(42).ToFTSearch())
}

func TestRedisSearchUintFieldGte(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "@Count:[10 +inf]", f.Gte(10).ToFTSearch())
}

func TestRedisSearchUintFieldLte(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "@Count:[0 10]", f.Lte(10).ToFTSearch())
}

func TestRedisSearchUintFieldGt(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "@Count:[(5 +inf]", f.Gt(5).ToFTSearch())
}

func TestRedisSearchUintFieldLt(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "@Count:[0 (5]", f.Lt(5).ToFTSearch())
}

func TestRedisSearchUintFieldBetween(t *testing.T) {
	f := RedisSearchUintField{Column: "Count"}
	assert.Equal(t, "@Count:[1 10]", f.Between(1, 10).ToFTSearch())
}

func TestRedisSearchIntFieldEq(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "Score", f.ColumnName())
	assert.Equal(t, "@Score:[42 42]", f.Eq(42).ToFTSearch())
}

func TestRedisSearchIntFieldNegative(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[-5 -5]", f.Eq(-5).ToFTSearch())
	assert.Equal(t, "@Score:[-10 +inf]", f.Gte(-10).ToFTSearch())
	assert.Equal(t, "@Score:[-inf -10]", f.Lte(-10).ToFTSearch())
}

func TestRedisSearchIntFieldGte(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[10 +inf]", f.Gte(10).ToFTSearch())
}

func TestRedisSearchIntFieldLte(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[-inf 10]", f.Lte(10).ToFTSearch())
}

func TestRedisSearchIntFieldGt(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[(5 +inf]", f.Gt(5).ToFTSearch())
}

func TestRedisSearchIntFieldLt(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[-inf (5]", f.Lt(5).ToFTSearch())
}

func TestRedisSearchIntFieldBetween(t *testing.T) {
	f := RedisSearchIntField{Column: "Score"}
	assert.Equal(t, "@Score:[1 10]", f.Between(1, 10).ToFTSearch())
}

func TestRedisSearchFieldInterface(t *testing.T) {
	var fields []RedisSearchField
	fields = append(fields,
		RedisSearchNumericField{Column: "num"},
		RedisSearchUintField{Column: "uint"},
		RedisSearchIntField{Column: "int"},
		RedisSearchTextField{Column: "txt"},
		RedisSearchTagField{Column: "tag"},
	)
	expected := []string{"num", "uint", "int", "txt", "tag"}
	for i, f := range fields {
		assert.Equal(t, expected[i], f.ColumnName())
	}
}
