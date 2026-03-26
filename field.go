package fluxaorm

import (
	"strings"
	"time"
)

// Condition represents a single WHERE clause predicate.
type Condition interface {
	ToSQL() (clause string, params []any)
	columnName() string
}

// Field is implemented by all field types for use in SortByASC/SortByDESC.
type Field interface {
	ColumnName() string
}

// EqCondition is exposed for generated code to type-assert and extract values for cached unique index detection.
type EqCondition interface {
	Condition
	EqValue() any
}

type eqCondition struct {
	column string
	value  any
}

func (c eqCondition) ToSQL() (string, []any) {
	return "`" + c.column + "` = ?", []any{c.value}
}

func (c eqCondition) columnName() string { return c.column }
func (c eqCondition) EqValue() any       { return c.value }

type compCondition struct {
	column   string
	operator string
	value    any
}

func (c compCondition) ToSQL() (string, []any) {
	return "`" + c.column + "` " + c.operator + " ?", []any{c.value}
}

func (c compCondition) columnName() string { return c.column }

type inCondition struct {
	column string
	values []any
}

func (c inCondition) ToSQL() (string, []any) {
	placeholders := strings.Repeat(",?", len(c.values))
	if len(placeholders) > 0 {
		placeholders = placeholders[1:]
	}
	return "`" + c.column + "` IN (" + placeholders + ")", c.values
}

func (c inCondition) columnName() string { return c.column }

type likeCondition struct {
	column string
	value  string
}

func (c likeCondition) ToSQL() (string, []any) {
	return "`" + c.column + "` LIKE ?", []any{c.value}
}

func (c likeCondition) columnName() string { return c.column }

type nullCondition struct {
	column string
	isNull bool
}

func (c nullCondition) ToSQL() (string, []any) {
	if c.isNull {
		return "`" + c.column + "` IS NULL", nil
	}
	return "`" + c.column + "` IS NOT NULL", nil
}

func (c nullCondition) columnName() string { return c.column }

// UintField represents an unsigned integer column.
type UintField struct{ Column string }

func (f UintField) ColumnName() string             { return f.Column }
func (f UintField) Eq(v uint64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f UintField) Gte(v uint64) Condition          { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f UintField) Lte(v uint64) Condition          { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f UintField) Gt(v uint64) Condition           { return compCondition{column: f.Column, operator: ">", value: v} }
func (f UintField) Lt(v uint64) Condition           { return compCondition{column: f.Column, operator: "<", value: v} }
func (f UintField) In(values ...uint64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// IntField represents a signed integer column.
type IntField struct{ Column string }

func (f IntField) ColumnName() string            { return f.Column }
func (f IntField) Eq(v int64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f IntField) Gte(v int64) Condition          { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f IntField) Lte(v int64) Condition          { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f IntField) Gt(v int64) Condition           { return compCondition{column: f.Column, operator: ">", value: v} }
func (f IntField) Lt(v int64) Condition           { return compCondition{column: f.Column, operator: "<", value: v} }
func (f IntField) In(values ...int64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// StringField represents a string column.
type StringField struct{ Column string }

func (f StringField) ColumnName() string              { return f.Column }
func (f StringField) Is(v string) Condition            { return eqCondition{column: f.Column, value: v} }
func (f StringField) Like(v string) Condition          { return likeCondition{column: f.Column, value: v} }
func (f StringField) In(values ...string) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// BoolField represents a boolean column.
type BoolField struct{ Column string }

func (f BoolField) ColumnName() string   { return f.Column }
func (f BoolField) Is(v bool) Condition   { return eqCondition{column: f.Column, value: v} }

// FloatField represents a floating-point column.
type FloatField struct{ Column string }

func (f FloatField) ColumnName() string               { return f.Column }
func (f FloatField) Eq(v float64) Condition            { return eqCondition{column: f.Column, value: v} }
func (f FloatField) Gte(v float64) Condition           { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f FloatField) Lte(v float64) Condition           { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f FloatField) Gt(v float64) Condition            { return compCondition{column: f.Column, operator: ">", value: v} }
func (f FloatField) Lt(v float64) Condition            { return compCondition{column: f.Column, operator: "<", value: v} }
func (f FloatField) In(values ...float64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// TimeField represents a time.Time column.
type TimeField struct{ Column string }

func (f TimeField) ColumnName() string              { return f.Column }
func (f TimeField) Eq(v time.Time) Condition         { return eqCondition{column: f.Column, value: v} }
func (f TimeField) Gte(v time.Time) Condition        { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f TimeField) Lte(v time.Time) Condition        { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f TimeField) Gt(v time.Time) Condition         { return compCondition{column: f.Column, operator: ">", value: v} }
func (f TimeField) Lt(v time.Time) Condition         { return compCondition{column: f.Column, operator: "<", value: v} }

// EnumField represents an enum column stored as a string.
type EnumField struct{ Column string }

func (f EnumField) ColumnName() string             { return f.Column }
func (f EnumField) Is(v string) Condition           { return eqCondition{column: f.Column, value: v} }
func (f EnumField) In(values ...string) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// ReferenceField represents a foreign key reference column.
type ReferenceField struct{ Column string }

func (f ReferenceField) ColumnName() string             { return f.Column }
func (f ReferenceField) Eq(v uint64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f ReferenceField) In(values ...uint64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}

// NullableUintField represents a nullable unsigned integer column.
type NullableUintField struct{ Column string }

func (f NullableUintField) ColumnName() string             { return f.Column }
func (f NullableUintField) Eq(v uint64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f NullableUintField) Gte(v uint64) Condition          { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f NullableUintField) Lte(v uint64) Condition          { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f NullableUintField) Gt(v uint64) Condition           { return compCondition{column: f.Column, operator: ">", value: v} }
func (f NullableUintField) Lt(v uint64) Condition           { return compCondition{column: f.Column, operator: "<", value: v} }
func (f NullableUintField) In(values ...uint64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableUintField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableUintField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableIntField represents a nullable signed integer column.
type NullableIntField struct{ Column string }

func (f NullableIntField) ColumnName() string            { return f.Column }
func (f NullableIntField) Eq(v int64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f NullableIntField) Gte(v int64) Condition          { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f NullableIntField) Lte(v int64) Condition          { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f NullableIntField) Gt(v int64) Condition           { return compCondition{column: f.Column, operator: ">", value: v} }
func (f NullableIntField) Lt(v int64) Condition           { return compCondition{column: f.Column, operator: "<", value: v} }
func (f NullableIntField) In(values ...int64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableIntField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableIntField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableStringField represents a nullable string column.
type NullableStringField struct{ Column string }

func (f NullableStringField) ColumnName() string              { return f.Column }
func (f NullableStringField) Is(v string) Condition            { return eqCondition{column: f.Column, value: v} }
func (f NullableStringField) Like(v string) Condition          { return likeCondition{column: f.Column, value: v} }
func (f NullableStringField) In(values ...string) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableStringField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableStringField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableBoolField represents a nullable boolean column.
type NullableBoolField struct{ Column string }

func (f NullableBoolField) ColumnName() string   { return f.Column }
func (f NullableBoolField) Is(v bool) Condition   { return eqCondition{column: f.Column, value: v} }
func (f NullableBoolField) IsNull() Condition     { return nullCondition{column: f.Column, isNull: true} }
func (f NullableBoolField) IsNotNull() Condition  { return nullCondition{column: f.Column, isNull: false} }

// NullableFloatField represents a nullable floating-point column.
type NullableFloatField struct{ Column string }

func (f NullableFloatField) ColumnName() string               { return f.Column }
func (f NullableFloatField) Eq(v float64) Condition            { return eqCondition{column: f.Column, value: v} }
func (f NullableFloatField) Gte(v float64) Condition           { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f NullableFloatField) Lte(v float64) Condition           { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f NullableFloatField) Gt(v float64) Condition            { return compCondition{column: f.Column, operator: ">", value: v} }
func (f NullableFloatField) Lt(v float64) Condition            { return compCondition{column: f.Column, operator: "<", value: v} }
func (f NullableFloatField) In(values ...float64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableFloatField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableFloatField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableEnumField represents a nullable enum column stored as a string.
type NullableEnumField struct{ Column string }

func (f NullableEnumField) ColumnName() string             { return f.Column }
func (f NullableEnumField) Is(v string) Condition           { return eqCondition{column: f.Column, value: v} }
func (f NullableEnumField) In(values ...string) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableEnumField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableEnumField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableReferenceField represents a nullable foreign key reference column.
type NullableReferenceField struct{ Column string }

func (f NullableReferenceField) ColumnName() string             { return f.Column }
func (f NullableReferenceField) Eq(v uint64) Condition           { return eqCondition{column: f.Column, value: v} }
func (f NullableReferenceField) In(values ...uint64) Condition {
	v := make([]any, len(values))
	for i, val := range values {
		v[i] = val
	}
	return inCondition{column: f.Column, values: v}
}
func (f NullableReferenceField) IsNull() Condition    { return nullCondition{column: f.Column, isNull: true} }
func (f NullableReferenceField) IsNotNull() Condition { return nullCondition{column: f.Column, isNull: false} }

// NullableTimeField represents a nullable time.Time column.
type NullableTimeField struct{ Column string }

func (f NullableTimeField) ColumnName() string              { return f.Column }
func (f NullableTimeField) Eq(v time.Time) Condition         { return eqCondition{column: f.Column, value: v} }
func (f NullableTimeField) Gte(v time.Time) Condition        { return compCondition{column: f.Column, operator: ">=", value: v} }
func (f NullableTimeField) Lte(v time.Time) Condition        { return compCondition{column: f.Column, operator: "<=", value: v} }
func (f NullableTimeField) Gt(v time.Time) Condition         { return compCondition{column: f.Column, operator: ">", value: v} }
func (f NullableTimeField) Lt(v time.Time) Condition         { return compCondition{column: f.Column, operator: "<", value: v} }
func (f NullableTimeField) IsNull() Condition                { return nullCondition{column: f.Column, isNull: true} }
func (f NullableTimeField) IsNotNull() Condition             { return nullCondition{column: f.Column, isNull: false} }
