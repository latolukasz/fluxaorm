package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFieldUintField(t *testing.T) {
	f := UintField{Column: "Age"}
	assert.Equal(t, "Age", f.ColumnName())

	clause, params := f.Eq(42).ToSQL()
	assert.Equal(t, "`Age` = ?", clause)
	assert.Equal(t, []any{uint64(42)}, params)

	clause, params = f.In(1, 2, 3).ToSQL()
	assert.Equal(t, "`Age` IN (?,?,?)", clause)
	assert.Equal(t, []any{uint64(1), uint64(2), uint64(3)}, params)

	clause, params = f.Gte(10).ToSQL()
	assert.Equal(t, "`Age` >= ?", clause)
	assert.Equal(t, []any{uint64(10)}, params)

	clause, params = f.Lte(100).ToSQL()
	assert.Equal(t, "`Age` <= ?", clause)
	assert.Equal(t, []any{uint64(100)}, params)

	clause, params = f.Gt(5).ToSQL()
	assert.Equal(t, "`Age` > ?", clause)
	assert.Equal(t, []any{uint64(5)}, params)

	clause, params = f.Lt(50).ToSQL()
	assert.Equal(t, "`Age` < ?", clause)
	assert.Equal(t, []any{uint64(50)}, params)
}

func TestFieldIntField(t *testing.T) {
	f := IntField{Column: "Score"}
	assert.Equal(t, "Score", f.ColumnName())

	clause, params := f.Eq(-10).ToSQL()
	assert.Equal(t, "`Score` = ?", clause)
	assert.Equal(t, []any{int64(-10)}, params)

	clause, params = f.In(1, 2, 3).ToSQL()
	assert.Equal(t, "`Score` IN (?,?,?)", clause)
	assert.Equal(t, []any{int64(1), int64(2), int64(3)}, params)

	clause, params = f.Gte(0).ToSQL()
	assert.Equal(t, "`Score` >= ?", clause)
	assert.Equal(t, []any{int64(0)}, params)

	clause, params = f.Lt(100).ToSQL()
	assert.Equal(t, "`Score` < ?", clause)
	assert.Equal(t, []any{int64(100)}, params)
}

func TestFieldStringField(t *testing.T) {
	f := StringField{Column: "Name"}
	assert.Equal(t, "Name", f.ColumnName())

	clause, params := f.Is("hello").ToSQL()
	assert.Equal(t, "`Name` = ?", clause)
	assert.Equal(t, []any{"hello"}, params)

	clause, params = f.Like("%test%").ToSQL()
	assert.Equal(t, "`Name` LIKE ?", clause)
	assert.Equal(t, []any{"%test%"}, params)

	clause, params = f.In("a", "b", "c").ToSQL()
	assert.Equal(t, "`Name` IN (?,?,?)", clause)
	assert.Equal(t, []any{"a", "b", "c"}, params)
}

func TestFieldBoolField(t *testing.T) {
	f := BoolField{Column: "Active"}
	assert.Equal(t, "Active", f.ColumnName())

	clause, params := f.Is(true).ToSQL()
	assert.Equal(t, "`Active` = ?", clause)
	assert.Equal(t, []any{true}, params)

	clause, params = f.Is(false).ToSQL()
	assert.Equal(t, "`Active` = ?", clause)
	assert.Equal(t, []any{false}, params)
}

func TestFieldFloatField(t *testing.T) {
	f := FloatField{Column: "Price"}
	assert.Equal(t, "Price", f.ColumnName())

	clause, params := f.Eq(9.99).ToSQL()
	assert.Equal(t, "`Price` = ?", clause)
	assert.Equal(t, []any{9.99}, params)

	clause, params = f.In(1.1, 2.2).ToSQL()
	assert.Equal(t, "`Price` IN (?,?)", clause)
	assert.Equal(t, []any{1.1, 2.2}, params)

	clause, params = f.Gte(0.5).ToSQL()
	assert.Equal(t, "`Price` >= ?", clause)
	assert.Equal(t, []any{0.5}, params)

	clause, params = f.Lt(100.0).ToSQL()
	assert.Equal(t, "`Price` < ?", clause)
	assert.Equal(t, []any{100.0}, params)
}

func TestFieldTimeField(t *testing.T) {
	f := TimeField{Column: "CreatedAt"}
	assert.Equal(t, "CreatedAt", f.ColumnName())

	now := time.Now()
	clause, params := f.Eq(now).ToSQL()
	assert.Equal(t, "`CreatedAt` = ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.Gte(now).ToSQL()
	assert.Equal(t, "`CreatedAt` >= ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.Lte(now).ToSQL()
	assert.Equal(t, "`CreatedAt` <= ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.Gt(now).ToSQL()
	assert.Equal(t, "`CreatedAt` > ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.Lt(now).ToSQL()
	assert.Equal(t, "`CreatedAt` < ?", clause)
	assert.Equal(t, []any{now}, params)
}

func TestFieldEnumField(t *testing.T) {
	f := EnumField{Column: "Status"}
	assert.Equal(t, "Status", f.ColumnName())

	clause, params := f.Is("active").ToSQL()
	assert.Equal(t, "`Status` = ?", clause)
	assert.Equal(t, []any{"active"}, params)

	clause, params = f.In("active", "pending").ToSQL()
	assert.Equal(t, "`Status` IN (?,?)", clause)
	assert.Equal(t, []any{"active", "pending"}, params)
}

func TestFieldReferenceField(t *testing.T) {
	f := ReferenceField{Column: "UserID"}
	assert.Equal(t, "UserID", f.ColumnName())

	clause, params := f.Eq(42).ToSQL()
	assert.Equal(t, "`UserID` = ?", clause)
	assert.Equal(t, []any{uint64(42)}, params)

	clause, params = f.In(1, 2, 3).ToSQL()
	assert.Equal(t, "`UserID` IN (?,?,?)", clause)
	assert.Equal(t, []any{uint64(1), uint64(2), uint64(3)}, params)
}

func TestFieldNullableUintField(t *testing.T) {
	f := NullableUintField{Column: "Age"}
	assert.Equal(t, "Age", f.ColumnName())

	clause, params := f.Eq(42).ToSQL()
	assert.Equal(t, "`Age` = ?", clause)
	assert.Equal(t, []any{uint64(42)}, params)

	clause, params = f.In(1, 2).ToSQL()
	assert.Equal(t, "`Age` IN (?,?)", clause)
	assert.Equal(t, []any{uint64(1), uint64(2)}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Age` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Age` IS NOT NULL", clause)
	assert.Nil(t, params)

	clause, params = f.Gte(10).ToSQL()
	assert.Equal(t, "`Age` >= ?", clause)
	assert.Equal(t, []any{uint64(10)}, params)
}

func TestFieldNullableIntField(t *testing.T) {
	f := NullableIntField{Column: "Score"}
	assert.Equal(t, "Score", f.ColumnName())

	clause, params := f.Eq(-5).ToSQL()
	assert.Equal(t, "`Score` = ?", clause)
	assert.Equal(t, []any{int64(-5)}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Score` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Score` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableStringField(t *testing.T) {
	f := NullableStringField{Column: "Bio"}
	assert.Equal(t, "Bio", f.ColumnName())

	clause, params := f.Is("hello").ToSQL()
	assert.Equal(t, "`Bio` = ?", clause)
	assert.Equal(t, []any{"hello"}, params)

	clause, params = f.Like("%world%").ToSQL()
	assert.Equal(t, "`Bio` LIKE ?", clause)
	assert.Equal(t, []any{"%world%"}, params)

	clause, params = f.In("a", "b").ToSQL()
	assert.Equal(t, "`Bio` IN (?,?)", clause)
	assert.Equal(t, []any{"a", "b"}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Bio` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Bio` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableBoolField(t *testing.T) {
	f := NullableBoolField{Column: "Verified"}
	assert.Equal(t, "Verified", f.ColumnName())

	clause, params := f.Is(true).ToSQL()
	assert.Equal(t, "`Verified` = ?", clause)
	assert.Equal(t, []any{true}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Verified` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Verified` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableFloatField(t *testing.T) {
	f := NullableFloatField{Column: "Rating"}
	assert.Equal(t, "Rating", f.ColumnName())

	clause, params := f.Eq(4.5).ToSQL()
	assert.Equal(t, "`Rating` = ?", clause)
	assert.Equal(t, []any{4.5}, params)

	clause, params = f.In(1.0, 2.0).ToSQL()
	assert.Equal(t, "`Rating` IN (?,?)", clause)
	assert.Equal(t, []any{1.0, 2.0}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Rating` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Rating` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableEnumField(t *testing.T) {
	f := NullableEnumField{Column: "Status"}
	assert.Equal(t, "Status", f.ColumnName())

	clause, params := f.Is("active").ToSQL()
	assert.Equal(t, "`Status` = ?", clause)
	assert.Equal(t, []any{"active"}, params)

	clause, params = f.In("active", "pending").ToSQL()
	assert.Equal(t, "`Status` IN (?,?)", clause)
	assert.Equal(t, []any{"active", "pending"}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`Status` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`Status` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableReferenceField(t *testing.T) {
	f := NullableReferenceField{Column: "UserID"}
	assert.Equal(t, "UserID", f.ColumnName())

	clause, params := f.Eq(42).ToSQL()
	assert.Equal(t, "`UserID` = ?", clause)
	assert.Equal(t, []any{uint64(42)}, params)

	clause, params = f.In(1, 2, 3).ToSQL()
	assert.Equal(t, "`UserID` IN (?,?,?)", clause)
	assert.Equal(t, []any{uint64(1), uint64(2), uint64(3)}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`UserID` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`UserID` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldNullableTimeField(t *testing.T) {
	f := NullableTimeField{Column: "DeletedAt"}
	assert.Equal(t, "DeletedAt", f.ColumnName())

	now := time.Now()
	clause, params := f.Eq(now).ToSQL()
	assert.Equal(t, "`DeletedAt` = ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.Gte(now).ToSQL()
	assert.Equal(t, "`DeletedAt` >= ?", clause)
	assert.Equal(t, []any{now}, params)

	clause, params = f.IsNull().ToSQL()
	assert.Equal(t, "`DeletedAt` IS NULL", clause)
	assert.Nil(t, params)

	clause, params = f.IsNotNull().ToSQL()
	assert.Equal(t, "`DeletedAt` IS NOT NULL", clause)
	assert.Nil(t, params)
}

func TestFieldEqConditionInterface(t *testing.T) {
	f := UintField{Column: "ID"}
	cond := f.Eq(42)

	eq, ok := cond.(EqCondition)
	assert.True(t, ok)
	assert.Equal(t, uint64(42), eq.EqValue())

	sf := StringField{Column: "Name"}
	cond = sf.Is("test")
	eq, ok = cond.(EqCondition)
	assert.True(t, ok)
	assert.Equal(t, "test", eq.EqValue())

	bf := BoolField{Column: "Active"}
	cond = bf.Is(true)
	eq, ok = cond.(EqCondition)
	assert.True(t, ok)
	assert.Equal(t, true, eq.EqValue())
}

func TestFieldColumnName(t *testing.T) {
	// Verify all field types implement Field interface
	var fields []Field
	fields = append(fields,
		UintField{Column: "a"},
		IntField{Column: "b"},
		StringField{Column: "c"},
		BoolField{Column: "d"},
		FloatField{Column: "e"},
		TimeField{Column: "f"},
		EnumField{Column: "g"},
		ReferenceField{Column: "h"},
		NullableUintField{Column: "i"},
		NullableIntField{Column: "j"},
		NullableStringField{Column: "k"},
		NullableBoolField{Column: "l"},
		NullableFloatField{Column: "m"},
		NullableTimeField{Column: "n"},
		NullableEnumField{Column: "o"},
		NullableReferenceField{Column: "p"},
	)

	expected := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"}
	for i, f := range fields {
		assert.Equal(t, expected[i], f.ColumnName())
	}
}

func TestFieldInEmptySlice(t *testing.T) {
	f := UintField{Column: "ID"}
	clause, params := f.In().ToSQL()
	assert.Equal(t, "`ID` IN ()", clause)
	assert.Empty(t, params)
}

func TestFieldConditionColumnName(t *testing.T) {
	f := UintField{Column: "Age"}
	cond := f.Eq(1)
	// Verify the unexported columnName via the Condition interface
	assert.Equal(t, "Age", cond.(eqCondition).columnName())

	cond2 := f.Gte(1)
	assert.Equal(t, "Age", cond2.(compCondition).columnName())

	cond3 := f.In(1, 2)
	assert.Equal(t, "Age", cond3.(inCondition).columnName())

	sf := StringField{Column: "Name"}
	cond4 := sf.Like("%x%")
	assert.Equal(t, "Name", cond4.(likeCondition).columnName())

	nf := NullableUintField{Column: "Val"}
	cond5 := nf.IsNull()
	assert.Equal(t, "Val", cond5.(nullCondition).columnName())
}
