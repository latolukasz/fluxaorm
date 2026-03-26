package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBQueryEmpty(t *testing.T) {
	q := NewQuery()

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "", clause)
	assert.Nil(t, params)

	assert.Equal(t, "", q.BuildOrderClause())
	assert.Equal(t, "", q.BuildLimitClause())
	assert.Nil(t, q.GetConditions())
	assert.False(t, q.IsWithFakeDeletes())
}

func TestDBQueryFilterConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().Filter(age.Gte(18), name.Is("John"))

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Name` = ?", clause)
	assert.Equal(t, []any{uint64(18), "John"}, params)
}

func TestDBQueryFilterMultipleCalls(t *testing.T) {
	age := UintField{Column: "Age"}
	active := BoolField{Column: "Active"}

	q := NewQuery().
		Filter(age.Gte(18)).
		Filter(active.Is(true))

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Active` = ?", clause)
	assert.Equal(t, []any{uint64(18), true}, params)
}

func TestDBQuerySortClauses(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().SortByASC(age).SortByDESC(name)
	assert.Equal(t, "ORDER BY `Age` ASC, `Name` DESC", q.BuildOrderClause())
}

func TestDBQuerySingleSort(t *testing.T) {
	f := UintField{Column: "ID"}
	q := NewQuery().SortByDESC(f)
	assert.Equal(t, "ORDER BY `ID` DESC", q.BuildOrderClause())
}

func TestDBQueryPager(t *testing.T) {
	p := NewPager(2, 10)
	q := NewQuery().Pager(p)
	assert.Equal(t, "LIMIT 10,10", q.BuildLimitClause())
}

func TestDBQueryFilterWhere(t *testing.T) {
	w := NewWhere("`Status` = ? AND `Level` > ?", "active", 5)
	q := NewQuery().FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Status` = ? AND `Level` > ?", clause)
	assert.Equal(t, []any{"active", 5}, params)
}

func TestDBQueryFilterWhereWithTypedConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	w := NewWhere("`Status` = ?", "active")

	q := NewQuery().
		Filter(age.Gte(18)).
		FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Status` = ?", clause)
	assert.Equal(t, []any{uint64(18), "active"}, params)
}

func TestDBQueryGetConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().Filter(age.Eq(25), name.Is("Alice"))

	conditions := q.GetConditions()
	assert.Len(t, conditions, 2)

	eq, ok := conditions[0].(EqCondition)
	assert.True(t, ok)
	assert.Equal(t, uint64(25), eq.EqValue())
}

func TestDBQueryIsWithFakeDeletes(t *testing.T) {
	q := NewQuery()
	assert.False(t, q.IsWithFakeDeletes())

	w := NewWhere("`Status` = ?", "active")
	q = NewQuery().FilterWhere(w)
	assert.False(t, q.IsWithFakeDeletes())

	w2 := NewWhere("`Status` = ?", "active").WithFakeDeletes()
	q = NewQuery().FilterWhere(w2)
	assert.True(t, q.IsWithFakeDeletes())
}

func TestDBQueryChaining(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}
	p := NewPager(1, 20)

	q := NewQuery().
		Filter(age.Gte(18), name.Is("John")).
		SortByASC(age).
		SortByDESC(name).
		Pager(p)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Name` = ?", clause)
	assert.Equal(t, []any{uint64(18), "John"}, params)
	assert.Equal(t, "ORDER BY `Age` ASC, `Name` DESC", q.BuildOrderClause())
	assert.Equal(t, "LIMIT 0,20", q.BuildLimitClause())
}

func TestDBQueryFilterWhereEmptyString(t *testing.T) {
	w := NewWhere("")
	q := NewQuery().FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "", clause)
	assert.Nil(t, params)
}
