package fluxaorm

import "strings"

type sortClause struct {
	column string
	asc    bool
}

// DBQuery is a type-safe query builder for MySQL queries.
type DBQuery struct {
	pager       *Pager
	sortClauses []sortClause
	conditions  []Condition
	rawWhere    Where
}

// NewQuery creates a new empty DBQuery.
func NewQuery() *DBQuery {
	return &DBQuery{}
}

// Pager sets the pagination for the query.
func (q *DBQuery) Pager(p *Pager) *DBQuery {
	q.pager = p
	return q
}

// SortByASC adds an ascending sort clause.
func (q *DBQuery) SortByASC(f Field) *DBQuery {
	q.sortClauses = append(q.sortClauses, sortClause{column: f.ColumnName(), asc: true})
	return q
}

// SortByDESC adds a descending sort clause.
func (q *DBQuery) SortByDESC(f Field) *DBQuery {
	q.sortClauses = append(q.sortClauses, sortClause{column: f.ColumnName(), asc: false})
	return q
}

// Filter adds typed conditions to the query.
func (q *DBQuery) Filter(conditions ...Condition) *DBQuery {
	q.conditions = append(q.conditions, conditions...)
	return q
}

// FilterWhere adds a raw Where clause to the query.
func (q *DBQuery) FilterWhere(w Where) *DBQuery {
	q.rawWhere = w
	return q
}

// GetConditions returns typed conditions for generated code's unique index detection.
func (q *DBQuery) GetConditions() []Condition {
	return q.conditions
}

// BuildWhereClause builds the WHERE portion (without "WHERE" keyword).
// Returns empty string and nil params if no conditions.
func (q *DBQuery) BuildWhereClause() (string, []any) {
	var parts []string
	var params []any

	for _, c := range q.conditions {
		clause, p := c.ToSQL()
		parts = append(parts, clause)
		params = append(params, p...)
	}

	if q.rawWhere != nil {
		if w := q.rawWhere.String(); w != "" {
			parts = append(parts, w)
			params = append(params, q.rawWhere.GetParameters()...)
		}
	}

	return strings.Join(parts, " AND "), params
}

// BuildOrderClause returns "ORDER BY `col1` ASC, `col2` DESC" or empty string.
func (q *DBQuery) BuildOrderClause() string {
	if len(q.sortClauses) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("ORDER BY ")
	for i, s := range q.sortClauses {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("`" + s.column + "`")
		if s.asc {
			b.WriteString(" ASC")
		} else {
			b.WriteString(" DESC")
		}
	}
	return b.String()
}

// BuildLimitClause returns "LIMIT offset,count" or empty string.
func (q *DBQuery) BuildLimitClause() string {
	if q.pager == nil {
		return ""
	}
	return q.pager.String()
}

// IsWithFakeDeletes returns true if rawWhere has WithFakeDeletes set.
func (q *DBQuery) IsWithFakeDeletes() bool {
	if q.rawWhere != nil {
		return q.rawWhere.IsWithFakeDeletes()
	}
	return false
}
