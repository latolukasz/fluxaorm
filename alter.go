package fluxaorm

import "sort"

// AlterSafety answers one question: can the previous code version keep running against this
// statement? AlterDestructive is the zero value so an unclassified path fails closed.
type AlterSafety uint8

const (
	// AlterDestructive must not run while the previous version is live.
	AlterDestructive AlterSafety = iota
	// AlterSafe is forward-compatible with the previous code version.
	AlterSafe
)

func (s AlterSafety) String() string {
	if s == AlterSafe {
		return "safe"
	}

	return "destructive"
}

// AlterKind is what metrics label and listings sort by.
type AlterKind string

const (
	AlterKindCreateTable    AlterKind = "create_table"
	AlterKindAddColumn      AlterKind = "add_column"
	AlterKindSetDefault     AlterKind = "set_default"
	AlterKindAddIndex       AlterKind = "add_index"
	AlterKindAddUniqueIndex AlterKind = "add_unique_index"
	AlterKindRebuildIndex   AlterKind = "rebuild_index"
	AlterKindChangeColumn   AlterKind = "change_column"
	AlterKindDropIndex      AlterKind = "drop_index"
	AlterKindDropColumn     AlterKind = "drop_column"
	AlterKindDropForeignKey AlterKind = "drop_foreign_key"
	AlterKindModifyTable    AlterKind = "modify_table"
	AlterKindConvertTable   AlterKind = "convert_table"
	AlterKindDropTable      AlterKind = "drop_table"
)

// alterKindRank orders a run: tables, then columns, then indexes over them, then drops.
var alterKindRank = map[AlterKind]int{
	AlterKindCreateTable: 0,
	AlterKindAddColumn:   10,
	// Straight after the adds it completes: a column exists NOT NULL and defaultless in between,
	// and an INSERT omitting it fails 1364 under strict mode.
	AlterKindSetDefault:     11,
	AlterKindAddIndex:       20,
	AlterKindAddUniqueIndex: 20,
	AlterKindRebuildIndex:   30,
	AlterKindModifyTable:    35,
	AlterKindChangeColumn:   40,
	AlterKindDropForeignKey: 45,
	AlterKindDropIndex:      50,
	AlterKindDropColumn:     60,
	AlterKindConvertTable:   70,
	AlterKindDropTable:      80,
}

// AllAlterKinds is every kind the diff can emit, for callers building a policy over them.
func AllAlterKinds() []AlterKind {
	kinds := make([]AlterKind, 0, len(alterKindRank))
	for kind := range alterKindRank {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })

	return kinds
}

func (a Alter) IsSafe() bool { return a.Safety == AlterSafe }

// SplitAlters partitions a plan into what may be applied during a rollout and what may not.
func SplitAlters(in []Alter) (safe, destructive []Alter) {
	for _, a := range in {
		if a.IsSafe() {
			safe = append(safe, a)
		} else {
			destructive = append(destructive, a)
		}
	}

	return safe, destructive
}

// sortAlters makes the plan deterministic; registry iteration is map-ordered.
func sortAlters(alters []Alter) {
	sort.SliceStable(alters, func(i, j int) bool {
		if alters[i].Pool != alters[j].Pool {
			return alters[i].Pool < alters[j].Pool
		}
		if alters[i].Table != alters[j].Table {
			return alters[i].Table < alters[j].Table
		}
		if alterKindRank[alters[i].Kind] != alterKindRank[alters[j].Kind] {
			return alterKindRank[alters[i].Kind] < alterKindRank[alters[j].Kind]
		}

		return alters[i].SQL < alters[j].SQL
	})
}
