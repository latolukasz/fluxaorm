package fluxaorm

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type alterV1 struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
}

type alterV2AddNullable struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
	Extra string
}

type alterV2AddRequiredText struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
	Notes string `orm:"required;length=max"`
}

type alterV2Reordered struct {
	ID    uint64 `orm:"table=alter_diff"`
	Count uint32
	Body  string `orm:"required;length=max"`
	Name  string `orm:"required"`
}

type alterV2Dropped struct {
	ID   uint64 `orm:"table=alter_diff"`
	Name string `orm:"required"`
	Body string `orm:"required;length=max"`
}

type alterV2UniqueIndex struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
}

func (alterV2UniqueIndex) UniqueIndexes() [][]string { return [][]string{{"Name"}} }

type alterV2Index struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
}

func (alterV2Index) Indexes() [][]string { return [][]string{{"Count"}} }

// Same index name, different definition — the one shape that forces a drop-and-re-add.
type alterV2IndexRebuilt struct {
	ID    uint64 `orm:"table=alter_diff"`
	Name  string `orm:"required"`
	Body  string `orm:"required;length=max"`
	Count uint32
}

func (alterV2IndexRebuilt) UniqueIndexes() [][]string { return [][]string{{"Count"}} }

// altersAgainstLiveTable diffs a changed entity against the table another version already created,
// which is the only shape that matters here: two code versions, one database.
func altersAgainstLiveTable(t *testing.T, entity any) []Alter {
	t.Helper()
	r := NewRegistry()
	r.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	r.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	r.RegisterEntity(entity)
	engine, err := r.Validate()
	require.NoError(t, err)
	alters, err := GetAlters(engine.NewContext(context.Background()))
	require.NoError(t, err)

	return onlyTable(alters, "alter_diff")
}

func onlyTable(alters []Alter, table string) []Alter {
	out := make([]Alter, 0, len(alters))
	for _, a := range alters {
		if a.Table == table {
			out = append(out, a)
		}
	}

	return out
}

func prepareAlterBaseline(t *testing.T) Context {
	t.Helper()

	return PrepareTables(t, NewRegistry(), alterV1{})
}

// The diff is recomputed from SHOW CREATE TABLE every time, so applying it must leave nothing
// behind. A rendering MySQL echoes back differently from how it was written — an expression
// default is the live example — otherwise re-emits the same statement on every boot, forever.
func TestAppliedAltersLeaveNothingPending(t *testing.T) {
	ctx := prepareAlterBaseline(t)

	alters, err := GetAlters(ctx)
	assert.NoError(t, err)
	assert.Empty(t, onlyTable(alters, "alter_diff"), "a freshly applied schema must produce no further alters")
}

// A required TEXT column cannot take a literal default, so it used to ship as NOT NULL with none —
// and the previous version's INSERT, which does not name the column, fails with 1364.
func TestRequiredTextColumnGetsAnExpressionDefault(t *testing.T) {
	ctx := prepareAlterBaseline(t)

	var skip, createSQL string
	_, err := ctx.Engine().DB(DefaultPoolCode).QueryRow(ctx, NewWhere("SHOW CREATE TABLE `alter_diff`"), &skip, &createSQL)
	assert.NoError(t, err)
	assert.Contains(t, createSQL, "`Body` mediumtext")
	assert.Regexp(t, "`Body` mediumtext[^\\n]*NOT NULL DEFAULT \\(_[a-z0-9]+''\\)", createSQL)

	_, err = ctx.Engine().DB(DefaultPoolCode).Exec(ctx, "INSERT INTO `alter_diff` (`ID`, `Name`) VALUES (1, 'x')")
	assert.NoError(t, err, "an insert that omits the column is exactly what the previous version does")
}

// The two halves have to be applied in the emitted order and both of them: in between, the column
// is NOT NULL with no default and the previous version's INSERT fails 1364. set_default ranks
// straight after add_column so nothing else can land in that window.
func TestAddedRequiredTextColumnEndsUpInsertable(t *testing.T) {
	ctx := prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2AddRequiredText{})
	require.Len(t, alters, 2)
	assert.Equal(t, AlterKindAddColumn, alters[0].Kind)
	assert.Equal(t, AlterKindSetDefault, alters[1].Kind)

	for _, a := range alters {
		require.NoError(t, a.Exec(ctx), a.SQL)
	}

	_, err := ctx.Engine().DB(DefaultPoolCode).Exec(ctx, "INSERT INTO `alter_diff` (`ID`, `Name`) VALUES (7, 'x')")
	assert.NoError(t, err, "the previous version does not name the new column")
}

// Physical column order is unobservable to the ORM, so moving a struct field must cost nothing.
// It used to emit CHANGE COLUMN ... AFTER for every later column: a full table rebuild.
func TestReorderingStructFieldsEmitsNoAlters(t *testing.T) {
	prepareAlterBaseline(t)

	assert.Empty(t, altersAgainstLiveTable(t, alterV2Reordered{}))
}

func TestAddingANullableColumnIsSafe(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2AddNullable{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindAddColumn, alters[0].Kind)
	assert.Equal(t, AlterSafe, alters[0].Safety)
	assert.Contains(t, alters[0].SQL, "ADD COLUMN `Extra`")
}

// A required TEXT column arrives in two instant halves. Carrying the expression default on the ADD
// itself is legal but rebuilds the table, which is exactly what must not happen at boot.
func TestAddingARequiredTextColumnIsTwoInstantHalves(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2AddRequiredText{})
	require.Len(t, alters, 2)

	assert.Equal(t, AlterKindAddColumn, alters[0].Kind)
	assert.Equal(t, AlterSafe, alters[0].Safety)
	assert.Contains(t, alters[0].SQL, "ADD COLUMN `Notes`")
	assert.NotContains(t, alters[0].SQL, "DEFAULT ('')", "an expression default on the ADD forces a rebuild")

	assert.Equal(t, AlterKindSetDefault, alters[1].Kind)
	assert.Equal(t, AlterSafe, alters[1].Safety)
	assert.Contains(t, alters[1].SQL, "DEFAULT ('')")

	for _, a := range alters {
		assert.Contains(t, a.SQL, "ALGORITHM=INSTANT")
	}
}

// Every ADD COLUMN pins the algorithm. MySQL otherwise falls back to a rebuild without saying so —
// past 64 instant additions to one table, for instance — and at boot that is an unexplained stall.
func TestAddColumnPinsTheInstantAlgorithm(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2AddNullable{})
	require.Len(t, alters, 1)
	assert.Contains(t, alters[0].SQL, "ALGORITHM=INSTANT")
}

// A column predating the expression default converges to it, and does so as a metadata change
// rather than a rebuild.
func TestLegacyTextColumnGainsItsDefaultInstantly(t *testing.T) {
	ctx := prepareAlterBaseline(t)
	_, err := ctx.Engine().DB(DefaultPoolCode).Exec(ctx,
		"ALTER TABLE `alter_diff` MODIFY `Body` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL")
	require.NoError(t, err)

	alters := altersAgainstLiveTable(t, alterV1{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindSetDefault, alters[0].Kind)
	assert.Equal(t, AlterSafe, alters[0].Safety)
	assert.Contains(t, alters[0].SQL, "MODIFY `Body`")
	assert.Contains(t, alters[0].SQL, "ALGORITHM=INSTANT")

	// Applying it must settle: MySQL echoes the default back with a charset introducer, and if
	// that is not recognised the same statement is re-emitted on every boot forever.
	_, err = ctx.Engine().DB(DefaultPoolCode).Exec(ctx, alters[0].SQL)
	require.NoError(t, err)
	assert.Empty(t, altersAgainstLiveTable(t, alterV1{}), "the applied default must not re-emit")
}

func TestRemovedFieldEmitsOnlyADestructiveAlter(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2Dropped{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindDropColumn, alters[0].Kind)
	assert.Equal(t, AlterDestructive, alters[0].Safety)
	assert.Contains(t, alters[0].SQL, "DROP COLUMN `Count`")

	safe, destructive := SplitAlters(alters)
	assert.Empty(t, safe, "a drop must never be applied alongside additive work")
	assert.Len(t, destructive, 1)
}

func TestAddingAPlainIndexIsSafe(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2Index{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindAddIndex, alters[0].Kind)
	assert.Equal(t, AlterSafe, alters[0].Safety)
}

// Applied mid-rollout it fails the previous version's inserts; deferred, the previous version
// creates the duplicates that make it fail forever. No ordering is safe.
func TestAddingAUniqueIndexIsDestructive(t *testing.T) {
	prepareAlterBaseline(t)

	alters := altersAgainstLiveTable(t, alterV2UniqueIndex{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindAddUniqueIndex, alters[0].Kind)
	assert.Equal(t, AlterDestructive, alters[0].Safety)
}

// The DROP and the ADD share an index name, so splitting them would make the ADD fail with 1061.
func TestRebuiltIndexStaysWithItsAdd(t *testing.T) {
	ctx := PrepareTables(t, NewRegistry(), alterV2Index{})
	_ = ctx

	alters := altersAgainstLiveTable(t, alterV2IndexRebuilt{})
	require.Len(t, alters, 1)
	assert.Equal(t, AlterKindRebuildIndex, alters[0].Kind)
	assert.Equal(t, AlterDestructive, alters[0].Safety)
	assert.Contains(t, alters[0].SQL, "DROP INDEX `Count`")
	assert.Contains(t, alters[0].SQL, "ADD UNIQUE INDEX `Count`")
}

// A hand-built Alter inherits the zero value, which must mean "never applied without a human".
func TestUnclassifiedAlterIsDestructive(t *testing.T) {
	assert.False(t, Alter{SQL: "SELECT 1", Pool: DefaultPoolCode}.IsSafe())
}

func TestSortAltersIsDeterministicAndOrdersByKind(t *testing.T) {
	alters := []Alter{
		{Table: "t", Kind: AlterKindDropColumn, SQL: "d"},
		{Table: "t", Kind: AlterKindAddIndex, SQL: "i"},
		{Table: "t", Kind: AlterKindCreateTable, SQL: "c"},
		{Table: "t", Kind: AlterKindAddColumn, SQL: "a"},
	}
	sortAlters(alters)
	got := make([]string, len(alters))
	for i, a := range alters {
		got[i] = a.SQL
	}
	assert.Equal(t, []string{"c", "a", "i", "d"}, got)
}

func TestDefinitionEquivalenceNormalisesOnlyTheIntroducer(t *testing.T) {
	base := "`Body` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
	wanted := base + " DEFAULT ('')"

	// The introducer follows the connection charset, so both renderings mean the same column.
	assert.True(t, isDefinitionEquivalent(base+" DEFAULT (_utf8mb4'')", wanted))
	assert.True(t, isDefinitionEquivalent(base+" DEFAULT (_latin1'')", wanted))

	// Having no default at all is a real difference, or the column never gains one.
	assert.False(t, isDefinitionEquivalent(base, wanted))
	assert.False(t, isDefinitionEquivalent(strings.Replace(base, "mediumtext", "text", 1), wanted))

	clause, ok := defaultOnlyDifference(base, wanted)
	assert.True(t, ok)
	assert.Equal(t, "MODIFY "+wanted, clause)
}
