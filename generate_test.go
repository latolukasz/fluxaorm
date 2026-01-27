package fluxaorm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testGenerateEnum string

func (s testGenerateEnum) EnumValues() any {
	return testGenerateEnumDefinition
}

var testGenerateEnumDefinition = struct {
	A testGenerateEnum
	B testGenerateEnum
	C testGenerateEnum
}{
	A: "a",
	B: "b",
	C: "c",
}

type generateSubStruct struct {
	Size uint8
}

var generateEntityIndexes = struct {
	Name IndexDefinition
}{
	Name: IndexDefinition{"Name", false},
}

func (e *generateEntity) Indexes() any {
	return generateEntityIndexes
}

type generateEntity struct {
	ID              uint64
	Age             uint32
	Balance         int8
	AgeNullable     *uint8
	BalanceNullable *int8
	Name            string `orm:"required"`
	Comment         string
	TestEnum        testGenerateEnum   `orm:"required"`
	TestSet         []testGenerateEnum `orm:"required"`
	Byte            []uint8
	Bool            bool
	BoolNullable    bool
	Float           float64
	FloatNullable   float64
	TimeNullable    *time.Time `orm:"time"`
	Time            time.Time  `orm:"time"`
	DateNullable    *time.Time
	Date            time.Time
	Reference       Reference[generateReferenceEntity]
	generateSubStruct
	TestSub generateSubStruct
}

type generateReferenceEntity struct {
	ID   uint16
	Name string
}

func TestGenerate(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), generateEntity{}, generateReferenceEntity{})
	_ = os.MkdirAll("test_output", 0755)

	f, _ := os.Create(filepath.Join("test_output", "test_file.txt"))
	_ = f.Close()

	err := Generate(orm.Engine(), "test_output")
	assert.NoError(t, err)

	_, err = os.Stat(filepath.Join("test_output", "test_file.txt"))
	assert.True(t, os.IsNotExist(err), "test_file.txt should have been deleted")

	content, err := os.ReadFile(filepath.Join("test_output", "generateEntity.go"))
	assert.NoError(t, err)
	assert.Contains(t, string(content), "package test_output\n")

	//_ = os.RemoveAll("test_output")
}
