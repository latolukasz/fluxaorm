package test_generate

import (
	"os"
	"testing"
	"time"

	"github.com/latolukasz/fluxaorm"
	"github.com/latolukasz/fluxaorm/test_generate/entities"
	"github.com/latolukasz/fluxaorm/test_generate/entities/enums"
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
	Name       fluxaorm.IndexDefinition
	AgeBalance fluxaorm.UniqueIndexDefinition
}{
	Name:       fluxaorm.IndexDefinition{"Name", false},
	AgeBalance: fluxaorm.UniqueIndexDefinition{"Age,Balance", false},
}

func (e *generateEntity) Indexes() any {
	return generateEntityIndexes
}

type generateEntity struct {
	ID                uint64 `orm:"redisCache"`
	Age               uint32
	Balance           int8
	AgeNullable       *uint8
	BalanceNullable   *int8
	Name              string `orm:"required"`
	Comment           string
	TestEnum          testGenerateEnum `orm:"required"`
	TestEnumOptional  testGenerateEnum
	TestSet           []testGenerateEnum `orm:"required"`
	TestSetOptional   []testGenerateEnum
	Byte              []uint8
	Bool              bool
	BoolNullable      *bool
	Float             float64
	FloatNullable     *float64
	TimeNullable      *time.Time `orm:"time"`
	Time              time.Time  `orm:"time"`
	DateNullable      *time.Time
	Date              time.Time
	ReferenceRequired fluxaorm.Reference[generateReferenceEntity] `orm:"required"`
	ReferenceOptional fluxaorm.Reference[generateReferenceEntity]
	generateSubStruct
	TestSub generateSubStruct
}

type generateEntityNoRedis struct {
	ID                uint64
	Age               uint32
	Balance           int8
	AgeNullable       *uint8
	BalanceNullable   *int8
	Name              string `orm:"required"`
	Comment           string
	TestEnum          testGenerateEnum `orm:"required"`
	TestEnumOptional  testGenerateEnum
	TestSet           []testGenerateEnum `orm:"required"`
	TestSetOptional   []testGenerateEnum
	Byte              []uint8
	Bool              bool
	BoolNullable      *bool
	Float             float64
	FloatNullable     *float64
	TimeNullable      *time.Time `orm:"time"`
	Time              time.Time  `orm:"time"`
	DateNullable      *time.Time
	Date              time.Time
	ReferenceRequired fluxaorm.Reference[generateReferenceEntity] `orm:"required"`
	ReferenceOptional fluxaorm.Reference[generateReferenceEntity]
	generateSubStruct
	TestSub generateSubStruct
}

type generateReferenceEntity struct {
	ID   uint16
	Name string
}

func TestGenerate(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{})
	_ = os.MkdirAll("entities", 0755)

	err := fluxaorm.Generate(ctx.Engine(), "entities")
	assert.NoError(t, err)

	e := entities.GenerateEntityNoRedisProvider.New(ctx)
	assert.NotEmpty(t, e.GetID())
	assert.Equal(t, uint64(0), e.GetAge())
	assert.Equal(t, int64(0), e.GetBalance())
	assert.Nil(t, e.GetAgeNullable())
	assert.Nil(t, e.GetBalanceNullable())
	assert.Equal(t, "", e.GetName())
	assert.Equal(t, "", e.GetComment())
	assert.Equal(t, enums.TestGenerateEnumList.A, e.GetTestEnum())
	assert.Equal(t, enums.TestGenerateEnum(""), e.GetTestEnumOptional())
	assert.Equal(t, []enums.TestGenerateEnum{enums.TestGenerateEnumList.A}, e.GetTestSet())
	assert.Nil(t, e.GetTestSetOptional())
	assert.Nil(t, e.GetByte())
	assert.False(t, e.GetBool())
	assert.Nil(t, e.GetBoolNullable())
	assert.Equal(t, float64(0), e.GetFloat())
	assert.Nil(t, e.GetFloatNullable())
	assert.Nil(t, e.GetTimeNullable())
	assert.Nil(t, e.GetDateNullable())
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), e.GetTime())
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), e.GetDate())
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
	assert.Nil(t, e.GetReferenceOptionalID())
	assert.NotNil(t, e)

	ctx.EnableQueryDebug()
	assert.NoError(t, ctx.Flush())

	e, found, err := entities.GenerateEntityNoRedisProvider.GetByID(ctx, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
}
