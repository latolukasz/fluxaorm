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

//func BenchmarkGenerate(b *testing.B) {
//	b.ReportAllocs()
//	v := struct {
//		m map[string]int
//	}{m: nil}
//	for i := 0; i < b.N; i++ {
//		delete(v.m, "a")
//	}
//}

func TestGenerate(t *testing.T) {
	ctx := fluxaorm.PrepareTablesBeta(t, fluxaorm.NewRegistry(), generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{})
	_ = os.MkdirAll("entities", 0755)

	err := fluxaorm.Generate(ctx.Engine(), "entities")
	assert.NoError(t, err)

	e := entities.GenerateEntityProvider.New(ctx)
	assert.NotEmpty(t, e.GetID())
	assert.Equal(t, uint64(0), e.GetAge())
	assert.Equal(t, int64(0), e.GetBalance())
	assert.Nil(t, e.GetAgeNullable())
	assert.Nil(t, e.GetBalanceNullable())
	assert.Equal(t, "", e.GetName())
	assert.Nil(t, e.GetComment())
	assert.Equal(t, enums.TestGenerateEnum(""), e.GetTestEnum())
	assert.Nil(t, e.GetTestEnumOptional())
	assert.Nil(t, e.GetTestSet())
	assert.Nil(t, e.GetTestSetOptional())
	assert.Nil(t, e.GetByte())
	assert.False(t, e.GetBool())
	assert.Nil(t, e.GetBoolNullable())
	assert.Equal(t, float64(0), e.GetFloat())
	assert.Nil(t, e.GetFloatNullable())
	assert.Nil(t, e.GetTimeNullable())
	assert.Nil(t, e.GetDateNullable())
	assert.Equal(t, time.Time{}, e.GetTime())
	assert.Equal(t, time.Time{}, e.GetDate())
	assert.Nil(t, e.GetReferenceOptionalID())
	assert.NotNil(t, e)

	e2 := entities.GenerateEntityNoRedisProvider.New(ctx)
	assert.NotEmpty(t, e2.GetID())
	assert.Equal(t, uint64(0), e2.GetAge())
	assert.Equal(t, int64(0), e2.GetBalance())
	assert.Nil(t, e2.GetAgeNullable())
	assert.Nil(t, e2.GetBalanceNullable())
	assert.Equal(t, "", e2.GetName())
	assert.Nil(t, e2.GetComment())
	assert.Equal(t, enums.TestGenerateEnum(""), e2.GetTestEnum())
	assert.Nil(t, e2.GetTestEnumOptional())
	assert.Nil(t, e2.GetTestSet())
	assert.Nil(t, e2.GetTestSetOptional())
	assert.Nil(t, e2.GetByte())
	assert.False(t, e2.GetBool())
	assert.Nil(t, e2.GetBoolNullable())
	assert.Equal(t, float64(0), e2.GetFloat())
	assert.Nil(t, e2.GetFloatNullable())
	assert.Nil(t, e2.GetTimeNullable())
	assert.Nil(t, e2.GetDateNullable())
	assert.Equal(t, time.Time{}, e2.GetTime())
	assert.Equal(t, time.Time{}, e2.GetDate())
	assert.Nil(t, e2.GetReferenceOptionalID())
	assert.NotNil(t, e2)

	now := time.Now().UTC()
	e.SetTime(now)
	e.SetDate(now)
	e.SetTestEnum(enums.TestGenerateEnumList.A)
	e2.SetTime(now)
	e2.SetDate(now)
	e2.SetTestEnum(enums.TestGenerateEnumList.A)
	ctx.EnableQueryDebug()
	assert.NoError(t, ctx.Flush())

	id := e.GetID()
	e, found, err := entities.GenerateEntityProvider.GetByID(ctx, id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, e)
	assert.Equal(t, id, e.GetID())
	assert.Equal(t, uint64(0), e.GetAge())
	assert.Equal(t, int64(0), e.GetBalance())
	assert.Nil(t, e.GetAgeNullable())
	assert.Nil(t, e.GetBalanceNullable())
	assert.Equal(t, "", e.GetName())
	assert.Nil(t, e.GetComment())
	assert.Equal(t, enums.TestGenerateEnumList.A, e.GetTestEnum())
	assert.Nil(t, e.GetTestEnumOptional())
	assert.Nil(t, e.GetTestSet())
	assert.Nil(t, e.GetTestSetOptional())
	assert.Nil(t, e.GetByte())
	assert.False(t, e.GetBool())
	assert.Nil(t, e.GetBoolNullable())
	assert.Equal(t, float64(0), e.GetFloat())
	assert.Nil(t, e.GetFloatNullable())
	assert.Nil(t, e.GetTimeNullable())
	assert.Nil(t, e.GetDateNullable())
	assert.Equal(t, now.Truncate(time.Second).Unix(), e.GetTime().Unix())
	assert.Equal(t, now.Truncate(time.Hour*24).Unix(), e.GetDate().Unix())
	assert.Nil(t, e.GetReferenceOptionalID())

	e2, found, err = entities.GenerateEntityNoRedisProvider.GetByID(ctx, id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, e2)
	assert.Equal(t, id, e2.GetID())
	assert.Equal(t, uint64(0), e2.GetAge())
	assert.Equal(t, int64(0), e2.GetBalance())
	assert.Nil(t, e2.GetAgeNullable())
	assert.Nil(t, e2.GetBalanceNullable())
	assert.Equal(t, "", e2.GetName())
	assert.Nil(t, e2.GetComment())
	assert.Equal(t, enums.TestGenerateEnumList.A, e2.GetTestEnum())
	assert.Nil(t, e2.GetTestEnumOptional())
	assert.Nil(t, e2.GetTestSet())
	assert.Nil(t, e2.GetTestSetOptional())
	assert.Nil(t, e2.GetByte())
	assert.False(t, e2.GetBool())
	assert.Nil(t, e2.GetBoolNullable())
	assert.Equal(t, float64(0), e2.GetFloat())
	assert.Nil(t, e2.GetFloatNullable())
	assert.Nil(t, e2.GetTimeNullable())
	assert.Nil(t, e2.GetDateNullable())
	assert.Equal(t, now.Truncate(time.Second).Unix(), e2.GetTime().Unix())
	assert.Equal(t, now.Truncate(time.Hour*24).Unix(), e2.GetDate().Unix())
	assert.Nil(t, e2.GetReferenceOptionalID())
}
