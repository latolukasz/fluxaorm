package test_generate

import (
	"os"
	"testing"
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/latolukasz/fluxaorm/v2/test_generate/models"
	"github.com/stretchr/testify/assert"
)

type generateSubStruct struct {
	Size uint8
}

type generateEntity struct {
	ID                uint64 `orm:"redisCache"`
	Age               uint32
	Balance           int8
	AgeNullable       *uint8
	BalanceNullable   *int8
	Name              string `orm:"required"`
	Comment           string
	TestEnum          string `orm:"enum=a,b,c;required;enumName=TestEnum"`
	TestEnumOptional  string `orm:"enum=a,b,c;enumName=TestEnum"`
	TestSet           string `orm:"set=a,b,c;required;enumName=TestEnum"`
	TestSetOptional   string `orm:"set=a,b,c;enumName=TestEnum"`
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
	Tags              fluxaorm.References[generateReferenceEntity] `orm:"required"`
	TagsOptional      fluxaorm.References[generateReferenceEntity]
	generateSubStruct
	TestSub     generateSubStruct
	JsonAddress *models.GenerateJsonAddress
}

type generateEntityNoRedis struct {
	ID                uint64
	Age               uint32
	Balance           int8
	AgeNullable       *uint8
	BalanceNullable   *int8
	Name              string `orm:"required"`
	Comment           string
	TestEnum          string `orm:"enum=a,b,c;required;enumName=TestEnum"`
	TestEnumOptional  string `orm:"enum=a,b,c;enumName=TestEnum"`
	TestSet           string `orm:"set=a,b,c;required;enumName=TestEnum"`
	TestSetOptional   string `orm:"set=a,b,c;enumName=TestEnum"`
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
	Tags              fluxaorm.References[generateReferenceEntity] `orm:"required"`
	TagsOptional      fluxaorm.References[generateReferenceEntity]
	generateSubStruct
	TestSub     generateSubStruct
	JsonAddress *models.GenerateJsonAddress
}

type generateReferenceEntity struct {
	ID         uint16
	Name       string
	FakeDelete bool
}

type generateEntityWithSearch struct {
	ID    uint64
	Age   uint32  `orm:"searchable;sortable"`
	Name  string  `orm:"required;searchable"`
	Score float64 `orm:"searchable"`
}

type generateEntityWithTimestamps struct {
	ID        uint64
	Name      string `orm:"required"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type generateEntityWithTimestampsRedis struct {
	ID        uint64 `orm:"redisCache"`
	Name      string `orm:"required"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type generateEntityCachedUnique struct {
	ID    uint64 `orm:"redisCache"`
	Name  string
	Age   uint8
	Email string
}

type generateEntityCachedUniqueNoRedis struct {
	ID    uint64
	Code  string
	Value int32
}

type generateEntityCachedUniqueFakeDelete struct {
	ID         uint64 `orm:"redisCache"`
	FakeDelete bool
	Name       string
}

type generateEntityWithIndex struct {
	ID   uint64
	Age  uint32
	Name string `orm:"required"`
}

func (e generateEntityWithIndex) Indexes() [][]string {
	return [][]string{{"Age"}, {"Name", "Age"}}
}

type generateEntityEnumRef struct {
	ID     uint64
	Status string `orm:"enumName=TestEnum"`
}

type generateEntityDebezium struct {
	ID   uint64 `orm:"debezium=kafka"`
	Name string `orm:"required;length=100"`
	Age  uint16
}

func (e generateEntity) UniqueIndexes() [][]string {
	return [][]string{{"Age", "Balance"}}
}

func (e generateEntityCachedUnique) UniqueIndexes() [][]string {
	return [][]string{
		{"Name", "Age"},
		{"Email"},
	}
}

func (e generateEntityCachedUnique) CachedUniqueIndexes() [][]string {
	return [][]string{
		{"Name", "Age"},
		{"Email"},
	}
}

func (e generateEntityCachedUniqueNoRedis) UniqueIndexes() [][]string {
	return [][]string{{"Code", "Value"}}
}

func (e generateEntityCachedUniqueNoRedis) CachedUniqueIndexes() [][]string {
	return [][]string{{"Code", "Value"}}
}

func (e generateEntityCachedUniqueFakeDelete) UniqueIndexes() [][]string {
	return [][]string{{"Name"}}
}

func (e generateEntityCachedUniqueFakeDelete) CachedUniqueIndexes() [][]string {
	return [][]string{{"Name"}}
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
	ctx := fluxaorm.PrepareTablesWithDebezium(t, fluxaorm.NewRegistry(), generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{}, generateEntityWithSearch{}, generateEntityWithTimestamps{}, generateEntityWithTimestampsRedis{}, generateEntityCachedUnique{}, generateEntityCachedUniqueNoRedis{}, generateEntityCachedUniqueFakeDelete{}, generateEntityWithIndex{}, generateEntityEnumRef{}, generateEntityDebezium{})
	defer ctx.Engine().Kafka("kafka").Close()
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
	assert.Equal(t, "", e.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e.GetTestEnum())
	assert.True(t, enums.TestEnumList.A.Valid())
	assert.True(t, enums.TestEnumList.B.Valid())
	assert.True(t, enums.TestEnumList.C.Valid())
	assert.False(t, enums.TestEnum("invalid").Valid())
	assert.False(t, enums.TestEnum("").Valid())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.B, enums.TestEnumList.C}, enums.TestEnumList.A.Values())
	assert.Nil(t, e.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e.GetTestSet())
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
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
	assert.Equal(t, uint64(0), e.GetReferenceOptionalID())
	assert.Nil(t, e.GetJsonAddress())
	assert.NotNil(t, e)

	e2 := entities.GenerateEntityNoRedisProvider.New(ctx)
	assert.NotEmpty(t, e2.GetID())
	assert.Equal(t, uint64(0), e2.GetAge())
	assert.Equal(t, int64(0), e2.GetBalance())
	assert.Nil(t, e2.GetAgeNullable())
	assert.Nil(t, e2.GetBalanceNullable())
	assert.Equal(t, "", e2.GetName())
	assert.Equal(t, "", e2.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e2.GetTestEnum())
	assert.Nil(t, e2.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e2.GetTestSet())
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
	assert.Equal(t, uint64(0), e2.GetReferenceRequiredID())
	assert.Equal(t, uint64(0), e2.GetReferenceOptionalID())
	assert.Nil(t, e2.GetJsonAddress())
	assert.NotNil(t, e2)

	now := time.Now().UTC()
	e.SetTime(now)
	e.SetDate(now)
	e.SetTestEnum(enums.TestEnumList.A)
	e2.SetTime(now)
	e2.SetDate(now)
	e2.SetTestEnum(enums.TestEnumList.A)
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
	assert.Equal(t, "", e.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e.GetTestEnum())
	assert.Nil(t, e.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e.GetTestSet())
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
	assert.Equal(t, uint64(0), e.GetReferenceOptionalID())
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
	assert.Nil(t, e.GetJsonAddress())

	id = e2.GetID()
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
	assert.Equal(t, "", e2.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e2.GetTestEnum())
	assert.Nil(t, e2.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e2.GetTestSet())
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
	assert.Equal(t, uint64(0), e2.GetReferenceOptionalID())
	assert.Equal(t, uint64(0), e2.GetReferenceRequiredID())
	assert.Nil(t, e2.GetJsonAddress())

	e.SetAge(0)
	e2.SetAge(0)
	e.SetBalance(0)
	e2.SetBalance(0)
	e.SetBalanceNullable(nil)
	e2.SetBalanceNullable(nil)
	e.SetName("")
	e2.SetName("")
	e.SetComment("")
	e2.SetComment("")
	e.SetTestEnum(enums.TestEnumList.A)
	e2.SetTestEnum(enums.TestEnumList.A)
	e.SetTestEnumOptional(nil)
	e2.SetTestEnumOptional(nil)
	e.SetTestSet(enums.TestEnumList.A)
	e2.SetTestSet(enums.TestEnumList.A)
	e.SetTestSetOptional()
	e2.SetTestSetOptional()
	e.SetByte(nil)
	e2.SetByte(nil)
	e.SetBool(false)
	e2.SetBool(false)
	e.SetBoolNullable(nil)
	e2.SetBoolNullable(nil)
	e.SetFloat(0)
	e2.SetFloat(0)
	e.SetFloatNullable(nil)
	e2.SetFloatNullable(nil)
	e.SetTimeNullable(nil)
	e2.SetTimeNullable(nil)
	e.SetDateNullable(nil)
	e2.SetDateNullable(nil)
	e.SetTime(now)
	e2.SetTime(now)
	e.SetDate(now)
	e2.SetDate(now)
	e.SetReferenceRequired(0)
	e2.SetReferenceRequired(0)
	e.SetReferenceOptional(0)
	e2.SetReferenceOptional(0)

	assert.Equal(t, uint64(0), e.GetAge())
	assert.Equal(t, int64(0), e.GetBalance())
	assert.Nil(t, e.GetAgeNullable())
	assert.Nil(t, e.GetBalanceNullable())
	assert.Equal(t, "", e.GetName())
	assert.Equal(t, "", e.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e.GetTestEnum())
	assert.Nil(t, e.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e.GetTestSet())
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
	assert.Equal(t, uint64(0), e.GetReferenceOptionalID())
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
	assert.Equal(t, uint64(0), e2.GetAge())
	assert.Equal(t, int64(0), e2.GetBalance())
	assert.Nil(t, e2.GetAgeNullable())
	assert.Nil(t, e2.GetBalanceNullable())
	assert.Equal(t, "", e2.GetName())
	assert.Equal(t, "", e2.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e2.GetTestEnum())
	assert.Nil(t, e2.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A}, e2.GetTestSet())
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
	assert.Equal(t, uint64(0), e2.GetReferenceOptionalID())
	assert.Equal(t, uint64(0), e2.GetReferenceRequiredID())
	assert.NoError(t, ctx.Flush())

	e.SetAge(1)
	e2.SetAge(1)
	e.SetBalance(2)
	e2.SetBalance(2)
	uint64Nullable := uint64(7)
	e.SetAgeNullable(&uint64Nullable)
	e2.SetAgeNullable(&uint64Nullable)
	int64Nullable := int64(3)
	e.SetBalanceNullable(&int64Nullable)
	e2.SetBalanceNullable(&int64Nullable)
	e.SetName("Hello")
	e2.SetName("Hello")
	e.SetComment("Test comment")
	e2.SetComment("Test comment")
	e.SetTestEnum(enums.TestEnumList.B)
	e2.SetTestEnum(enums.TestEnumList.B)
	e.SetTestEnumOptional(&enums.TestEnumList.B)
	e2.SetTestEnumOptional(&enums.TestEnumList.B)
	e.SetTestSet(enums.TestEnumList.A, enums.TestEnumList.B)
	e2.SetTestSet(enums.TestEnumList.A, enums.TestEnumList.B)
	e.SetTestSetOptional(enums.TestEnumList.A, enums.TestEnumList.B)
	e2.SetTestSetOptional(enums.TestEnumList.A, enums.TestEnumList.B)
	e.SetByte([]uint8("hello"))
	e2.SetByte([]uint8("hello"))
	e.SetBool(true)
	e2.SetBool(true)
	bValue := true
	e.SetBoolNullable(&bValue)
	e2.SetBoolNullable(&bValue)
	e.SetFloat(12.3)
	e2.SetFloat(12.3)
	fValue := 12.3
	e.SetFloatNullable(&fValue)
	e2.SetFloatNullable(&fValue)
	e.SetTimeNullable(&now)
	e2.SetTimeNullable(&now)
	e.SetDateNullable(&now)
	e2.SetDateNullable(&now)
	now2 := now.Add(time.Hour * 24)
	e.SetTime(now2)
	e2.SetTime(now2)
	e.SetDate(now2)
	e2.SetDate(now2)
	ref := entities.GenerateReferenceEntityProvider.New(ctx)
	ref.SetName("Test Reference")
	e.SetReferenceRequired(ref.GetID())
	e2.SetReferenceRequired(ref.GetID())
	e.SetReferenceOptional(ref.GetID())
	e2.SetReferenceOptional(ref.GetID())
	jsonAddr := &models.GenerateJsonAddress{Street: "123 Main St", City: "Springfield", Zip: "62701"}
	e.SetJsonAddress(jsonAddr)
	e2.SetJsonAddress(jsonAddr)
	assert.NoError(t, ctx.Flush())

	e, found, err = entities.GenerateEntityProvider.GetByID(ctx, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	e2, found, err = entities.GenerateEntityNoRedisProvider.GetByID(ctx, e2.GetID())
	assert.NoError(t, err)
	assert.True(t, found)

	assert.Equal(t, uint64(1), e.GetAge())
	assert.Equal(t, uint64(1), e2.GetAge())
	assert.Equal(t, int64(2), e.GetBalance())
	assert.Equal(t, int64(2), e2.GetBalance())
	assert.Equal(t, uint64Nullable, *e.GetAgeNullable())
	assert.Equal(t, uint64Nullable, *e2.GetAgeNullable())
	assert.Equal(t, int64Nullable, *e.GetBalanceNullable())
	assert.Equal(t, int64Nullable, *e2.GetBalanceNullable())
	assert.Equal(t, "Hello", e.GetName())
	assert.Equal(t, "Hello", e2.GetName())
	assert.Equal(t, "Test comment", e.GetComment())
	assert.Equal(t, "Test comment", e2.GetComment())
	assert.Equal(t, enums.TestEnumList.B, e.GetTestEnum())
	assert.Equal(t, enums.TestEnumList.B, e2.GetTestEnum())
	assert.Equal(t, enums.TestEnumList.B, *e.GetTestEnumOptional())
	assert.Equal(t, enums.TestEnumList.B, *e2.GetTestEnumOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.B}, e.GetTestSet())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.B}, e2.GetTestSet())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.B}, e.GetTestSetOptional())
	assert.Equal(t, []enums.TestEnum{enums.TestEnumList.A, enums.TestEnumList.B}, e2.GetTestSetOptional())
	assert.Equal(t, []uint8("hello"), e.GetByte())
	assert.Equal(t, []uint8("hello"), e2.GetByte())
	assert.True(t, e.GetBool())
	assert.True(t, e2.GetBool())
	assert.True(t, *e.GetBoolNullable())
	assert.True(t, *e2.GetBoolNullable())
	assert.Equal(t, 12.3, e.GetFloat())
	assert.Equal(t, 12.3, e2.GetFloat())
	assert.Equal(t, 12.3, *e.GetFloatNullable())
	assert.Equal(t, 12.3, *e2.GetFloatNullable())
	assert.Equal(t, now.Truncate(time.Second), *e.GetTimeNullable())
	assert.Equal(t, now.Truncate(time.Second), *e2.GetTimeNullable())
	assert.Equal(t, now.Truncate(time.Hour*24), *e.GetDateNullable())
	assert.Equal(t, now.Truncate(time.Hour*24), *e2.GetDateNullable())
	assert.Equal(t, now2.Truncate(time.Second), e.GetTime())
	assert.Equal(t, now2.Truncate(time.Second), e2.GetTime())
	assert.Equal(t, now2.Truncate(time.Hour*24), e.GetDate())
	assert.Equal(t, now2.Truncate(time.Hour*24), e2.GetDate())
	assert.Equal(t, ref.GetID(), e.GetReferenceRequiredID())
	assert.Equal(t, ref.GetID(), e2.GetReferenceRequiredID())
	assert.Equal(t, ref.GetID(), e.GetReferenceOptionalID())
	assert.Equal(t, ref.GetID(), e2.GetReferenceOptionalID())
	assert.Equal(t, jsonAddr, e.GetJsonAddress())
	assert.Equal(t, jsonAddr, e2.GetJsonAddress())

	e.SetAge(1)
	e2.SetAge(1)
	e.SetBalance(2)
	e2.SetBalance(2)
	e.SetAgeNullable(&uint64Nullable)
	e2.SetAgeNullable(&uint64Nullable)
	e.SetBalanceNullable(&int64Nullable)
	e2.SetBalanceNullable(&int64Nullable)
	e.SetName("Hello")
	e2.SetName("Hello")
	e.SetComment("Test comment")
	e2.SetComment("Test comment")
	e.SetTestEnum(enums.TestEnumList.B)
	e2.SetTestEnum(enums.TestEnumList.B)
	e.SetTestEnumOptional(&enums.TestEnumList.B)
	e2.SetTestEnumOptional(&enums.TestEnumList.B)
	e.SetTestSet(enums.TestEnumList.A, enums.TestEnumList.B)
	e2.SetTestSet(enums.TestEnumList.A, enums.TestEnumList.B)
	e.SetTestSetOptional(enums.TestEnumList.A, enums.TestEnumList.B)
	e2.SetTestSetOptional(enums.TestEnumList.A, enums.TestEnumList.B)
	e.SetByte([]uint8("hello"))
	e2.SetByte([]uint8("hello"))
	e.SetBool(true)
	e2.SetBool(true)
	e.SetBoolNullable(&bValue)
	e2.SetBoolNullable(&bValue)
	e.SetFloat(12.3)
	e2.SetFloat(12.3)
	e.SetFloatNullable(&fValue)
	e2.SetFloatNullable(&fValue)
	e.SetTimeNullable(&now)
	e2.SetTimeNullable(&now)
	e.SetDateNullable(&now)
	e2.SetDateNullable(&now)
	e.SetTime(now2)
	e2.SetTime(now2)
	e.SetDate(now2)
	e2.SetDate(now2)
	ref.SetName("Test Reference")
	e.SetReferenceRequired(ref.GetID())
	e2.SetReferenceRequired(ref.GetID())
	e.SetReferenceOptional(ref.GetID())
	e2.SetReferenceOptional(ref.GetID())
	e.SetJsonAddress(jsonAddr)
	e2.SetJsonAddress(jsonAddr)
	assert.NoError(t, ctx.Flush())

	// Set JsonAddress back to nil
	e.SetJsonAddress(nil)
	e2.SetJsonAddress(nil)
	assert.NoError(t, ctx.Flush())
	e, found, err = entities.GenerateEntityProvider.GetByID(ctx, e.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Nil(t, e.GetJsonAddress())
	e2, found, err = entities.GenerateEntityNoRedisProvider.GetByID(ctx, e2.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Nil(t, e2.GetJsonAddress())

	// SearchMany: generateEntityNoRedis (no FakeDelete)
	var total int
	var searchResults2 []*entities.GenerateEntityNoRedis
	searchResults2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.Len(t, searchResults2, 1)
	assert.Equal(t, e2.GetID(), searchResults2[0].GetID())

	searchResults2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("Hello")))
	assert.NoError(t, err)
	assert.Len(t, searchResults2, 1)
	assert.Equal(t, e2.GetID(), searchResults2[0].GetID())

	searchResults2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("NoMatch")))
	assert.NoError(t, err)
	assert.Nil(t, searchResults2)

	searchResults2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Len(t, searchResults2, 1)
	assert.Equal(t, e2.GetID(), searchResults2[0].GetID())

	// SearchManyWithTotal: generateEntityNoRedis
	searchResults2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, searchResults2, 1)
	assert.Equal(t, e2.GetID(), searchResults2[0].GetID())

	searchResults2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("NoMatch")).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Nil(t, searchResults2)

	searchResults2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(2, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Nil(t, searchResults2)

	// SearchMany and SearchManyWithTotal: generateReferenceEntity (FakeDelete)
	ref2 := entities.GenerateReferenceEntityProvider.New(ctx)
	ref2.SetName("Test Reference 2")
	assert.NoError(t, ctx.Flush())

	var listRefSearch []*entities.GenerateReferenceEntity
	listRefSearch, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.Len(t, listRefSearch, 2)

	listRefSearch, total, err = entities.GenerateReferenceEntityProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, listRefSearch, 2)

	ref.Delete()
	assert.NoError(t, ctx.Flush())

	listRefSearch, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.Len(t, listRefSearch, 1)
	assert.Equal(t, ref2.GetID(), listRefSearch[0].GetID())

	listRefSearch, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("1 = 1").WithFakeDeletes()))
	assert.NoError(t, err)
	assert.Len(t, listRefSearch, 2)

	listRefSearch, total, err = entities.GenerateReferenceEntityProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, listRefSearch, 1)
	assert.Equal(t, ref2.GetID(), listRefSearch[0].GetID())

	listRefSearch, total, err = entities.GenerateReferenceEntityProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("1 = 1").WithFakeDeletes()).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, listRefSearch, 2)

	// SearchMany (entity return): generateEntityNoRedis (no FakeDelete)
	var list2 []*entities.GenerateEntityNoRedis
	list2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())
	assert.Equal(t, "Hello", list2[0].GetName())

	list2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("Hello")))
	assert.NoError(t, err)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())

	list2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("NoMatch")))
	assert.NoError(t, err)
	assert.Nil(t, list2)

	list2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Len(t, list2, 1)

	list2, err = entities.GenerateEntityNoRedisProvider.SearchMany(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(2, 10)))
	assert.NoError(t, err)
	assert.Nil(t, list2)

	// SearchManyWithTotal (entity return): generateEntityNoRedis (no FakeDelete)
	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())

	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("NoMatch")).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Nil(t, list2)

	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(2, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Nil(t, list2)

	// SearchMany: generateReferenceEntity (FakeDelete) — ref soft-deleted, ref2 active
	var listRef []*entities.GenerateReferenceEntity
	listRef, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref2.GetID(), listRef[0].GetID())

	listRef, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("1 = 1").WithFakeDeletes()))
	assert.NoError(t, err)
	assert.Len(t, listRef, 2)

	listRef, err = entities.GenerateReferenceEntityProvider.SearchMany(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("`Name` = ?", "Test Reference").WithFakeDeletes()))
	assert.NoError(t, err)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref.GetID(), listRef[0].GetID())

	// SearchManyWithTotal: generateReferenceEntity (FakeDelete)
	listRef, total, err = entities.GenerateReferenceEntityProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref2.GetID(), listRef[0].GetID())

	listRef, total, err = entities.GenerateReferenceEntityProvider.SearchManyWithTotal(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("1 = 1").WithFakeDeletes()).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, listRef, 2)

	// SearchOne: generateEntityNoRedis (no FakeDelete)
	var one *entities.GenerateEntityNoRedis
	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("Hello")))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, e2.GetID(), one.GetID())

	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(entities.GenerateEntityNoRedisProvider.Fields.Name.Is("NoMatch")))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, one)

	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, one)

	// SearchOne: generateReferenceEntity (FakeDelete) — ref is soft-deleted, ref2 is active
	var oneRef *entities.GenerateReferenceEntity
	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(entities.GenerateReferenceEntityProvider.Fields.Name.Is("Test Reference")))
	assert.NoError(t, err)
	assert.False(t, found) // soft-deleted, filtered out
	assert.Nil(t, oneRef)

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewQuery().FilterWhere(fluxaorm.NewWhere("`Name` = ?", "Test Reference").WithFakeDeletes()))
	assert.NoError(t, err)
	assert.True(t, found) // WithFakeDeletes bypasses filter
	assert.Equal(t, ref.GetID(), oneRef.GetID())

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(entities.GenerateReferenceEntityProvider.Fields.Name.Is("Test Reference 2")))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, ref2.GetID(), oneRef.GetID())

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewQuery())
	assert.NoError(t, err)
	assert.True(t, found) // returns one active row
	assert.Equal(t, ref2.GetID(), oneRef.GetID())

	e.Delete()
	e2.Delete()
	assert.NoError(t, ctx.Flush())
	e, found, err = entities.GenerateEntityProvider.GetByID(ctx, e.GetID())
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, e)
	e2, found, err = entities.GenerateEntityNoRedisProvider.GetByID(ctx, e2.GetID())
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, e2)

	// Redis Search tests
	alters, err := fluxaorm.GetRedisSearchAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 1)
	assert.NoError(t, alters[0].Exec(ctx))

	// Second call: index already exists, no alters returned
	alters, err = fluxaorm.GetRedisSearchAlters(ctx)
	assert.NoError(t, err)
	assert.Len(t, alters, 0)

	es1 := entities.GenerateEntityWithSearchProvider.New(ctx)
	es1.SetAge(10)
	es1.SetName("Alice")
	es1.SetScore(1.5)
	es2 := entities.GenerateEntityWithSearchProvider.New(ctx)
	es2.SetAge(20)
	es2.SetName("Bob")
	es2.SetScore(2.5)
	es3 := entities.GenerateEntityWithSearchProvider.New(ctx)
	es3.SetAge(30)
	es3.SetName("Charlie")
	es3.SetScore(3.5)
	assert.NoError(t, ctx.Flush())

	// SearchManyInRedis: all entities
	var searchEntities []*entities.GenerateEntityWithSearch
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery())
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 3)

	// SearchManyInRedis: numeric range
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Gte(10),
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Lte(20),
	))
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 2)

	// SearchManyInRedisWithTotal
	var searchTotal int
	searchEntities, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedisWithTotal(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Gte(20),
	).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 2, searchTotal)
	assert.Len(t, searchEntities, 2)

	// SearchManyInRedisWithTotal: no match
	searchEntities, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedisWithTotal(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(999),
	).Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 0, searchTotal)
	assert.Nil(t, searchEntities)

	// SearchManyInRedis: returns entities with filter
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Lte(20),
	))
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 2)

	// SearchOneInRedis
	var searchOne *entities.GenerateEntityWithSearch
	searchOne, found, err = entities.GenerateEntityWithSearchProvider.SearchOneInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(10),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, es1.GetID(), searchOne.GetID())

	searchOne, found, err = entities.GenerateEntityWithSearchProvider.SearchOneInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(999),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, searchOne)

	// SearchManyInRedisWithTotal: all
	var searchWithTotal []*entities.GenerateEntityWithSearch
	searchWithTotal, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedisWithTotal(ctx, fluxaorm.NewRedisSearchQuery().Pager(fluxaorm.NewPager(1, 10)))
	assert.NoError(t, err)
	assert.Equal(t, 3, searchTotal)
	assert.Len(t, searchWithTotal, 3)

	// Update searchable field: es1 Age 10 → 15
	es1.SetAge(15)
	assert.NoError(t, ctx.Flush())

	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(10),
	))
	assert.NoError(t, err)
	assert.Nil(t, searchEntities)

	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(15),
	))
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 1)
	assert.Equal(t, es1.GetID(), searchEntities[0].GetID())

	// Delete entity: should be removed from Redis Search
	es3.Delete()
	assert.NoError(t, ctx.Flush())

	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery())
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 2)

	// ReindexRedisSearch: rebuild the entire index from MySQL; results should remain correct
	err = entities.GenerateEntityWithSearchProvider.ReindexRedisSearch(ctx)
	assert.NoError(t, err)
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery())
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 2)
	// After reindex, updated age=15 for es1 should be searchable
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchManyInRedis(ctx, fluxaorm.NewRedisSearchQuery().Filter(
		entities.GenerateEntityWithSearchProvider.FieldsRedisSearch.Age.Eq(15),
	))
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 1)
	assert.Equal(t, es1.GetID(), searchEntities[0].GetID())

	// ---- Timestamp auto-set tests (no Redis cache) ----
	beforeInsert := time.Now().UTC().Truncate(time.Second)
	ts1 := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	ts1.SetName("TimestampTest")
	assert.NoError(t, ctx.Flush())
	afterInsert := time.Now().UTC().Truncate(time.Second).Add(time.Second)

	ts1, found, err = entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, ts1.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	// CreatedAt auto-set on INSERT
	assert.False(t, ts1.GetCreatedAt().IsZero())
	assert.True(t, !ts1.GetCreatedAt().Before(beforeInsert))
	assert.True(t, ts1.GetCreatedAt().Before(afterInsert))
	// UpdatedAt auto-set on INSERT
	assert.False(t, ts1.GetUpdatedAt().IsZero())
	assert.True(t, !ts1.GetUpdatedAt().Before(beforeInsert))
	assert.True(t, ts1.GetUpdatedAt().Before(afterInsert))

	origCreatedAt := ts1.GetCreatedAt()
	origUpdatedAt := ts1.GetUpdatedAt()

	// UPDATE: change Name, UpdatedAt should auto-set, CreatedAt unchanged
	time.Sleep(time.Second)
	ts1.SetName("TimestampTestUpdated")
	assert.NoError(t, ctx.Flush())

	ts1, found, err = entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, ts1.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, origCreatedAt, ts1.GetCreatedAt())
	assert.True(t, ts1.GetUpdatedAt().After(origUpdatedAt))

	// INSERT with explicit CreatedAt: should preserve user value
	customTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := entities.GenerateEntityWithTimestampsProvider.New(ctx)
	ts2.SetName("CustomCreatedAt")
	ts2.SetCreatedAt(customTime)
	assert.NoError(t, ctx.Flush())

	ts2, found, err = entities.GenerateEntityWithTimestampsProvider.GetByID(ctx, ts2.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, customTime, ts2.GetCreatedAt())
	assert.False(t, ts2.GetUpdatedAt().IsZero()) // auto-set

	// ---- Timestamp auto-set tests (with Redis cache) ----
	beforeInsertR := time.Now().UTC().Truncate(time.Second)
	tsr1 := entities.GenerateEntityWithTimestampsRedisProvider.New(ctx)
	tsr1.SetName("TimestampRedisTest")
	assert.NoError(t, ctx.Flush())
	afterInsertR := time.Now().UTC().Truncate(time.Second).Add(time.Second)

	tsr1, found, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(ctx, tsr1.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.False(t, tsr1.GetCreatedAt().IsZero())
	assert.True(t, !tsr1.GetCreatedAt().Before(beforeInsertR))
	assert.True(t, tsr1.GetCreatedAt().Before(afterInsertR))
	assert.False(t, tsr1.GetUpdatedAt().IsZero())

	origCreatedAtR := tsr1.GetCreatedAt()
	origUpdatedAtR := tsr1.GetUpdatedAt()

	time.Sleep(time.Second)
	tsr1.SetName("TimestampRedisTestUpdated")
	assert.NoError(t, ctx.Flush())

	tsr1, found, err = entities.GenerateEntityWithTimestampsRedisProvider.GetByID(ctx, tsr1.GetID())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, origCreatedAtR, tsr1.GetCreatedAt())
	assert.True(t, tsr1.GetUpdatedAt().After(origUpdatedAtR))

	// Test non-cached unique index getter (AgeBalance on generateEntity)
	ctx.DisableContextCache()
	eIdx := entities.GenerateEntityProvider.New(ctx)
	eIdx.SetAge(25)
	eIdx.SetBalance(10)
	eIdx.SetTime(now)
	eIdx.SetDate(now)
	eIdx.SetTestEnum(enums.TestEnumList.A)
	assert.NoError(t, ctx.Flush())
	eByIdx, found, err := entities.GenerateEntityProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityProvider.Fields.Age.Eq(uint64(25)),
		entities.GenerateEntityProvider.Fields.Balance.Eq(int64(10)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, eIdx.GetID(), eByIdx.GetID())
	assert.Equal(t, uint64(25), eByIdx.GetAge())
	assert.Equal(t, int64(10), eByIdx.GetBalance())
	// Not found case
	eByIdx, found, err = entities.GenerateEntityProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityProvider.Fields.Age.Eq(uint64(999)),
		entities.GenerateEntityProvider.Fields.Balance.Eq(int64(999)),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, eByIdx)

	// Test cached unique index getter (generateEntityCachedUnique)
	cu := entities.GenerateEntityCachedUniqueProvider.New(ctx)
	cu.SetName("Alice")
	cu.SetAge(30)
	cu.SetEmail("alice@example.com")
	assert.NoError(t, ctx.Flush())

	// First call: should go to MySQL, cache in Redis, then GetByID
	cuByName, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Alice"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cu.GetID(), cuByName.GetID())
	// Second call: should hit Redis cache
	cuByName2, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Alice"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cu.GetID(), cuByName2.GetID())
	// Not found case
	cuByName3, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Nobody"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(0)),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, cuByName3)

	// Email single-column cached index
	cuByEmail, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Email.Is("alice@example.com"),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cu.GetID(), cuByEmail.GetID())

	// Test INSERT populates cached unique index key (already tested above via getter)
	// Test UPDATE with changed index column: change Name and verify new lookup works
	cuByName, found, err = entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Alice"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	cuByName.SetName("Bob")
	assert.NoError(t, ctx.Flush())
	// Old key should no longer work
	cuOld, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Alice"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, cuOld)
	// New key should work
	cuNew, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Bob"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cu.GetID(), cuNew.GetID())

	// Test DELETE removes cached unique index key
	cuNew.Delete()
	assert.NoError(t, ctx.Flush())
	cuDel, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Bob"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(30)),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, cuDel)
	cuDelEmail, found, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Email.Is("alice@example.com"),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, cuDelEmail)

	// Test cached unique index without Redis entity cache (generateEntityCachedUniqueNoRedis)
	cunr := entities.GenerateEntityCachedUniqueNoRedisProvider.New(ctx)
	cunr.SetCode("test123")
	cunr.SetValue(42)
	assert.NoError(t, ctx.Flush())
	cunrByCode, found, err := entities.GenerateEntityCachedUniqueNoRedisProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueNoRedisProvider.Fields.Code.Is("test123"),
		entities.GenerateEntityCachedUniqueNoRedisProvider.Fields.Value.Eq(int64(42)),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cunr.GetID(), cunrByCode.GetID())

	// Test FakeDelete removes cached unique index key
	cufd := entities.GenerateEntityCachedUniqueFakeDeleteProvider.New(ctx)
	cufd.SetName("FakeDeleteTest")
	assert.NoError(t, ctx.Flush())
	cufdByName, found, err := entities.GenerateEntityCachedUniqueFakeDeleteProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueFakeDeleteProvider.Fields.Name.Is("FakeDeleteTest"),
	))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cufd.GetID(), cufdByName.GetID())
	// Soft delete
	cufdByName.Delete()
	assert.NoError(t, ctx.Flush())
	// Cached key should be removed, MySQL query filters by FakeDelete=0
	cufdAfterDelete, found, err := entities.GenerateEntityCachedUniqueFakeDeleteProvider.SearchOne(ctx, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueFakeDeleteProvider.Fields.Name.Is("FakeDeleteTest"),
	))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, cufdAfterDelete)

	// ---- enumName-only reference tests ----
	eRef := entities.GenerateEntityEnumRefProvider.New(ctx)
	assert.Nil(t, eRef.GetStatus())
	val := enums.TestEnumList.A
	eRef.SetStatus(&val)
	assert.Equal(t, enums.TestEnumList.A, *eRef.GetStatus())
	assert.NoError(t, ctx.Flush())
	eRefLoaded, eRefFound, err := entities.GenerateEntityEnumRefProvider.GetByID(ctx, eRef.GetID())
	assert.NoError(t, err)
	assert.True(t, eRefFound)
	assert.Equal(t, enums.TestEnumList.A, *eRefLoaded.GetStatus())

	// ---- Provider interface tests ----

	// EntityProvider: all providers implement TableName() and DBCode()
	assert.Equal(t, "generateEntity", entities.GenerateEntityProvider.TableName())
	assert.Equal(t, "default", entities.GenerateEntityProvider.DBCode())
	assert.Equal(t, "generateEntityNoRedis", entities.GenerateEntityNoRedisProvider.TableName())
	assert.Equal(t, "default", entities.GenerateEntityNoRedisProvider.DBCode())
	assert.Equal(t, "generateEntityWithSearch", entities.GenerateEntityWithSearchProvider.TableName())
	assert.Equal(t, "default", entities.GenerateEntityWithSearchProvider.DBCode())
	assert.Equal(t, "generateEntityCachedUnique", entities.GenerateEntityCachedUniqueProvider.TableName())
	assert.Equal(t, "generateEntityCachedUniqueNoRedis", entities.GenerateEntityCachedUniqueNoRedisProvider.TableName())
	assert.Equal(t, "generateEntityCachedUniqueFakeDelete", entities.GenerateEntityCachedUniqueFakeDeleteProvider.TableName())
	assert.Equal(t, "generateEntityEnumRef", entities.GenerateEntityEnumRefProvider.TableName())
	assert.Equal(t, "generateEntityWithTimestamps", entities.GenerateEntityWithTimestampsProvider.TableName())
	assert.Equal(t, "generateEntityWithTimestampsRedis", entities.GenerateEntityWithTimestampsRedisProvider.TableName())
	assert.Equal(t, "generateReferenceEntity", entities.GenerateReferenceEntityProvider.TableName())

	// RedisCacheEntityProvider: only redis cache providers
	var redisCacheProvider fluxaorm.RedisCacheEntityProvider

	redisCacheProvider = &entities.GenerateEntityProvider
	assert.Equal(t, "default", redisCacheProvider.RedisCode())
	assert.Equal(t, "54849:", redisCacheProvider.RedisCachePrefix())
	assert.NotNil(t, redisCacheProvider.ClearRedisCache)

	redisCacheProvider = &entities.GenerateEntityCachedUniqueProvider
	assert.NotEmpty(t, redisCacheProvider.RedisCachePrefix())

	redisCacheProvider = &entities.GenerateEntityCachedUniqueFakeDeleteProvider
	assert.NotEmpty(t, redisCacheProvider.RedisCachePrefix())

	redisCacheProvider = &entities.GenerateEntityWithTimestampsRedisProvider
	assert.NotEmpty(t, redisCacheProvider.RedisCachePrefix())

	// Negative: non-redis providers do NOT implement RedisCacheEntityProvider
	var entityProvider fluxaorm.EntityProvider
	entityProvider = &entities.GenerateEntityNoRedisProvider
	_, isRedisCache := entityProvider.(fluxaorm.RedisCacheEntityProvider)
	assert.False(t, isRedisCache)

	entityProvider = &entities.GenerateEntityWithSearchProvider
	_, isRedisCache = entityProvider.(fluxaorm.RedisCacheEntityProvider)
	assert.False(t, isRedisCache)

	// RedisSearchEntityProvider: only search providers
	var redisSearchProvider fluxaorm.RedisSearchEntityProvider
	redisSearchProvider = &entities.GenerateEntityWithSearchProvider
	assert.Equal(t, "default", redisSearchProvider.RedisSearchCode())
	assert.NotEmpty(t, redisSearchProvider.RedisSearchIndexName())
	assert.NotEmpty(t, redisSearchProvider.RedisSearchHashPrefix())

	// Negative: non-search providers do NOT implement RedisSearchEntityProvider
	entityProvider = &entities.GenerateEntityProvider
	_, isRedisSearch := entityProvider.(fluxaorm.RedisSearchEntityProvider)
	assert.False(t, isRedisSearch)

	entityProvider = &entities.GenerateEntityNoRedisProvider
	_, isRedisSearch = entityProvider.(fluxaorm.RedisSearchEntityProvider)
	assert.False(t, isRedisSearch)

	// AllProviders: correct length (11 entities)
	assert.Len(t, entities.AllProviders, 11)

	// AllProviders: all entries implement EntityProvider and have non-empty TableName
	for _, p := range entities.AllProviders {
		assert.NotEmpty(t, p.TableName())
		assert.NotEmpty(t, p.DBCode())
	}

	// ClearRedisCache: insert entity to populate redis cache, then clear it
	clearEntity := entities.GenerateEntityProvider.New(ctx)
	clearEntity.SetName("clear_test")
	clearEntity.SetTestEnum(enums.TestEnumList.A)
	clearEntity.SetTime(time.Now().UTC())
	clearEntity.SetDate(time.Now().UTC())
	assert.NoError(t, ctx.Flush())

	clearID := clearEntity.GetID()
	_, found, err = entities.GenerateEntityProvider.GetByID(ctx, clearID)
	assert.NoError(t, err)
	assert.True(t, found)

	// Verify redis cache key exists
	redisClient := ctx.Engine().Redis("default")
	cacheKeys, _, errScan := redisClient.Scan(ctx, 0, entities.GenerateEntityProvider.RedisCachePrefix()+"*", 1000)
	assert.NoError(t, errScan)
	assert.Greater(t, len(cacheKeys), 0)

	// Clear redis cache
	removed, err := entities.GenerateEntityProvider.ClearRedisCache(ctx)
	assert.NoError(t, err)
	assert.Greater(t, removed, 0)

	// Verify cache keys are gone
	cacheKeys, _, errScan = redisClient.Scan(ctx, 0, entities.GenerateEntityProvider.RedisCachePrefix()+"*", 1000)
	assert.NoError(t, errScan)
	assert.Len(t, cacheKeys, 0)

	// Verify DebeziumTopicName on debezium-enabled provider
	topicName := entities.GenerateEntityDebeziumProvider.DebeziumTopicName(ctx)
	assert.Equal(t, "fluxa_default.test.generateEntityDebezium", topicName)
}
