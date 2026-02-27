package test_generate

import (
	"os"
	"testing"
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/stretchr/testify/assert"
)

type generateSubStruct struct {
	Size uint8
}

type generateEntity struct {
	ID                uint64 `orm:"redisCache"`
	Age               uint32 `orm:"unique=AgeBalance"`
	Balance           int8   `orm:"unique=AgeBalance:2"`
	AgeNullable       *uint8
	BalanceNullable   *int8
	Name              string `orm:"required"`
	Comment           string
	TestEnum          string `orm:"enum=a,b,c;required"`
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
	TestEnum          string `orm:"enum=a,b,c;required"`
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
	generateSubStruct
	TestSub generateSubStruct
}

type generateReferenceEntity struct {
	ID         uint16
	Name       string
	FakeDelete bool
}

type generateEntityWithSearch struct {
	ID    uint64  `orm:"redisSearch=default"`
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
	ctx := fluxaorm.PrepareTablesBeta(t, fluxaorm.NewRegistry(), generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{}, generateEntityWithSearch{}, generateEntityWithTimestamps{}, generateEntityWithTimestampsRedis{})
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
	assert.Equal(t, enums.TestEnum(""), e.GetTestEnum())
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
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
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
	assert.Equal(t, enums.TestEnum(""), e2.GetTestEnum())
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
	assert.Equal(t, uint64(0), e2.GetReferenceRequiredID())
	assert.Nil(t, e2.GetReferenceOptionalID())
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
	assert.Nil(t, e.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e.GetTestEnum())
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
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())

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
	assert.Nil(t, e2.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e2.GetTestEnum())
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
	assert.Equal(t, uint64(0), e2.GetReferenceRequiredID())

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
	e.SetTestSet()
	e2.SetTestSet()
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
	assert.Nil(t, e.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e.GetTestEnum())
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
	assert.Equal(t, uint64(0), e.GetReferenceRequiredID())
	assert.Equal(t, uint64(0), e2.GetAge())
	assert.Equal(t, int64(0), e2.GetBalance())
	assert.Nil(t, e2.GetAgeNullable())
	assert.Nil(t, e2.GetBalanceNullable())
	assert.Equal(t, "", e2.GetName())
	assert.Nil(t, e2.GetComment())
	assert.Equal(t, enums.TestEnumList.A, e2.GetTestEnum())
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
	assert.Equal(t, "Test comment", *e.GetComment())
	assert.Equal(t, "Test comment", *e2.GetComment())
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
	assert.Equal(t, ref.GetID(), *e.GetReferenceOptionalID())
	assert.Equal(t, ref.GetID(), *e2.GetReferenceOptionalID())

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
	assert.NoError(t, ctx.Flush())

	// SearchIDs: generateEntityNoRedis (no FakeDelete)
	var ids []uint64
	var total int
	ids, err = entities.GenerateEntityNoRedisProvider.SearchIDs(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{e2.GetID()}, ids)

	ids, err = entities.GenerateEntityNoRedisProvider.SearchIDs(ctx, fluxaorm.NewWhere("`Name` = ?", "Hello"), nil)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{e2.GetID()}, ids)

	ids, err = entities.GenerateEntityNoRedisProvider.SearchIDs(ctx, fluxaorm.NewWhere("`Name` = ?", "NoMatch"), nil)
	assert.NoError(t, err)
	assert.Nil(t, ids)

	ids, err = entities.GenerateEntityNoRedisProvider.SearchIDs(ctx, nil, fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, []uint64{e2.GetID()}, ids)

	// SearchIDsWithCount: generateEntityNoRedis
	ids, total, err = entities.GenerateEntityNoRedisProvider.SearchIDsWithCount(ctx, nil, *fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []uint64{e2.GetID()}, ids)

	ids, total, err = entities.GenerateEntityNoRedisProvider.SearchIDsWithCount(ctx, fluxaorm.NewWhere("`Name` = ?", "NoMatch"), *fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Nil(t, ids)

	ids, total, err = entities.GenerateEntityNoRedisProvider.SearchIDsWithCount(ctx, nil, *fluxaorm.NewPager(2, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Nil(t, ids)

	// SearchIDs and SearchIDsWithCount: generateReferenceEntity (FakeDelete)
	ref2 := entities.GenerateReferenceEntityProvider.New(ctx)
	ref2.SetName("Test Reference 2")
	assert.NoError(t, ctx.Flush())

	ids, err = entities.GenerateReferenceEntityProvider.SearchIDs(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, ref.GetID())
	assert.Contains(t, ids, ref2.GetID())

	ids, total, err = entities.GenerateReferenceEntityProvider.SearchIDsWithCount(ctx, nil, *fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, ids, 2)

	ref.Delete()
	assert.NoError(t, ctx.Flush())

	ids, err = entities.GenerateReferenceEntityProvider.SearchIDs(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{ref2.GetID()}, ids)

	ids, err = entities.GenerateReferenceEntityProvider.SearchIDs(ctx, fluxaorm.NewWhere("1 = 1").WithFakeDeletes(), nil)
	assert.NoError(t, err)
	assert.Len(t, ids, 2)

	ids, total, err = entities.GenerateReferenceEntityProvider.SearchIDsWithCount(ctx, nil, *fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []uint64{ref2.GetID()}, ids)

	ids, total, err = entities.GenerateReferenceEntityProvider.SearchIDsWithCount(ctx, fluxaorm.NewWhere("1 = 1").WithFakeDeletes(), *fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, ids, 2)

	// Search: generateEntityNoRedis (no FakeDelete)
	var list2 []*entities.GenerateEntityNoRedis
	list2, err = entities.GenerateEntityNoRedisProvider.Search(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())
	assert.Equal(t, "Hello", list2[0].GetName())

	list2, err = entities.GenerateEntityNoRedisProvider.Search(ctx, fluxaorm.NewWhere("`Name` = ?", "Hello"), nil)
	assert.NoError(t, err)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())

	list2, err = entities.GenerateEntityNoRedisProvider.Search(ctx, fluxaorm.NewWhere("`Name` = ?", "NoMatch"), nil)
	assert.NoError(t, err)
	assert.Nil(t, list2)

	list2, err = entities.GenerateEntityNoRedisProvider.Search(ctx, nil, fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Len(t, list2, 1)

	list2, err = entities.GenerateEntityNoRedisProvider.Search(ctx, nil, fluxaorm.NewPager(2, 10))
	assert.NoError(t, err)
	assert.Nil(t, list2)

	// SearchWithCount: generateEntityNoRedis (no FakeDelete)
	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchWithCount(ctx, nil, fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, list2, 1)
	assert.Equal(t, e2.GetID(), list2[0].GetID())

	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchWithCount(ctx, fluxaorm.NewWhere("`Name` = ?", "NoMatch"), fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Nil(t, list2)

	list2, total, err = entities.GenerateEntityNoRedisProvider.SearchWithCount(ctx, nil, fluxaorm.NewPager(2, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Nil(t, list2)

	// Search: generateReferenceEntity (FakeDelete) — ref soft-deleted, ref2 active
	var listRef []*entities.GenerateReferenceEntity
	listRef, err = entities.GenerateReferenceEntityProvider.Search(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref2.GetID(), listRef[0].GetID())

	listRef, err = entities.GenerateReferenceEntityProvider.Search(ctx, fluxaorm.NewWhere("1 = 1").WithFakeDeletes(), nil)
	assert.NoError(t, err)
	assert.Len(t, listRef, 2)

	listRef, err = entities.GenerateReferenceEntityProvider.Search(ctx, fluxaorm.NewWhere("`Name` = ?", "Test Reference").WithFakeDeletes(), nil)
	assert.NoError(t, err)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref.GetID(), listRef[0].GetID())

	// SearchWithCount: generateReferenceEntity (FakeDelete)
	listRef, total, err = entities.GenerateReferenceEntityProvider.SearchWithCount(ctx, nil, fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, listRef, 1)
	assert.Equal(t, ref2.GetID(), listRef[0].GetID())

	listRef, total, err = entities.GenerateReferenceEntityProvider.SearchWithCount(ctx, fluxaorm.NewWhere("1 = 1").WithFakeDeletes(), fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, listRef, 2)

	// SearchOne: generateEntityNoRedis (no FakeDelete)
	var one *entities.GenerateEntityNoRedis
	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, fluxaorm.NewWhere("`Name` = ?", "Hello"))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, e2.GetID(), one.GetID())

	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, fluxaorm.NewWhere("`Name` = ?", "NoMatch"))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, one)

	one, found, err = entities.GenerateEntityNoRedisProvider.SearchOne(ctx, nil)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, one)

	// SearchOne: generateReferenceEntity (FakeDelete) — ref is soft-deleted, ref2 is active
	var oneRef *entities.GenerateReferenceEntity
	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewWhere("`Name` = ?", "Test Reference"))
	assert.NoError(t, err)
	assert.False(t, found) // soft-deleted, filtered out
	assert.Nil(t, oneRef)

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewWhere("`Name` = ?", "Test Reference").WithFakeDeletes())
	assert.NoError(t, err)
	assert.True(t, found) // WithFakeDeletes bypasses filter
	assert.Equal(t, ref.GetID(), oneRef.GetID())

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, fluxaorm.NewWhere("`Name` = ?", "Test Reference 2"))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, ref2.GetID(), oneRef.GetID())

	oneRef, found, err = entities.GenerateReferenceEntityProvider.SearchOne(ctx, nil)
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

	// SearchIDsInRedis: all entities
	var searchIDs []uint64
	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, searchIDs, 3)
	assert.Contains(t, searchIDs, es1.GetID())
	assert.Contains(t, searchIDs, es2.GetID())
	assert.Contains(t, searchIDs, es3.GetID())

	// SearchIDsInRedis: numeric range
	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericRange("Age", 10, 20), nil)
	assert.NoError(t, err)
	assert.Len(t, searchIDs, 2)
	assert.Contains(t, searchIDs, es1.GetID())
	assert.Contains(t, searchIDs, es2.GetID())

	// SearchIDsInRedisWithCount
	var searchTotal int
	searchIDs, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedisWithCount(ctx, fluxaorm.NewRedisSearchWhere().NumericMin("Age", 20), fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 2, searchTotal)
	assert.Len(t, searchIDs, 2)
	assert.Contains(t, searchIDs, es2.GetID())
	assert.Contains(t, searchIDs, es3.GetID())

	// SearchIDsInRedisWithCount: no match
	searchIDs, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedisWithCount(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 999), fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 0, searchTotal)
	assert.Nil(t, searchIDs)

	// SearchInRedis: returns entities
	var searchEntities []*entities.GenerateEntityWithSearch
	searchEntities, err = entities.GenerateEntityWithSearchProvider.SearchInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericMax("Age", 20), nil)
	assert.NoError(t, err)
	assert.Len(t, searchEntities, 2)

	// SearchOneInRedis
	var searchOne *entities.GenerateEntityWithSearch
	searchOne, found, err = entities.GenerateEntityWithSearchProvider.SearchOneInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 10))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, es1.GetID(), searchOne.GetID())

	searchOne, found, err = entities.GenerateEntityWithSearchProvider.SearchOneInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 999))
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, searchOne)

	// SearchInRedisWithCount
	var searchWithTotal []*entities.GenerateEntityWithSearch
	searchWithTotal, searchTotal, err = entities.GenerateEntityWithSearchProvider.SearchInRedisWithCount(ctx, nil, fluxaorm.NewPager(1, 10))
	assert.NoError(t, err)
	assert.Equal(t, 3, searchTotal)
	assert.Len(t, searchWithTotal, 3)

	// Update searchable field: es1 Age 10 → 15
	es1.SetAge(15)
	assert.NoError(t, ctx.Flush())

	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 10), nil)
	assert.NoError(t, err)
	assert.Nil(t, searchIDs)

	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 15), nil)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{es1.GetID()}, searchIDs)

	// Delete entity: should be removed from Redis Search
	es3.Delete()
	assert.NoError(t, ctx.Flush())

	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, searchIDs, 2)
	assert.NotContains(t, searchIDs, es3.GetID())

	// ReindexRedisSearch: rebuild the entire index from MySQL; results should remain correct
	err = entities.GenerateEntityWithSearchProvider.ReindexRedisSearch(ctx)
	assert.NoError(t, err)
	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, searchIDs, 2)
	assert.Contains(t, searchIDs, es1.GetID())
	assert.Contains(t, searchIDs, es2.GetID())
	// After reindex, updated age=15 for es1 should be searchable
	searchIDs, err = entities.GenerateEntityWithSearchProvider.SearchIDsInRedis(ctx, fluxaorm.NewRedisSearchWhere().NumericEqual("Age", 15), nil)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{es1.GetID()}, searchIDs)

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
}
