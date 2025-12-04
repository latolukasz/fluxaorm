package fluxaorm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flushStruct struct {
	Name2    string
	Age      int
	Sub      flushSubStruct
	TestTime *time.Time `orm:"time=true"`
}

type flushSubStruct struct {
	Name3 string
	Age3  int
}

type flushStructAnonymous struct {
	SubName string
	SubAge  float32 `orm:"decimal=9,5;unsigned=false"`
}

type flushStructJSON struct {
	A string
	B uint64
}

type testEnum string

func (s testEnum) EnumValues() any {
	return testEnumDefinition
}

var testEnumDefinition = struct {
	A testEnum
	B testEnum
	C testEnum
}{
	A: "a",
	B: "b",
	C: "c",
}

type testSet string

func (s testSet) EnumValues() any {
	return testSetDefinition
}

var testSetDefinition = struct {
	D testSet
	E testSet
	F testSet
}{
	D: "d",
	E: "e",
	F: "f",
}

var flushEntityIndexes = struct {
	City UniqueIndexDefinition
	Name UniqueIndexDefinition
}{
	City: UniqueIndexDefinition{"City", false},
	Name: UniqueIndexDefinition{"Name", false},
}

type flushEntity struct {
	ID                        uint16 `orm:"localCache;redisCache;ttl=30"`
	City                      string `orm:"length=40"`
	Name                      string `orm:"required"`
	StringArray               [2]string
	Age                       int
	IntArray                  [2]int
	Uint                      uint
	UintArray                 [2]uint
	UintNullable              *uint
	UintNullableArray         [2]*uint
	IntNullable               *int
	IntNullableArray          [2]*int
	BoolNullable              *bool
	BoolNullableArray         [2]*bool
	FloatNullable             *float64    `orm:"precision=3;unsigned"`
	FloatNullableArray        [2]*float64 `orm:"precision=3;unsigned"`
	Float32Nullable           *float32    `orm:"precision=4"`
	Float32NullableArray      [2]*float32 `orm:"precision=4"`
	SetNullable               []testSet
	SetNullableArray          [2][]testSet
	SetNotNull                []testSet `orm:"required"`
	EnumNullable              testEnum
	EnumNullableArray         [2]testEnum
	EnumNotNull               testEnum `orm:"required"`
	Ignored                   []string `orm:"ignore"`
	Blob                      []uint8
	BlobArray                 [2][]uint8
	Bool                      bool
	BoolArray                 [2]bool
	Float64                   float64     `orm:"precision=5"`
	Float64Array              [2]float64  `orm:"precision=5"`
	Float32                   float32     `orm:"precision=5"`
	Float32Array              [2]float32  `orm:"precision=5"`
	Decimal                   float64     `orm:"decimal=5,2"`
	DecimalArray              [2]float64  `orm:"decimal=5,2"`
	DecimalNullable           *float64    `orm:"decimal=5,2"`
	DecimalNullableArray      [2]*float64 `orm:"decimal=5,2"`
	Float64Unsigned           float64     `orm:"unsigned"`
	Float64UnsignedArray      [2]float64  `orm:"unsigned"`
	Float64Signed             float64
	Float64SignedArray        [2]float64
	Time                      time.Time
	TimeArray                 [2]time.Time
	TimeWithTime              time.Time    `orm:"time"`
	TimeWithTimeArray         [2]time.Time `orm:"time"`
	TimeNullable              *time.Time
	TimeNullableArray         [2]*time.Time
	TimeWithTimeNullable      *time.Time    `orm:"time"`
	TimeWithTimeNullableArray [2]*time.Time `orm:"time"`
	FlushStruct               flushStruct
	FlushStructArray          [2]flushStruct
	Int8Nullable              *int8
	Int16Nullable             *int16
	Int32Nullable             *int32
	Int64Nullable             *int64
	Uint8Nullable             *uint8
	Uint16Nullable            *uint16
	Uint32Nullable            *uint32
	Uint32NullableArray       [2]*uint32
	Uint64Nullable            *uint64
	Reference                 Reference[flushEntityReference]
	ReferenceArray            [2]Reference[flushEntityReference]
	ReferenceRequired         Reference[flushEntityReference] `orm:"required"`
	TestJsons                 Struct[flushStructJSON]
	References                References[flushEntityReference]
	flushStructAnonymous
}

func (e *flushEntity) Indexes() any {
	return flushEntityIndexes
}

type flushEntityReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestFlushInsertLocalRedis(t *testing.T) {
	testFlushInsert(t, false, true, true)
}

func TestFlushAsyncInsertLocalRedis(t *testing.T) {
	testFlushInsert(t, true, true, true)
}

func TestFlushInsertLocal(t *testing.T) {
	testFlushInsert(t, false, true, false)
}

func TestFlushAsyncInsertLocal(t *testing.T) {
	testFlushInsert(t, true, true, false)
}

func TestFlushInsertNoCache(t *testing.T) {
	testFlushInsert(t, false, false, false)
}

func TestFlushAsyncInsertNoCache(t *testing.T) {
	testFlushInsert(t, true, false, false)
}

func TestFlushInsertRedis(t *testing.T) {
	testFlushInsert(t, false, false, true)
}

func TestFlushAsyncInsertRedis(t *testing.T) {
	testFlushInsert(t, true, false, true)
}

func TestFlushDeleteLocalRedis(t *testing.T) {
	testFlushDelete(t, false, true, true)
}

func TestFlushDeleteLocal(t *testing.T) {
	testFlushDelete(t, false, true, false)
}

func TestFlushDeleteNoCache(t *testing.T) {
	testFlushDelete(t, false, false, false)
}

func TestFlushAsyncDeleteNoCache(t *testing.T) {
	testFlushDelete(t, true, false, false)
}

func TestFlushDeleteRedis(t *testing.T) {
	testFlushDelete(t, false, false, true)
}

func TestFlushUpdateLocalRedis(t *testing.T) {
	testFlushUpdate(t, false, true, true)
}

func TestFlushUpdateLocal(t *testing.T) {
	testFlushUpdate(t, false, true, false)
}

func TestFlushUpdateNoCache(t *testing.T) {
	testFlushUpdate(t, false, false, false)
}

func TestFlushUpdateRedis(t *testing.T) {
	testFlushUpdate(t, false, false, true)
}

func TestFlushUpdateUpdateLocalRedis(t *testing.T) {
	testFlushUpdate(t, true, true, true)
}

func TestFlushUpdateUpdateLocal(t *testing.T) {
	testFlushUpdate(t, true, true, false)
}

func TestFlushUpdateUpdateNoCache(t *testing.T) {
	testFlushUpdate(t, true, false, false)
}

func TestFlushUpdateUpdateRedis(t *testing.T) {
	testFlushUpdate(t, true, false, true)
}

func testFlushInsert(t *testing.T, async, local, redis bool) {
	r := NewRegistry()
	orm := PrepareTables(t, r, flushEntity{}, flushEntityReference{})

	schema, err := GetEntitySchema[flushEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)

	reference, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference"
	err = testFlush(orm, async)
	assert.NoError(t, err)

	reference2, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference2.Name = "test reference 2"
	err = testFlush(orm, async)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerLocal, false, false, true)
	loggerRedis := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerRedis, false, true, false)

	// Adding empty entity
	nE, err := schema.NewEntity(orm)
	assert.NoError(t, err)
	newEntity := nE.(*flushEntity)
	assert.NoError(t, err)
	assert.NotNil(t, newEntity.BoolArray)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NotEmpty(t, newEntity.ID)

	assert.NoError(t, testFlush(orm, async))
	loggerDB.Clear()

	entity, _, err := GetByID[flushEntity](orm, uint64(newEntity.ID))
	assert.NoError(t, err)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
		assert.NotNil(t, entity)
	} else if async {
		assert.Nil(t, entity)
		err = runAsyncConsumer(orm)
		assert.NoError(t, err)
		entity, _, err = GetByID[flushEntity](orm, uint64(newEntity.ID))
		assert.NoError(t, err)
		assert.NotNil(t, entity)
	}

	assert.Equal(t, newEntity.ID, entity.ID)
	assert.Equal(t, "", entity.City)
	assert.Equal(t, "Name", entity.Name)
	assert.Equal(t, 0, entity.Age)
	assert.Equal(t, uint(0), entity.Uint)
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, []testSet{testSetDefinition.D}, entity.SetNotNull)
	assert.Equal(t, testEnum(""), entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Nil(t, entity.Blob)
	assert.False(t, entity.Bool)
	assert.Equal(t, 0.0, entity.Float64)
	assert.Equal(t, float32(0.0), entity.Float32)
	assert.Equal(t, 0.0, entity.Decimal)
	assert.Nil(t, entity.DecimalNullable)
	assert.Equal(t, 0.0, entity.Float64Unsigned)
	assert.Equal(t, 0.0, entity.Float64Signed)
	assert.Equal(t, new(time.Time).UTC(), entity.Time)
	assert.Equal(t, new(time.Time).UTC(), entity.TimeWithTime)
	assert.Nil(t, entity.TimeNullable)
	assert.Nil(t, entity.TimeWithTimeNullable)
	assert.Equal(t, "", entity.FlushStruct.Name2)
	assert.Equal(t, 0, entity.FlushStruct.Age)
	assert.Equal(t, "", entity.FlushStruct.Sub.Name3)
	assert.Equal(t, 0, entity.FlushStruct.Sub.Age3)
	assert.Nil(t, entity.FlushStruct.TestTime)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.Int16Nullable)
	assert.Nil(t, entity.Int32Nullable)
	assert.Nil(t, entity.Int64Nullable)
	assert.Nil(t, entity.Uint8Nullable)
	assert.Nil(t, entity.Uint16Nullable)
	assert.Nil(t, entity.Uint32Nullable)
	assert.Nil(t, entity.Uint64Nullable)
	assert.Equal(t, "", entity.SubName)
	assert.Equal(t, float32(0), entity.SubAge)
	assert.Zero(t, entity.Reference)
	assert.NotNil(t, reference.ID, entity.ReferenceRequired)
	assert.Nil(t, entity.TestJsons.Get())
	assert.Equal(t, 0, entity.References.Len())

	for i := 0; i < 2; i++ {
		assert.Equal(t, "", entity.StringArray[i])
		assert.Equal(t, 0, entity.IntArray[i])
		assert.Equal(t, uint(0), entity.UintArray[i])
		assert.Nil(t, entity.UintNullableArray[i])
		assert.Nil(t, entity.IntNullableArray[i])
		assert.Nil(t, entity.BoolNullableArray[i])
		assert.Nil(t, entity.Float32NullableArray[i])
		assert.Nil(t, entity.SetNullableArray[i])
		assert.Equal(t, testEnum(""), entity.EnumNullableArray[i])
		assert.Nil(t, entity.BlobArray[i])
		assert.Equal(t, false, entity.BoolArray[i])
		assert.Equal(t, float64(0), entity.Float64Array[i])
		assert.Equal(t, float32(0), entity.Float32Array[i])
		assert.Equal(t, float64(0), entity.DecimalArray[i])
		assert.Nil(t, entity.DecimalNullableArray[i])
		assert.Equal(t, float64(0), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(0), entity.Float64SignedArray[i])
		assert.Equal(t, new(time.Time).UTC(), entity.TimeArray[i])
		assert.Equal(t, new(time.Time).UTC(), entity.TimeWithTimeArray[i])
		assert.Nil(t, entity.TimeNullableArray[i])
		assert.Nil(t, entity.TimeWithTimeNullableArray[i])
		assert.Nil(t, entity.Uint32NullableArray[i])
		assert.Zero(t, entity.ReferenceArray[i])
		assert.Nil(t, entity.FlushStructArray[i].TestTime)
		assert.Equal(t, 0, entity.FlushStructArray[i].Age)
		assert.Equal(t, "", entity.FlushStructArray[i].Name2)
		assert.Equal(t, "", entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, 0, entity.FlushStructArray[i].Sub.Age3)
	}

	// Adding full entity
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.City = "New York"
	newEntity.Name = "Test name"
	newEntity.Age = -19
	newEntity.Uint = 134
	uintNullable := uint(23)
	newEntity.UintNullable = &uintNullable
	intNullable := -45
	newEntity.IntNullable = &intNullable
	boolNullable := true
	newEntity.BoolNullable = &boolNullable
	floatNullable := 12.23
	newEntity.FloatNullable = &floatNullable
	float32Nullable := float32(12.24)
	newEntity.Float32Nullable = &float32Nullable
	newEntity.SetNullable = []testSet{testSetDefinition.E, testSetDefinition.F}
	newEntity.SetNotNull = []testSet{testSetDefinition.D, testSetDefinition.F}
	newEntity.EnumNullable = testEnumDefinition.C
	newEntity.EnumNotNull = testEnumDefinition.A
	newEntity.Blob = []byte("test binary")
	newEntity.Bool = true
	newEntity.Float64 = 986.2322
	newEntity.Float32 = 86.232
	newEntity.Decimal = 78.24
	decimalNullable := 123.23
	newEntity.DecimalNullable = &decimalNullable
	newEntity.Float64Unsigned = 8932.299423
	newEntity.Float64Signed = -352.120321
	newEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC)
	newEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC)
	timeNullable := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	newEntity.TimeNullable = &timeNullable
	timeWithTimeNullable := time.Date(2025, 11, 4, 21, 0, 5, 6, time.UTC)
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	newEntity.FlushStruct.Name2 = "Tom"
	newEntity.FlushStruct.Age = 23
	newEntity.FlushStruct.Sub.Name3 = "Zoya"
	newEntity.FlushStruct.Sub.Age3 = 18
	testTime := time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
	newEntity.FlushStruct.TestTime = &testTime
	int8Nullable := int8(23)
	newEntity.Int8Nullable = &int8Nullable
	int16Nullable := int16(-29)
	newEntity.Int16Nullable = &int16Nullable
	int32Nullable := int32(-2923)
	newEntity.Int32Nullable = &int32Nullable
	int64Nullable := int64(98872)
	newEntity.Int64Nullable = &int64Nullable
	uint8Nullable := uint8(23)
	newEntity.Uint8Nullable = &uint8Nullable
	uint16Nullable := uint16(29)
	newEntity.Uint16Nullable = &uint16Nullable
	uint32Nullable := uint32(2923)
	newEntity.Uint32Nullable = &uint32Nullable
	uint64Nullable := uint64(98872)
	newEntity.Uint64Nullable = &uint64Nullable
	newEntity.SubName = "sub name"
	newEntity.SubAge = 123
	newEntity.Reference = Reference[flushEntityReference](reference.ID)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.TestJsons.Set(&flushStructJSON{"Hi", 12})
	newEntity.References.SetIDs([]uint64{1, 2, reference.ID, reference2.ID})

	for i := 0; i < 2; i++ {
		newEntity.StringArray[i] = fmt.Sprintf("Test %d", i)
		newEntity.IntArray[i] = i + 1
		newEntity.UintArray[i] = uint(i + 1)
		newEntity.UintNullableArray[i] = &newEntity.UintArray[i]
		newEntity.IntNullableArray[i] = &newEntity.IntArray[i]
		newEntity.BoolArray[i] = true
		newEntity.BoolNullableArray[i] = &newEntity.BoolArray[i]
		newEntity.Float64Array[i] = float64(i + 1)
		newEntity.Float32Array[i] = float32(i + 1)
		newEntity.DecimalArray[i] = float64(i + 1)
		newEntity.Float64UnsignedArray[i] = float64(i + 1)
		newEntity.Float64SignedArray[i] = float64(i + 1)
		newEntity.Float32NullableArray[i] = &newEntity.Float32Array[i]
		newEntity.SetNullableArray[i] = []testSet{testSetDefinition.E, testSetDefinition.F}
		newEntity.EnumNullableArray[i] = testEnumDefinition.C
		newEntity.BlobArray[i] = []byte(fmt.Sprintf("Test %d", i))
		newEntity.DecimalNullableArray[i] = &newEntity.DecimalArray[i]
		newEntity.TimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		newEntity.TimeWithTimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		newEntity.TimeNullableArray[i] = &newEntity.TimeWithTimeArray[i]
		newEntity.TimeWithTimeNullableArray[i] = &newEntity.TimeWithTimeArray[i]
		newEntity.Uint32NullableArray[i] = &uint32Nullable
		newEntity.ReferenceArray[i] = Reference[flushEntityReference](reference.ID)
		newEntity.FlushStructArray[i].Age = i + 1
		newEntity.FlushStructArray[i].Name2 = fmt.Sprintf("Name %d", i)
		newEntity.FlushStructArray[i].Sub.Name3 = fmt.Sprintf("Name %d", i)
		newEntity.FlushStructArray[i].Sub.Age3 = i + 1
	}
	assert.NoError(t, testFlush(orm, async))
	if async {
		err = runAsyncConsumer(orm)
		assert.NoError(t, err)
	}
	entity, _, err = GetByID[flushEntity](orm, uint64(newEntity.ID))
	assert.NoError(t, err)
	assert.NotNil(t, entity)

	assert.Equal(t, newEntity.ID, entity.ID)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, "Test name", entity.Name)
	assert.Equal(t, -19, entity.Age)
	assert.Equal(t, uint(134), entity.Uint)
	assert.Equal(t, uint(23), *entity.UintNullable)
	assert.Equal(t, -45, *entity.IntNullable)
	assert.True(t, *entity.BoolNullable)
	assert.Equal(t, 12.23, *entity.FloatNullable)
	assert.Equal(t, float32(12.24), *entity.Float32Nullable)
	assert.Equal(t, []testSet{testSetDefinition.E, testSetDefinition.F}, entity.SetNullable)
	assert.Equal(t, []testSet{testSetDefinition.D, testSetDefinition.F}, entity.SetNotNull)
	assert.Equal(t, testEnumDefinition.C, entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Equal(t, []byte("test binary"), entity.Blob)
	assert.True(t, entity.Bool)
	assert.Equal(t, 986.2322, entity.Float64)
	assert.Equal(t, float32(86.232), entity.Float32)
	assert.Equal(t, 78.24, entity.Decimal)
	assert.Equal(t, 123.23, *entity.DecimalNullable)
	assert.Equal(t, 8932.299423, entity.Float64Unsigned)
	assert.Equal(t, -352.120321, entity.Float64Signed)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), entity.TimeWithTime)
	assert.Equal(t, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), *entity.TimeNullable)
	assert.Equal(t, time.Date(2025, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullable)
	assert.Equal(t, "Tom", entity.FlushStruct.Name2)
	assert.Equal(t, 23, entity.FlushStruct.Age)
	assert.Equal(t, "Zoya", entity.FlushStruct.Sub.Name3)
	assert.Equal(t, 18, entity.FlushStruct.Sub.Age3)
	assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.FlushStruct.TestTime)
	assert.Equal(t, int8(23), *entity.Int8Nullable)
	assert.Equal(t, int16(-29), *entity.Int16Nullable)
	assert.Equal(t, int32(-2923), *entity.Int32Nullable)
	assert.Equal(t, int64(98872), *entity.Int64Nullable)
	assert.Equal(t, uint8(23), *entity.Uint8Nullable)
	assert.Equal(t, uint16(29), *entity.Uint16Nullable)
	assert.Equal(t, uint32(2923), *entity.Uint32Nullable)
	assert.Equal(t, uint64(98872), *entity.Uint64Nullable)
	assert.Equal(t, "sub name", entity.SubName)
	assert.Equal(t, float32(123), entity.SubAge)
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())
	assert.NotNil(t, entity.TestJsons.Get())
	assert.Equal(t, "Hi", entity.TestJsons.Get().A)
	assert.Equal(t, uint64(12), entity.TestJsons.Get().B)
	assert.Equal(t, 4, entity.References.Len())
	assert.Equal(t, []uint64{1, 2, reference.ID, reference2.ID}, entity.References.GetIDs())

	refs, err := entity.References.GetEntity(orm, 0)
	assert.NoError(t, err)
	assert.Nil(t, refs)
	refs, err = entity.References.GetEntity(orm, 1)
	assert.NoError(t, err)
	assert.Nil(t, refs)
	refs2, err := entity.References.GetEntity(orm, 2)
	assert.NoError(t, err)
	assert.NotNil(t, refs2)
	refs3, err := entity.References.GetEntity(orm, 3)
	assert.NoError(t, err)
	assert.NotNil(t, refs3)
	assert.Equal(t, "test reference", refs2.Name)
	assert.Equal(t, "test reference 2", refs3.Name)
	references, err := entity.References.GetEntities(orm)
	assert.NoError(t, err)
	assert.Equal(t, 4, references.Len())
	for i := 0; i < 2; i++ {
		assert.Equal(t, fmt.Sprintf("Test %d", i), entity.StringArray[i])
		assert.Equal(t, i+1, entity.IntArray[i])
		assert.Equal(t, uint(i+1), entity.UintArray[i])
		assert.Equal(t, uint(i+1), *entity.UintNullableArray[i])
		assert.Equal(t, i+1, *entity.IntNullableArray[i])
		assert.True(t, *entity.BoolNullableArray[i])
		assert.Equal(t, float32(i+1), *entity.Float32NullableArray[i])
		assert.Equal(t, []testSet{testSetDefinition.E, testSetDefinition.F}, entity.SetNullableArray[i])
		assert.Equal(t, testEnumDefinition.C, entity.EnumNullableArray[i])
		assert.Equal(t, []byte(fmt.Sprintf("Test %d", i)), entity.BlobArray[i])
		assert.True(t, entity.BoolArray[i])
		assert.Equal(t, float64(i+1), entity.Float64Array[i])
		assert.Equal(t, float32(i+1), entity.Float32Array[i])
		assert.Equal(t, float64(i+1), entity.DecimalArray[i])
		assert.Equal(t, float64(i+1), *entity.DecimalNullableArray[i])
		assert.Equal(t, float64(i+1), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(i+1), entity.Float64SignedArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), entity.TimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), entity.TimeWithTimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), *entity.TimeNullableArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullableArray[i])
		assert.Equal(t, uint32(2923), *entity.Uint32NullableArray[i])
		assert.Equal(t, entity.Reference, entity.ReferenceArray[i])
		assert.Equal(t, i+1, entity.FlushStructArray[i].Age)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Name2)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, i+1, entity.FlushStructArray[i].Sub.Age3)
	}

	// rounding dates
	newEntity = &flushEntity{}
	err = NewEntityFromSource(orm, newEntity)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "rounding dates"
	newEntity.City = "rounding dates"
	newEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	newEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, testFlush(orm, async))
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), newEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), newEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *newEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *newEntity.TimeWithTimeNullable)

	// rounding floats
	newEntity = &flushEntity{}
	err = orm.NewEntity(newEntity)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "rounding floats"
	newEntity.City = "rounding floats"
	newEntity.Float64 = 1.123456
	newEntity.Decimal = 1.123
	floatNullable = 1.1234
	newEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	newEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, testFlush(orm, async))
	assert.Equal(t, 1.12346, newEntity.Float64)
	assert.Equal(t, 1.12, newEntity.Decimal)
	assert.Equal(t, 1.123, *newEntity.FloatNullable)
	assert.Equal(t, 1.13, *newEntity.DecimalNullable)

	// invalid values

	// empty string
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	orm.ClearFlush()

	// string too long
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = strings.Repeat("a", 256)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	orm.ClearFlush()
	assert.NoError(t, testFlush(orm, async))
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.City = strings.Repeat("a", 41)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	newEntity.City = strings.Repeat("a", 40)
	newEntity.Name = "String to long"
	assert.NoError(t, testFlush(orm, async))

	// invalid decimal
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Decimal = 1234
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	orm.ClearFlush()
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	decimalNullable = 1234
	newEntity.DecimalNullable = &decimalNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	orm.ClearFlush()

	// float signed
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Float64Unsigned = -1
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	newEntity.Float64Unsigned = 1
	floatNullable = -1
	newEntity.FloatNullable = &floatNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	orm.ClearFlush()

	// invalid enum, set
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.Name = "Name 2"
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.EnumNotNull = ""
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = "invalid"
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	newEntity.EnumNotNull = testEnumDefinition.C
	newEntity.SetNotNull = nil
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = []testSet{}
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	newEntity.SetNotNull = []testSet{"invalid"}
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	orm.ClearFlush()

	// Time
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.Name = "Name"
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Time = time.Now().Local()
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	newEntity.Time = newEntity.Time.UTC()
	newEntity.TimeWithTime = time.Now().Local()
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	newEntity.TimeWithTime = newEntity.Time.UTC()
	timeNullable = time.Now().Local()
	newEntity.TimeNullable = &timeNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	newEntity.TimeNullable = &timeNullable
	newEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	orm.ClearFlush()

	if !async {

		// duplicated key
		newEntity, err = NewEntity[flushEntity](orm)
		assert.NoError(t, err)
		newEntity.City = "Another city "
		newEntity.Name = "Name"
		newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
		err = testFlush(orm, async)
		duplicateKeyError, isDuplicateKeyError := err.(*DuplicateKeyError)
		assert.EqualError(t, err, "Duplicate entry 'Name' for key 'flushEntity.Name'")
		assert.True(t, isDuplicateKeyError)
		assert.Equal(t, "Name", duplicateKeyError.Index)
		orm.ClearFlush()

		err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
		assert.NoError(t, err)
		newEntity, err = NewEntity[flushEntity](orm)
		assert.NoError(t, err)
		newEntity.Name = "Name"
		newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
		err = testFlush(orm, async)
		assert.EqualError(t, err, "Duplicate entry 'Name' for key 'flushEntity.Name'")
		orm.ClearFlush()
	}
}

func testFlushDelete(t *testing.T, async, local, redis bool) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, flushEntity{}, flushEntityReference{})

	schema, err := GetEntitySchema[flushEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)

	reference, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference"
	err = testFlush(orm, false)
	assert.NoError(t, err)

	entity, err := NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	entity.Name = "Test 1"
	entity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	err = testFlush(orm, false)
	assert.NoError(t, err)

	id := entity.ID

	err = DeleteEntity(orm, entity)
	assert.NoError(t, err)
	err = testFlush(orm, async)
	assert.NoError(t, err)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	if redis || local {
		_, found, err := GetByID[flushEntity](orm, uint64(id))
		assert.NoError(t, err)
		assert.False(t, found)
		assert.Len(t, loggerDB.Logs, 0)
		loggerDB.Clear()
	}

	if async {
		err = runAsyncConsumer(orm)
		assert.NoError(t, err)
	}

	entity, found, err := GetByID[flushEntity](orm, uint64(id))
	assert.NoError(t, err)
	assert.False(t, found)

	// duplicated key
	entity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	entity.Name = "Test 1"
	entity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	err = testFlush(orm, false)
	assert.NoError(t, err)
}

func testFlushUpdate(t *testing.T, async, local, redis bool) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, flushEntity{}, flushEntityReference{})

	schema, err := GetEntitySchema[flushEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(!local, !redis)

	reference, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference"
	err = testFlush(orm, false)
	assert.NoError(t, err)

	newEntity, err := NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "Name"
	assert.NoError(t, testFlush(orm, false))

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)
	loggerLocal := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerLocal, false, false, true)

	// empty entity
	editedEntity, err := EditEntity(orm, newEntity)
	assert.NoError(t, err)
	assert.Equal(t, "Name", editedEntity.Name)
	assert.Equal(t, newEntity.ReferenceRequired, editedEntity.ReferenceRequired)
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// editing to full entity
	editedEntityFull, err := EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity = editedEntityFull
	editedEntity.City = "New York"
	editedEntity.Name = "Test name"
	editedEntity.Age = -19
	editedEntity.Uint = 134
	uintNullable := uint(23)
	editedEntity.UintNullable = &uintNullable
	intNullable := -45
	editedEntity.IntNullable = &intNullable
	boolNullable := true
	editedEntity.BoolNullable = &boolNullable
	floatNullable := 12.23
	editedEntity.FloatNullable = &floatNullable
	float32Nullable := float32(12.24)
	editedEntity.Float32Nullable = &float32Nullable
	editedEntity.SetNullable = []testSet{testSetDefinition.E, testSetDefinition.F}
	editedEntity.SetNotNull = []testSet{testSetDefinition.D, testSetDefinition.F}
	editedEntity.EnumNullable = testEnumDefinition.C
	editedEntity.EnumNotNull = testEnumDefinition.A
	editedEntity.Blob = []byte("test binary")
	editedEntity.Bool = true
	editedEntity.Float64 = 986.2322
	editedEntity.Decimal = 78.24
	decimalNullable := 123.23
	editedEntity.DecimalNullable = &decimalNullable
	editedEntity.Float64Unsigned = 8932.299423
	editedEntity.Float64Signed = -352.120321
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 0, time.UTC)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC)
	timeNullable := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	timeWithTimeNullable := time.Date(2025, 11, 4, 21, 0, 5, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	editedEntity.FlushStruct.Name2 = "Tom"
	editedEntity.FlushStruct.Age = 23
	editedEntity.FlushStruct.Sub.Name3 = "Zoya"
	editedEntity.FlushStruct.Sub.Age3 = 18
	testTime := time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
	editedEntity.FlushStruct.TestTime = &testTime
	int8Nullable := int8(23)
	editedEntity.Int8Nullable = &int8Nullable
	int16Nullable := int16(-29)
	editedEntity.Int16Nullable = &int16Nullable
	int32Nullable := int32(-2923)
	editedEntity.Int32Nullable = &int32Nullable
	int64Nullable := int64(98872)
	editedEntity.Int64Nullable = &int64Nullable
	uint8Nullable := uint8(23)
	editedEntity.Uint8Nullable = &uint8Nullable
	uint16Nullable := uint16(29)
	editedEntity.Uint16Nullable = &uint16Nullable
	uint32Nullable := uint32(2923)
	editedEntity.Uint32Nullable = &uint32Nullable
	uint64Nullable := uint64(98872)
	editedEntity.Uint64Nullable = &uint64Nullable
	editedEntity.SubName = "sub name"
	editedEntity.SubAge = 123
	editedEntity.Reference = Reference[flushEntityReference](reference.ID)
	editedEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	editedEntity.TestJsons.Set(&flushStructJSON{A: "F", B: 13})
	for i := 0; i < 2; i++ {
		editedEntity.StringArray[i] = fmt.Sprintf("Test %d", i)
		editedEntity.IntArray[i] = i + 1
		editedEntity.UintArray[i] = uint(i + 1)
		editedEntity.UintNullableArray[i] = &editedEntity.UintArray[i]
		editedEntity.IntNullableArray[i] = &editedEntity.IntArray[i]
		editedEntity.BoolArray[i] = true
		editedEntity.BoolNullableArray[i] = &editedEntity.BoolArray[i]
		editedEntity.Float64Array[i] = float64(i + 1)
		editedEntity.Float32Array[i] = float32(i + 1)
		editedEntity.DecimalArray[i] = float64(i + 1)
		editedEntity.Float64UnsignedArray[i] = float64(i + 1)
		editedEntity.Float64SignedArray[i] = float64(i + 1)
		editedEntity.Float32NullableArray[i] = &editedEntity.Float32Array[i]
		editedEntity.SetNullableArray[i] = []testSet{testSetDefinition.E, testSetDefinition.F}
		editedEntity.EnumNullableArray[i] = testEnumDefinition.C
		editedEntity.BlobArray[i] = []byte(fmt.Sprintf("Test %d", i))
		editedEntity.DecimalNullableArray[i] = &editedEntity.DecimalArray[i]
		editedEntity.TimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		editedEntity.TimeWithTimeArray[i] = time.Date(1982, 11, 4, 21, 0, 5, 6, time.UTC)
		editedEntity.TimeNullableArray[i] = &editedEntity.TimeWithTimeArray[i]
		editedEntity.TimeWithTimeNullableArray[i] = &editedEntity.TimeWithTimeArray[i]
		editedEntity.Uint32NullableArray[i] = &uint32Nullable
		editedEntity.ReferenceArray[i] = Reference[flushEntityReference](reference.ID)
		editedEntity.FlushStructArray[i].Age = i + 1
		editedEntity.FlushStructArray[i].Name2 = fmt.Sprintf("Name %d", i)
		editedEntity.FlushStructArray[i].Sub.Name3 = fmt.Sprintf("Name %d", i)
		editedEntity.FlushStructArray[i].Sub.Age3 = i + 1
	}

	oldValues, newValues, isDirty, err := IsDirty[flushEntity](orm, uint64(editedEntity.ID))
	assert.NoError(t, err)
	assert.True(t, isDirty)
	assert.NotNil(t, oldValues)
	assert.NotNil(t, newValues)
	assert.Len(t, oldValues, 94)
	assert.Len(t, newValues, 94)

	loggerLocal.Clear()
	assert.NoError(t, testFlush(orm, async))
	if !async {
		assert.Len(t, loggerDB.Logs, 1)
	} else {
		assert.Len(t, loggerDB.Logs, 0)
		err = runAsyncConsumer(orm)
		assert.NoError(t, err)
	}
	loggerDB.Clear()
	loggerLocal.Clear()
	entity, _, err := GetByID[flushEntity](orm, uint64(editedEntity.ID))
	assert.NoError(t, err)
	if local || redis {
		assert.Len(t, loggerDB.Logs, 0)
	}
	assert.NotNil(t, entity)
	assert.Equal(t, editedEntity.ID, entity.ID)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, "Test name", entity.Name)
	assert.Equal(t, -19, entity.Age)
	assert.Equal(t, uint(134), entity.Uint)
	assert.Equal(t, uint(23), *entity.UintNullable)
	assert.Equal(t, -45, *entity.IntNullable)
	assert.True(t, *entity.BoolNullable)
	assert.Equal(t, 12.23, *entity.FloatNullable)
	assert.Equal(t, float32(12.24), *entity.Float32Nullable)
	assert.Equal(t, []testSet{testSetDefinition.E, testSetDefinition.F}, entity.SetNullable)
	assert.Equal(t, []testSet{testSetDefinition.D, testSetDefinition.F}, entity.SetNotNull)
	assert.Equal(t, testEnumDefinition.C, entity.EnumNullable)
	assert.Equal(t, testEnumDefinition.A, entity.EnumNotNull)
	assert.Equal(t, []byte("test binary"), entity.Blob)
	assert.True(t, entity.Bool)
	assert.Equal(t, 986.2322, entity.Float64)
	assert.Equal(t, 78.24, entity.Decimal)
	assert.Equal(t, 123.23, *entity.DecimalNullable)
	assert.Equal(t, 8932.299423, entity.Float64Unsigned)
	assert.Equal(t, -352.120321, entity.Float64Signed)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), entity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), entity.TimeWithTime)
	assert.Equal(t, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), *entity.TimeNullable)
	assert.Equal(t, time.Date(2025, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullable)
	assert.Equal(t, "Tom", entity.FlushStruct.Name2)
	assert.Equal(t, 23, entity.FlushStruct.Age)
	assert.Equal(t, "Zoya", entity.FlushStruct.Sub.Name3)
	assert.Equal(t, 18, entity.FlushStruct.Sub.Age3)
	assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.FlushStruct.TestTime)
	assert.Equal(t, int8(23), *entity.Int8Nullable)
	assert.Equal(t, int16(-29), *entity.Int16Nullable)
	assert.Equal(t, int32(-2923), *entity.Int32Nullable)
	assert.Equal(t, int64(98872), *entity.Int64Nullable)
	assert.Equal(t, uint8(23), *entity.Uint8Nullable)
	assert.Equal(t, uint16(29), *entity.Uint16Nullable)
	assert.Equal(t, uint32(2923), *entity.Uint32Nullable)
	assert.Equal(t, uint64(98872), *entity.Uint64Nullable)
	assert.Equal(t, "sub name", entity.SubName)
	assert.Equal(t, float32(123), entity.SubAge)
	assert.Equal(t, reference.ID, entity.Reference.GetID())
	assert.Equal(t, reference.ID, entity.ReferenceRequired.GetID())
	assert.NotNil(t, entity.TestJsons.Get())
	assert.Equal(t, "F", entity.TestJsons.Get().A)
	assert.Equal(t, uint64(13), entity.TestJsons.Get().B)
	for i := 0; i < 2; i++ {
		assert.Equal(t, fmt.Sprintf("Test %d", i), entity.StringArray[i])
		assert.Equal(t, i+1, entity.IntArray[i])
		assert.Equal(t, uint(i+1), entity.UintArray[i])
		assert.Equal(t, uint(i+1), *entity.UintNullableArray[i])
		assert.Equal(t, i+1, *entity.IntNullableArray[i])
		assert.True(t, *entity.BoolNullableArray[i])
		assert.Equal(t, float32(i+1), *entity.Float32NullableArray[i])
		assert.Equal(t, []testSet{testSetDefinition.E, testSetDefinition.F}, entity.SetNullableArray[i])
		assert.Equal(t, testEnumDefinition.C, entity.EnumNullableArray[i])
		assert.Equal(t, []byte(fmt.Sprintf("Test %d", i)), entity.BlobArray[i])
		assert.True(t, entity.BoolArray[i])
		assert.Equal(t, float64(i+1), entity.Float64Array[i])
		assert.Equal(t, float32(i+1), entity.Float32Array[i])
		assert.Equal(t, float64(i+1), entity.DecimalArray[i])
		assert.Equal(t, float64(i+1), *entity.DecimalNullableArray[i])
		assert.Equal(t, float64(i+1), entity.Float64UnsignedArray[i])
		assert.Equal(t, float64(i+1), entity.Float64SignedArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), entity.TimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), entity.TimeWithTimeArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 0, 0, 0, 0, time.UTC), *entity.TimeNullableArray[i])
		assert.Equal(t, time.Date(1982, 11, 4, 21, 0, 5, 0, time.UTC), *entity.TimeWithTimeNullableArray[i])
		assert.Equal(t, uint32(2923), *entity.Uint32NullableArray[i])
		assert.Equal(t, entity.Reference, entity.ReferenceArray[i])
		assert.Equal(t, i+1, entity.FlushStructArray[i].Age)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Name2)
		assert.Equal(t, fmt.Sprintf("Name %d", i), entity.FlushStructArray[i].Sub.Name3)
		assert.Equal(t, i+1, entity.FlushStructArray[i].Sub.Age3)
	}
	if local {
		assert.Len(t, loggerDB.Logs, 0)
		assert.Len(t, loggerLocal.Logs, 1)
	}

	loggerDB.Clear()
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// rounding dates
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	editedEntity.Name = "rounding dates"
	editedEntity.City = "rounding dates"
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, testFlush(orm, async))
	if !async {
		assert.Len(t, loggerDB.Logs, 1)
	}
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), editedEntity.Time)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), editedEntity.TimeWithTime)
	assert.Equal(t, time.Date(2023, 11, 12, 0, 0, 0, 0, time.UTC), *editedEntity.TimeNullable)
	assert.Equal(t, time.Date(2023, 8, 16, 12, 23, 11, 0, time.UTC), *editedEntity.TimeWithTimeNullable)

	// same dates
	loggerDB.Clear()
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Time = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	timeNullable = time.Date(2023, 11, 12, 22, 12, 34, 4, time.UTC)
	editedEntity.TimeNullable = &timeNullable
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// same times
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.TimeWithTime = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	timeWithTimeNullable = time.Date(2023, 8, 16, 12, 23, 11, 6, time.UTC)
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// rounding floats
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Name = "rounding floats"
	editedEntity.City = "rounding floats"
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, testFlush(orm, async))
	if !async {
		assert.Len(t, loggerDB.Logs, 1)
	}
	assert.Equal(t, 1.12346, editedEntity.Float64)
	assert.Equal(t, 1.12, editedEntity.Decimal)
	assert.Equal(t, 1.123, *editedEntity.FloatNullable)
	assert.Equal(t, 1.13, *editedEntity.DecimalNullable)
	loggerDB.Clear()

	// same floats
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Float64 = 1.123456
	editedEntity.Decimal = 1.123
	floatNullable = 1.1234
	editedEntity.FloatNullable = &floatNullable
	decimalNullable = 1.126
	editedEntity.DecimalNullable = &decimalNullable
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// same set
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.SetNullable = []testSet{testSetDefinition.E, testSetDefinition.F}
	editedEntity.SetNotNull = []testSet{testSetDefinition.D, testSetDefinition.F}
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.SetNullable = []testSet{testSetDefinition.E, testSetDefinition.F}
	editedEntity.SetNotNull = []testSet{testSetDefinition.D, testSetDefinition.F}
	assert.NoError(t, testFlush(orm, async))
	assert.Len(t, loggerDB.Logs, 0)

	// copy entity
	copiedEntity, err := Copy(orm, editedEntity)
	assert.NoError(t, err)
	assert.NotNil(t, copiedEntity)
	assert.NotEqual(t, copiedEntity.ID, editedEntity.ID)
	assert.Equal(t, copiedEntity.Name, editedEntity.Name)
	copiedEntity.City = "Copy"
	copiedEntity.Name = "Copy"
	assert.NoError(t, orm.Flush())
	copiedEntity, found, err := GetByID[flushEntity](orm, uint64(copiedEntity.ID))
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, copiedEntity)
	assert.Equal(t, copiedEntity.Age, editedEntity.Age)

	// invalid values

	// empty string
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Name = ""
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Name] empty string not allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	orm.ClearFlush()

	// string too long
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Name = strings.Repeat("a", 256)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Name] text too long, max 255 allowed")
	assert.Equal(t, "Name", err.(*BindError).Field)
	orm.ClearFlush()
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.City = strings.Repeat("a", 41)
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[City] text too long, max 40 allowed")
	editedEntity.City = strings.Repeat("a", 40)
	editedEntity.Name = "String to long"
	assert.NoError(t, testFlush(orm, async))

	// invalid decimal
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Decimal = 1234
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Decimal] decimal size too big, max 3 allowed")
	assert.Equal(t, "Decimal", err.(*BindError).Field)
	orm.ClearFlush()
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Decimal = 123
	decimalNullable = 1234
	editedEntity.DecimalNullable = &decimalNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[DecimalNullable] decimal size too big, max 3 allowed")
	assert.Equal(t, "DecimalNullable", err.(*BindError).Field)
	orm.ClearFlush()
	decimalNullable = 123
	editedEntity.DecimalNullable = &decimalNullable

	// float signed
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Float64Unsigned = -1
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Float64Unsigned] negative value not allowed")
	assert.Equal(t, "Float64Unsigned", err.(*BindError).Field)
	editedEntity.Float64Unsigned = 1
	floatNullable = -1
	editedEntity.FloatNullable = &floatNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[FloatNullable] negative value not allowed")
	assert.Equal(t, "FloatNullable", err.(*BindError).Field)
	orm.ClearFlush()
	floatNullable = 1
	editedEntity.FloatNullable = &floatNullable

	// invalid enum, set
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.EnumNotNull = ""
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[EnumNotNull] empty value not allowed")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = "invalid"
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[EnumNotNull] invalid value: invalid")
	assert.Equal(t, "EnumNotNull", err.(*BindError).Field)
	editedEntity.EnumNotNull = testEnumDefinition.C
	editedEntity.SetNotNull = nil
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testSet{}
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] empty value not allowed")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testSet{"invalid"}
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[SetNotNull] invalid value: invalid")
	assert.Equal(t, "SetNotNull", err.(*BindError).Field)
	editedEntity.SetNotNull = []testSet{testSetDefinition.E}
	orm.ClearFlush()

	// Time
	editedEntity, err = EditEntity(orm, editedEntity)
	assert.NoError(t, err)
	editedEntity.Time = time.Now().Local()
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[Time] time must be in UTC location")
	assert.Equal(t, "Time", err.(*BindError).Field)
	editedEntity.Time = newEntity.Time.UTC()
	editedEntity.TimeWithTime = time.Now().Local()
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeWithTime] time must be in UTC location")
	assert.Equal(t, "TimeWithTime", err.(*BindError).Field)

	// nullable times
	editedEntity.TimeWithTime = editedEntity.Time.UTC()
	timeNullable = time.Now().Local()
	editedEntity.TimeNullable = &timeNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().Local()
	timeNullable = time.Now().UTC()
	editedEntity.TimeNullable = &timeNullable
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	err = testFlush(orm, async)
	assert.EqualError(t, err, "[TimeWithTimeNullable] time must be in UTC location")
	assert.Equal(t, "TimeWithTimeNullable", err.(*BindError).Field)
	timeWithTimeNullable = time.Now().UTC()
	editedEntity.TimeWithTimeNullable = &timeWithTimeNullable
	orm.ClearFlush()

	// duplicated key
	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, testFlush(orm, async))

	if !async {
		editedEntity, err = EditEntity(orm, editedEntity)
		assert.NoError(t, err)
		editedEntity.Name = "Name 2"
		err = testFlush(orm, async)
		assert.EqualError(t, err, "Duplicate entry 'Name 2' for key 'flushEntity.Name'")
		orm.ClearFlush()
	}

	editedEntity, err = EditEntity(orm, newEntity)
	assert.NoError(t, err)
	editedEntity.Name = "Name 3"
	assert.NoError(t, testFlush(orm, async))

	newEntity, err = NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	newEntity.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	newEntity.Name = "Name 2"
	assert.NoError(t, testFlush(orm, async))
}

func TestFlushTransaction(t *testing.T) {
	registry := NewRegistry()
	orm := PrepareTables(t, registry, flushEntity{}, flushEntityReference{})

	schema, err := GetEntitySchema[flushEntity](orm)
	assert.NoError(t, err)
	schema.DisableCache(true, true)

	loggerDB := &MockLogHandler{}
	orm.RegisterQueryLogger(loggerDB, true, false, false)

	reference, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference"
	err = testFlush(orm, false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 2)
	loggerDB.Clear()

	reference, err = NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference 2"
	reference2, err := NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference2.Name = "test reference 3"
	err = testFlush(orm, false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 1)
	loggerDB.Clear()

	reference, err = NewEntity[flushEntityReference](orm)
	assert.NoError(t, err)
	reference.Name = "test reference 2"
	flushE, err := NewEntity[flushEntity](orm)
	assert.NoError(t, err)
	flushE.Name = "test"
	flushE.ReferenceRequired = Reference[flushEntityReference](reference.ID)
	err = testFlush(orm, false)
	assert.NoError(t, err)
	assert.Len(t, loggerDB.Logs, 5)
	assert.Equal(t, "START TRANSACTION", loggerDB.Logs[1]["query"])
	assert.Equal(t, "COMMIT", loggerDB.Logs[4]["query"])
	loggerDB.Clear()

	// Skipping invalid event
}

func testFlush(ctx Context, async bool) error {
	if async {
		return ctx.FlushAsync()
	}
	return ctx.Flush()
}
