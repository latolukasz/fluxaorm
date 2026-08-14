package test_generate

import (
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/models"
)

// The fixtures live outside _test.go so cmd/genboot can regenerate entities/
// without compiling the test binary - which imports the very package the
// generator writes. Without that split, one bad generator change bricks the
// only path back to a good one.

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
	ID         uint64
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

// generateEntityDirty is the CDC test entity. Tagged into two streams so we
// can exercise both single-stream-many-entities and many-streams-per-entity flows.
type generateEntityDirty struct {
	ID   uint64 `orm:"dirty=test_stream,test_stream_b"`
	Name string `orm:"required;length=100"`
	Age  uint16
}

// generateEntityDirtyB shares test_stream with generateEntityDirty so the
// consumer's group-by-entity batching has more than one group to split.
type generateEntityDirtyB struct {
	ID    uint64 `orm:"dirty=test_stream"`
	Label string `orm:"required;length=100"`
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

// FixtureCDCStreams lists the CDC streams the fixtures publish to.
func FixtureCDCStreams() []fluxaorm.CDCStream {
	return []fluxaorm.CDCStream{
		fluxaorm.NewCDCStreamByName("test_stream"),
		fluxaorm.NewCDCStreamByName("test_stream_b"),
	}
}

// FixtureEntities is the registration list shared by TestGenerate and genboot.
func FixtureEntities() []any {
	return []any{
		generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{},
		generateEntityWithSearch{}, generateEntityWithTimestamps{}, generateEntityWithTimestampsRedis{},
		generateEntityCachedUnique{}, generateEntityCachedUniqueNoRedis{}, generateEntityCachedUniqueFakeDelete{},
		generateEntityWithIndex{}, generateEntityEnumRef{}, generateEntityDirty{}, generateEntityDirtyB{},
	}
}
