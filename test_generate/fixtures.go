package test_generate

import (
	"time"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_fixtures/jobtasks"
	mediatasks "github.com/latolukasz/fluxaorm/v2/test_fixtures/media/jobtasks"
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

// generateEntityDirty is the CDC test entity. Two consumers declare it, which
// is the case that used to publish two copies of every write.
type generateEntityDirty struct {
	ID   uint64 `orm:"cdc"`
	Name string `orm:"required;length=100"`
	Age  uint16
}

// generateEntityDirtyB shares a consumer with generateEntityDirty so the
// group-by-subject batching has more than one group to split.
type generateEntityDirtyB struct {
	ID    uint64 `orm:"cdc"`
	Label string `orm:"required;length=100"`
}

// generateEntityOutbox is cdc + outbox: durable delivery. Its relayed row has
// to reach the same subject the inline publish would have used.
type generateEntityOutbox struct {
	ID   uint64 `orm:"cdc;outbox"`
	Name string `orm:"required;length=100"`
	Age  uint16
}

// generateEntityStoreOnly is outbox without cdc: a transactional change log
// with no subject to publish to.
type generateEntityStoreOnly struct {
	ID   uint64 `orm:"outbox"`
	Name string `orm:"required;length=100"`
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

// FixtureConsumers are the consumers the fixtures declare.
//
// test-indexer takes three entities so the multi-subject filter path is
// exercised, and generateEntityDirty is on both consumers so "one write, one
// message however many consumers" has something to prove.
func FixtureConsumers() []fluxaorm.ConsumerDef {
	return append([]fluxaorm.ConsumerDef{
		{
			Name: "test-indexer",
			Entities: []any{
				generateEntityDirty{}, generateEntityDirtyB{}, generateEntityOutbox{},
			},
		},
		{
			Name:     "test-notifier",
			Entities: []any{generateEntityDirty{}},
		},
	}, FixtureTaskConsumers()...)
}

// FixtureEntities is the registration list shared by TestGenerate and genboot.
func FixtureEntities() []any {
	return []any{
		generateEntity{}, generateEntityNoRedis{}, generateReferenceEntity{},
		generateEntityWithSearch{}, generateEntityWithTimestamps{}, generateEntityWithTimestampsRedis{},
		generateEntityCachedUnique{}, generateEntityCachedUniqueNoRedis{}, generateEntityCachedUniqueFakeDelete{},
		generateEntityWithIndex{}, generateEntityEnumRef{}, generateEntityDirty{}, generateEntityDirtyB{},
		generateEntityOutbox{}, generateEntityStoreOnly{}, fluxaorm.CDCOutboxEntity{},
		fluxaorm.JobRunEntity{},
	}
}

// FixtureRegistry is a registry with the fixture tasks already registered.
// FixtureConsumers declares consumers for their queues, and a queue with no
// tasks is a startup error, so the two always travel together.
func FixtureRegistry() fluxaorm.Registry {
	registry := fluxaorm.NewRegistry()
	for _, task := range FixtureTasks() {
		registry.RegisterTask(task.Task, task.Options)
	}

	return registry
}

// FixtureTasks are the tasks the generator emits dispatch functions and
// consumer builders for. They live in their own leaf packages because the
// generated package imports them, and a task package that imported anything
// generated would close a cycle.
//
// The two packages deliberately share the base name `jobtasks`, so the
// generator's import aliasing has something to disambiguate.
func FixtureTasks() []FixtureTask {
	return []FixtureTask{
		{Task: jobtasks.SendWelcomeEmail{}},
		{Task: jobtasks.SendReceipt{}},
		{Task: jobtasks.SendPasswordReset{}},
		{Task: mediatasks.TranscodeClip{}, Options: fluxaorm.TaskOptions{MaxAttempts: 3}},
	}
}

type FixtureTask struct {
	Task    any
	Options fluxaorm.TaskOptions
}

// FixtureTaskConsumers drain the fixture queues. `default` is declared because
// SendPasswordReset names no queue and falls through to it - a queue nothing
// drains is a startup error.
func FixtureTaskConsumers() []fluxaorm.ConsumerDef {
	return []fluxaorm.ConsumerDef{
		{Name: "emails-worker", Queues: []fluxaorm.Queue{"emails"}},
		{Name: "media-worker", Queues: []fluxaorm.Queue{"media"}},
		{Name: "default-worker", Queues: []fluxaorm.Queue{fluxaorm.DefaultQueue}},
	}
}
