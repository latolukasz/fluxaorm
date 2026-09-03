package test_generate

import (
	"testing"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/stretchr/testify/assert"
)

// The structs below exist only to drive Validate. They are not in
// FixtureEntities, so they have no generated code and never reach a query.

type oldDirtyTagEntity struct {
	ID   uint64 `orm:"dirty=some-stream"`
	Name string `orm:"required;length=100"`
}

type unconsumedCdcEntity struct {
	ID   uint64 `orm:"cdc"`
	Name string `orm:"required;length=100"`
}

type consumedCdcEntity struct {
	ID   uint64 `orm:"cdc"`
	Name string `orm:"required;length=100"`
}

type untaggedEntity struct {
	ID   uint64
	Name string `orm:"required;length=100"`
}

func validateRegistry(entities []any, consumers ...fluxaorm.ConsumerDef) error {
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(entities...)
	for _, def := range consumers {
		registry.RegisterConsumer(def)
	}
	_, err := registry.Validate()

	return err
}

// TestValidateRejectsTheOldDirtyTag stops an entity being half-migrated. The
// tag used to name the entity's readers; leaving it accepted-but-ignored would
// mean an entity that silently stopped publishing.
func TestValidateRejectsTheOldDirtyTag(t *testing.T) {
	err := validateRegistry([]any{oldDirtyTagEntity{}})

	assert.ErrorContains(t, err, "no longer exists")
	assert.ErrorContains(t, err, "oldDirtyTagEntity")
	assert.ErrorContains(t, err, "ConsumerDef")
}

// TestValidateRejectsACdcEntityNobodyConsumes is the inverse of the old orphan
// check. A tagged entity publishes on every write, so one no consumer filters
// is bytes on the stream that nothing will ever read.
func TestValidateRejectsACdcEntityNobodyConsumes(t *testing.T) {
	err := validateRegistry([]any{unconsumedCdcEntity{}})

	assert.ErrorContains(t, err, "no consumer declares it")
	assert.ErrorContains(t, err, "unconsumedCdcEntity")
}

func TestValidateRejectsAConsumerDeclaringAnUnregisteredEntity(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}, untaggedEntity{}}},
	)

	assert.ErrorContains(t, err, "is not registered")
	assert.ErrorContains(t, err, "untaggedEntity")
}

// TestValidateRejectsAConsumerDeclaringAnUntaggedEntity covers the case that
// otherwise fails silently: the filter subject exists, but nothing publishes to
// it, so the handler simply never fires.
func TestValidateRejectsAConsumerDeclaringAnUntaggedEntity(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}, untaggedEntity{}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}, untaggedEntity{}}},
	)

	assert.ErrorContains(t, err, "not tagged")
	assert.ErrorContains(t, err, "untaggedEntity")
}

func TestValidateRejectsADuplicateConsumerName(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "declared twice")
}

// TestValidateRejectsAConsumerWithOverlappingFilters catches at boot what
// JetStream would otherwise reject at reconcile time, when the error is a long
// way from the declaration that caused it.
func TestValidateRejectsAConsumerWithOverlappingFilters(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}, consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "twice")
	assert.ErrorContains(t, err, "overlap")
}

func TestValidateRejectsABadConsumerName(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "Order_Indexer", Entities: []any{consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "must match")
}

func TestValidateRejectsAConsumerWithNoEntities(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
		fluxaorm.ConsumerDef{Name: "idle"},
	)

	assert.ErrorContains(t, err, "declares neither entities nor queues")
}

func TestValidateAcceptsAnEntityOnSeveralConsumers(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		fluxaorm.ConsumerDef{Name: "reader-one", Entities: []any{consumedCdcEntity{}}},
		fluxaorm.ConsumerDef{Name: "reader-two", Entities: []any{consumedCdcEntity{}}},
	)

	assert.NoError(t, err, "several consumers reading one entity is the whole point of subject filters")
}
