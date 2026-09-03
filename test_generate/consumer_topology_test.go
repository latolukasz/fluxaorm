package test_generate

import (
	"context"
	"testing"
	"time"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsumerCarriesItsStreamExplicitly pins the fix that multi-subject
// consumers force. The reconciler used to find a durable's stream by scanning
// for one that covered filterSubjects[0], which is guesswork the moment a
// consumer filters more than one subject.
func TestConsumerCarriesItsStreamExplicitly(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithConsumers(t, FixtureRegistry(), FixtureConsumers(),
		generateEntityDirty{}, generateEntityDirtyB{}, generateEntityOutbox{},
		fluxaorm.CDCOutboxEntity{}, fluxaorm.JobRunEntity{})
	defer ctx.Engine().Nats("nats").Close()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)

	cons, err := js.Consumer(ctx.Context(), fluxaorm.EntityStreamName, "test-indexer")
	require.NoError(t, err, "the durable must be created on the stream its builder names")

	info, err := cons.Info(ctx.Context())
	require.NoError(t, err)

	assert.Equal(t, []string{
		"fluxa.entity.generateEntityDirty",
		"fluxa.entity.generateEntityDirtyB",
		"fluxa.entity.generateEntityOutbox",
		"fluxa.replay.test-indexer.>",
	}, info.Config.FilterSubjects, "a multi-entity consumer needs the plural filter list")
	assert.Empty(t, info.Config.FilterSubject, "the singular field is for one-filter consumers only")
}

// TestEntityStreamCarriesBothWildcards is why adding an entity never touches
// the stream: its subjects are wildcards, so only the consumer's filter list
// changes.
func TestEntityStreamCarriesBothWildcards(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithConsumers(t, FixtureRegistry(), FixtureConsumers(),
		generateEntityDirty{}, generateEntityDirtyB{}, generateEntityOutbox{},
		fluxaorm.CDCOutboxEntity{}, fluxaorm.JobRunEntity{})
	defer ctx.Engine().Nats("nats").Close()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	stream, err := js.Stream(ctx.Context(), fluxaorm.EntityStreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx.Context())
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"fluxa.entity.>", "fluxa.replay.>"}, info.Config.Subjects)
	assert.Empty(t, alterDescriptions(t, ctx), "a second reconcile of an unchanged topology must be a no-op")
}

// TestAddingAnEntityUpdatesTheDurableInPlace is the test that makes "add an
// entity to a consumer" a safe operation. If the durable were deleted and
// recreated it would lose its position, so anything unacked at that moment
// would either vanish or be redelivered from the start of the stream.
func TestAddingAnEntityUpdatesTheDurableInPlace(t *testing.T) {
	ctx := fluxaorm.PrepareTablesWithConsumers(t, fluxaorm.NewRegistry(),
		[]fluxaorm.ConsumerDef{{Name: "test-indexer", Entities: []any{generateEntityDirty{}}}},
		generateEntityDirty{})
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyProvider.New(ctx)
	e.SetName("in-place")
	e.SetAge(1)
	require.NoError(t, ctx.Save(e))

	require.Eventually(t, func() bool {
		pending, err := fluxaorm.ConsumerPending(ctx, "test-indexer")

		return err == nil && pending == 1
	}, 30*time.Second, 200*time.Millisecond, "the write never reached the consumer")

	// Second engine against the same NATS, declaring one more entity. Built by
	// hand rather than through the test helper because that helper purges the
	// stream, which would throw away the very message this test is watching.
	widened := reconcileConsumers(t,
		[]fluxaorm.ConsumerDef{{
			Name:     "test-indexer",
			Entities: []any{generateEntityDirty{}, generateEntityDirtyB{}},
		}},
		generateEntityDirty{}, generateEntityDirtyB{})
	defer widened.Engine().Nats("nats").Close()

	js, err := widened.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	cons, err := js.Consumer(widened.Context(), fluxaorm.EntityStreamName, "test-indexer")
	require.NoError(t, err)
	info, err := cons.Info(widened.Context())
	require.NoError(t, err)

	assert.Contains(t, info.Config.FilterSubjects, "fluxa.entity.generateEntityDirtyB",
		"the widened filter list must be applied")
	assert.Equal(t, uint64(1), info.NumPending,
		"the durable kept its position, so the message published before the change is still owed")
}

// reconcileConsumers stands up a second engine on the same NATS and applies its
// topology, without touching MySQL or purging the stream.
func reconcileConsumers(t *testing.T, consumers []fluxaorm.ConsumerDef, entities ...any) fluxaorm.Context {
	t.Helper()

	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterEntityStream(fluxaorm.EntityStreamOptions{NatsPool: "nats"})
	registry.RegisterEntity(entities...)
	for _, def := range consumers {
		def.NatsPool = "nats"
		registry.RegisterConsumer(def)
	}

	engine, err := registry.Validate()
	require.NoError(t, err)

	ctx := engine.NewContext(context.Background())
	alters, err := fluxaorm.GetNatsAlters(ctx)
	require.NoError(t, err)
	for _, alter := range alters {
		require.NoError(t, alter.Exec(ctx))
	}

	return ctx
}

func alterDescriptions(t *testing.T, ctx fluxaorm.Context) []string {
	t.Helper()

	alters, err := fluxaorm.GetNatsAlters(ctx)
	require.NoError(t, err)

	out := make([]string, 0, len(alters))
	for _, alter := range alters {
		// Ensuring a consumer is idempotent and always emitted; only stream
		// create/update/delete says the topology actually drifted.
		if alter.Description[:6] == "ENSURE" {
			continue
		}
		out = append(out, alter.Description)
	}

	return out
}

// TestStreamMaxBytesIsApplied covers the bound droplet sets in registerStreams:
// replay shares the entity stream, so an unbounded reindex would either evict
// live events or fill the disk. A stale bound also has to reconcile in place.
func TestStreamMaxBytesIsApplied(t *testing.T) {
	const bound = 64 << 20

	ctx := reconcileStreams(t, bound)
	defer ctx.Engine().Nats("nats").Close()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)

	for _, name := range []string{fluxaorm.EntityStreamName, fluxaorm.TaskStreamName} {
		stream, err := js.Stream(ctx.Context(), name)
		require.NoError(t, err)
		info, err := stream.Info(ctx.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(bound), info.Config.MaxBytes, name)
	}

	assert.Empty(t, alterDescriptions(t, ctx), "a second reconcile of an unchanged bound must be a no-op")

	widened := reconcileStreams(t, bound*2)
	defer widened.Engine().Nats("nats").Close()

	stream, err := js.Stream(widened.Context(), fluxaorm.EntityStreamName)
	require.NoError(t, err)
	info, err := stream.Info(widened.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(bound*2), info.Config.MaxBytes, "raising the bound must update in place")
}

// reconcileStreams applies the fixture topology carrying an explicit MaxBytes on
// both streams, without touching MySQL or purging them.
func reconcileStreams(t *testing.T, maxBytes int64) fluxaorm.Context {
	t.Helper()

	registry := FixtureRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterEntityStream(fluxaorm.EntityStreamOptions{NatsPool: "nats", MaxBytes: maxBytes})
	registry.RegisterTaskStream(fluxaorm.TaskStreamOptions{NatsPool: "nats", MaxBytes: maxBytes})
	registry.RegisterEntity(FixtureEntities()...)
	for _, def := range FixtureConsumers() {
		def.NatsPool = "nats"
		registry.RegisterConsumer(def)
	}

	engine, err := registry.Validate()
	require.NoError(t, err)

	ctx := engine.NewContext(context.Background())
	alters, err := fluxaorm.GetNatsAlters(ctx)
	require.NoError(t, err)
	for _, alter := range alters {
		require.NoError(t, alter.Exec(ctx))
	}

	return ctx
}
