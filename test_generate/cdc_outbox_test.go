package test_generate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	fluxaorm "github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities/enums"
	"github.com/stretchr/testify/assert"
)

// outboxOnSecondPool exists only to prove Validate rejects an outbox that cannot
// share the source write's transaction. It is not in FixtureEntities, so it has
// no generated code and never reaches a query.
type outboxOnSecondPool struct {
	ID   uint64 `orm:"mysql=second;outbox"`
	Name string `orm:"required;length=100"`
}

func outboxCtx(t *testing.T) fluxaorm.Context {
	t.Helper()

	return fluxaorm.PrepareTablesWithCDC(t,
		fluxaorm.NewRegistry(),
		[]fluxaorm.CDCStream{entities.StreamTestStream, entities.StreamTestStreamB},
		generateEntityOutbox{}, generateEntityStoreOnly{}, generateEntityDirty{}, fluxaorm.CDCOutboxEntity{},
	)
}

func outboxRows(t *testing.T, ctx fluxaorm.Context) []*entities.CdcOutbox {
	t.Helper()

	rows, err := entities.CdcOutboxProvider.SearchMany(ctx, fluxaorm.NewQuery().
		SortByASC(entities.CdcOutboxProvider.Fields.ID).
		Pager(fluxaorm.NewPager(1, 100)))
	assert.NoError(t, err)

	return rows
}

// onlyOutboxRow fails the test outright when the row count is wrong, so a
// staging regression reports as a clean assertion rather than an index panic.
func onlyOutboxRow(t *testing.T, ctx fluxaorm.Context) *entities.CdcOutbox {
	t.Helper()

	rows := outboxRows(t, ctx)
	if !assert.Len(t, rows, 1) {
		t.FailNow()
	}

	return rows[0]
}

func newOutboxEntity(ctx fluxaorm.Context, name string) *entities.GenerateEntityOutbox {
	e := entities.GenerateEntityOutboxProvider.New(ctx)
	e.SetName(name)
	e.SetAge(30)

	return e
}

func TestOutboxRowRollsBackWithTheEntity(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	err := ctx.Transaction(func(tx fluxaorm.Context) error {
		if saveErr := tx.Save(newOutboxEntity(tx, "rollback-me")); saveErr != nil {
			return saveErr
		}

		return assert.AnError
	})
	assert.ErrorIs(t, err, assert.AnError)

	assert.Empty(t, outboxRows(t, ctx.Clone()), "outbox row must not survive a rolled back transaction")

	sources, err := entities.GenerateEntityOutboxProvider.SearchMany(ctx.Clone(),
		fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 100)))
	assert.NoError(t, err)
	assert.Empty(t, sources)
}

func TestOutboxRowIsAtomicWithoutAnExplicitTransaction(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	logs := captureQueries(ctx)
	e := newOutboxEntity(ctx, "no-explicit-tx")
	assert.NoError(t, ctx.Save(e))

	// A bare single-entity Save queues two statements, so DatabasePipeline opens
	// its own transaction rather than running them in autocommit.
	assert.Positive(t, logs.count("START TRANSACTION"),
		"entity INSERT and outbox INSERT must share a transaction even without ctx.Transaction")

	assert.Equal(t, e.GetID(), onlyOutboxRow(t, ctx.Clone()).GetEntityID())
}

func TestOutboxRowIsWrittenPending(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	// Seen from inside the transaction: the row is already there and it is
	// pending. Nothing has published at this point, so anything else would be a
	// claim of delivery the ORM cannot back.
	assert.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		if err := tx.Save(newOutboxEntity(tx, "pending-check")); err != nil {
			return err
		}

		row := onlyOutboxRow(t, tx)
		assert.Equal(t, enums.CDCOutboxStatusList.Pending, row.GetStatus())
		assert.Nil(t, row.GetDispatchedAt())

		return nil
	}))
}

func TestOutboxRowIsMarkedDispatchedAfterPublish(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "dispatched-check")))

	row := onlyOutboxRow(t, ctx.Clone())
	assert.Equal(t, enums.CDCOutboxStatusList.Dispatched, row.GetStatus())
	assert.NotNil(t, row.GetDispatchedAt())
}

func TestDispatchMarkIsOneUpdateForTheWholeSave(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	logs := captureQueries(ctx)
	assert.NoError(t, ctx.Save(
		newOutboxEntity(ctx, "batch-a"),
		newOutboxEntity(ctx, "batch-b"),
		newOutboxEntity(ctx, "batch-c"),
	))

	assert.Equal(t, 1, logs.count("UPDATE `cdc_outbox`"),
		"three staged rows must be marked by one UPDATE, not one per row")
	assert.Len(t, outboxRows(t, ctx.Clone()), 3)
}

func TestStoreOnlyEntityWritesATerminalRow(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	e.SetName("store-only")
	assert.NoError(t, ctx.Save(e))

	row := onlyOutboxRow(t, ctx.Clone())
	assert.Equal(t, enums.CDCOutboxStatusList.Stored, row.GetStatus())
	assert.Empty(t, row.GetStreams(), "a store-only row has no stream to publish to")
	assert.Empty(t, row.GetNatsPool())
	assert.Nil(t, row.GetDispatchedAt())
}

func TestStoreOnlyEntityGetsAGeneratedPublisher(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	e.SetName("payload-check")
	assert.NoError(t, ctx.Save(e))

	assert.Contains(t, onlyOutboxRow(t, ctx.Clone()).GetPayload(), "payload-check",
		"store-only entities need the generated snapshot builder too")
}

func TestDirtyWithoutOutboxWritesNoRow(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyProvider.New(ctx)
	e.SetName("dirty-only")
	e.SetAge(7)
	assert.NoError(t, ctx.Save(e))

	assert.Empty(t, outboxRows(t, ctx.Clone()),
		"an entity tagged dirty= without outbox keeps today's fire-and-forget behaviour")
}

func TestOneRowFansOutToEveryTaggedStream(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "fan-out")))

	row := onlyOutboxRow(t, ctx.Clone())
	assert.Equal(t, "test_stream,test_stream_b", row.GetStreams(),
		"one write is one row however many streams it feeds")
	assert.Equal(t, "nats", row.GetNatsPool())
}

func TestRolledBackSaveMarksNothingOnALaterSave(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.Error(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		if err := tx.Save(newOutboxEntity(tx, "doomed")); err != nil {
			return err
		}

		return assert.AnError
	}))

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "survivor")))

	assert.Equal(t, enums.CDCOutboxStatusList.Dispatched, onlyOutboxRow(t, ctx.Clone()).GetStatus(),
		"the rolled back save must leave no staged IDs behind")
}

func TestRelayPublishesRowsLeftPendingByAFailedPublish(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	killStreams(t, ctx)
	assert.Error(t, ctx.Save(newOutboxEntity(ctx, "relay-me")))
	assert.Equal(t, enums.CDCOutboxStatusList.Pending, onlyOutboxRow(t, ctx.Clone()).GetStatus())

	restoreStreams(t, ctx)

	res, err := fluxaorm.RelayCDCOutbox(ctx, 0, 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, res.Fetched)
	assert.Equal(t, 2, res.Published["nats"], "one row fans back out to both of its streams")

	assert.Equal(t, enums.CDCOutboxStatusList.Dispatched, onlyOutboxRow(t, ctx.Clone()).GetStatus())
}

func TestRelaySkipsRowsInsideTheGracePeriod(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	killStreams(t, ctx)
	assert.Error(t, ctx.Save(newOutboxEntity(ctx, "too-fresh")))
	restoreStreams(t, ctx)

	res, err := fluxaorm.RelayCDCOutbox(ctx, time.Hour, 100)
	assert.NoError(t, err)
	assert.Zero(t, res.Fetched, "a fresh row still belongs to the ORM's own post-commit mark")

	assert.Equal(t, enums.CDCOutboxStatusList.Pending, onlyOutboxRow(t, ctx.Clone()).GetStatus())
}

func TestRelayLeavesRowsPendingWhenPublishStillFails(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	killStreams(t, ctx)
	assert.Error(t, ctx.Save(newOutboxEntity(ctx, "still-broken")))

	res, err := fluxaorm.RelayCDCOutbox(ctx, 0, 100)
	assert.Error(t, err, "the caller has to learn the backlog is not draining")
	assert.Equal(t, 1, res.Fetched)
	assert.Empty(t, res.Published)

	row := onlyOutboxRow(t, ctx.Clone())
	assert.Equal(t, enums.CDCOutboxStatusList.Pending, row.GetStatus(),
		"a row may only be marked dispatched once its event is actually on the wire")
	assert.Nil(t, row.GetDispatchedAt())
}

func TestRelayIgnoresStoredRows(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	e.SetName("never-relayed")
	assert.NoError(t, ctx.Save(e))

	res, err := fluxaorm.RelayCDCOutbox(ctx, 0, 100)
	assert.NoError(t, err)
	assert.Zero(t, res.Fetched, "store-only rows are terminal and have no stream to publish to")

	assert.Equal(t, enums.CDCOutboxStatusList.Stored, onlyOutboxRow(t, ctx.Clone()).GetStatus())
}

func TestPurgeRemovesTerminalRowsOnly(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "delivered")))

	storeOnly := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	storeOnly.SetName("logged")
	assert.NoError(t, ctx.Save(storeOnly))

	// A negative window puts the cutoff in the future, so every terminal row
	// qualifies without the test having to fake a clock.
	deleted, err := fluxaorm.PurgeCDCOutbox(ctx, -time.Minute, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted, "dispatched and stored are both terminal")
	assert.Empty(t, outboxRows(t, ctx.Clone()))
}

func TestPurgeNeverRemovesPendingRows(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	killStreams(t, ctx)
	assert.Error(t, ctx.Save(newOutboxEntity(ctx, "undelivered")))

	// Same future cutoff: if pending were purgeable at all, this would take it.
	deleted, err := fluxaorm.PurgeCDCOutbox(ctx, -time.Hour, 100)
	assert.NoError(t, err)
	assert.Zero(t, deleted, "purging an undelivered event is the loss the outbox exists to prevent")

	assert.Equal(t, enums.CDCOutboxStatusList.Pending, onlyOutboxRow(t, ctx.Clone()).GetStatus())
}

func TestBacklogCountsOnlyUndeliveredRows(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "delivered")))

	depth, oldest, err := fluxaorm.CDCOutboxBacklog(ctx)
	assert.NoError(t, err)
	assert.Zero(t, depth, "a dispatched row is not a backlog")
	assert.Zero(t, oldest)

	killStreams(t, ctx)
	assert.Error(t, ctx.Save(newOutboxEntity(ctx, "stuck")))

	depth, oldest, err = fluxaorm.CDCOutboxBacklog(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, depth)
	assert.Positive(t, oldest)
}

func TestValidateRejectsCdcOutboxWithoutTheEntityRegistered(t *testing.T) {
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(generateEntityStoreOnly{})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "fluxaorm.CDCOutboxEntity is not registered")
	assert.ErrorContains(t, err, "generateEntityStoreOnly")
}

func TestValidateRejectsAnOutboxInAnotherPool(t *testing.T) {
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "second", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(outboxOnSecondPool{}, fluxaorm.CDCOutboxEntity{})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "would not be in the same transaction")
}

// queryLog records every MySQL statement executed on a context.
type queryLog struct{ queries []string }

func (q *queryLog) Handle(_ fluxaorm.Context, log map[string]any) {
	if s, ok := log["query"].(string); ok {
		q.queries = append(q.queries, s)
	}
}

func (q *queryLog) count(fragment string) int {
	n := 0
	for _, s := range q.queries {
		if strings.Contains(s, fragment) {
			n++
		}
	}

	return n
}

func captureQueries(ctx fluxaorm.Context) *queryLog {
	log := &queryLog{}
	ctx.RegisterQueryLogger(log, fluxaorm.QueryLoggerOptions{MySQL: true})

	return log
}

// killStreams removes the JetStream streams so PublishBatch fails. The next
// PrepareTablesWithCDC recreates them, so this stays local to one test.
func killStreams(t *testing.T, ctx fluxaorm.Context) {
	t.Helper()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	assert.NoError(t, err)
	for _, name := range []string{"FLUXA_DIRTY_test_stream", "FLUXA_DIRTY_test_stream_b"} {
		assert.NoError(t, js.DeleteStream(ctx.Context(), name))
	}
}

func TestOutboxRowStaysPendingOnPublishFailure(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	killStreams(t, ctx)

	e := newOutboxEntity(ctx, "publish-fails")
	err := ctx.Save(e)

	var postCommit *fluxaorm.PostCommitError
	assert.ErrorAs(t, err, &postCommit, "a failed publish is reported after the rows are durable")

	// The write committed - that is the whole premise. What must not happen is
	// the row claiming delivery that never occurred.
	sources, searchErr := entities.GenerateEntityOutboxProvider.SearchMany(ctx.Clone(),
		fluxaorm.NewQuery().Pager(fluxaorm.NewPager(1, 100)))
	assert.NoError(t, searchErr)
	assert.Len(t, sources, 1)

	row := onlyOutboxRow(t, ctx.Clone())
	assert.Equal(t, enums.CDCOutboxStatusList.Pending, row.GetStatus())
	assert.Nil(t, row.GetDispatchedAt())
}

func TestTwoSavesInOneTransactionMarkOnlyTheirOwnRows(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	logs := captureQueries(ctx)
	assert.NoError(t, ctx.Transaction(func(tx fluxaorm.Context) error {
		if err := tx.Save(newOutboxEntity(tx, "save-one")); err != nil {
			return err
		}

		return tx.Save(newOutboxEntity(tx, "save-two"))
	}))

	// Each Save queues its own post-commit closure and marks only the rows it
	// staged. Parking the staged IDs on the context instead would let the first
	// closure mark the second Save's row before it had published - collapsing
	// this to a single UPDATE.
	assert.Equal(t, 2, logs.count("UPDATE `cdc_outbox`"))
	assert.Len(t, outboxRows(t, ctx.Clone()), 2)
}

// restoreStreams recreates the JetStream streams killStreams removed, so a test
// can stage a failed publish and then let the relay succeed.
func restoreStreams(t *testing.T, ctx fluxaorm.Context) {
	t.Helper()

	alters, err := fluxaorm.GetNatsAlters(ctx)
	assert.NoError(t, err)
	for _, alter := range alters {
		assert.NoError(t, alter.Exec(ctx))
	}
}

// TestOutboxStoresTheEventThatWasPublished is the one that keeps replay honest.
// buildEvent stamps TsMs, so serialising the event twice yields different bytes
// and therefore a different Nats-Msg-Id. If the stored row were not the exact
// payload that went on the wire, a relay replay would dodge JetStream's dedup
// and deliver the event a second time.
func TestOutboxStoresTheEventThatWasPublished(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	var delivered []byte
	consumer := fluxaorm.NewCDCConsumer(ctx.Engine(), entities.StreamTestStream).
		OnGenerateEntityOutbox(func(_ fluxaorm.Context, ev *entities.GenerateEntityOutboxDirtyEvent) error {
			raw, marshalErr := json.Marshal(ev)
			if marshalErr != nil {
				return marshalErr
			}
			delivered = raw

			return nil
		}).
		Build()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "byte-identical")))

	stored := onlyOutboxRow(t, ctx.Clone()).GetPayload()

	deadline := time.Now().Add(30 * time.Second)
	for delivered == nil && time.Now().Before(deadline) {
		assert.NoError(t, consumer.Consume(ctx.Context(), 10, time.Second))
	}
	if !assert.NotNil(t, delivered, "no event arrived on test_stream") {
		t.FailNow()
	}

	assert.JSONEq(t, string(delivered), stored,
		"the outbox must hold the event consumers actually saw, not a second serialisation of it")
}

// outboxAcrossNatsPools tags an entity whose two streams are registered on
// different NATS pools - unrepresentable in a row that records one pool.
type outboxAcrossNatsPools struct {
	ID   uint64 `orm:"dirty=test_stream,test_stream_b;outbox"`
	Name string `orm:"required;length=100"`
}

func TestValidateRejectsOutboxStreamsSpanningNatsPools(t *testing.T) {
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "other", nil)
	registry.RegisterCDCStream(entities.StreamTestStream, fluxaorm.CDCStreamOptions{NatsPool: "nats"})
	registry.RegisterCDCStream(entities.StreamTestStreamB, fluxaorm.CDCStreamOptions{NatsPool: "other"})
	registry.RegisterEntity(outboxAcrossNatsPools{}, fluxaorm.CDCOutboxEntity{})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "span nats pools",
		"one row records one pool, so the relay would republish both streams to whichever came first")
}

func TestDispatchMarkFailureKeepsTheUnderlyingCause(t *testing.T) {
	// The wrap has to stay unwrappable: an operator seeing ErrOutboxDispatchMark
	// still needs the driver error to tell a deadlock from a dead connection.
	cause := errors.New("Error 1213: Deadlock found")
	wrapped := fmt.Errorf("%w: %w", fluxaorm.ErrOutboxDispatchMark, cause)

	assert.ErrorIs(t, wrapped, fluxaorm.ErrOutboxDispatchMark)
	assert.ErrorIs(t, wrapped, cause)
}
