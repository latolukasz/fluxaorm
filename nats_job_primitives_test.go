package fluxaorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testJobStream   = "TEST_JOB_PRIMITIVES"
	testJobSubject  = "test.jobprimitives.one"
	testJobDurable  = "test-job-primitives-workers"
	testJobDedupWin = time.Minute
)

// natsPrimitivesContext gives a purged single-subject stream with a durable
// consumer, so each test starts from an empty stream.
func natsPrimitivesContext(t *testing.T, ackWait time.Duration) (Context, NatsConsumer) {
	t.Helper()

	registry := NewRegistry()
	registry.RegisterNatsStream(NewNatsStream(testJobStream, "nats").
		Subjects("test.jobprimitives.>").
		Duplicates(testJobDedupWin))
	registry.RegisterNatsConsumer(NewNatsConsumer(testJobDurable, "nats").
		FilterSubjects("test.jobprimitives.>").
		AckWait(ackWait).
		MaxAckPending(10).
		MaxDeliver(-1))

	orm := PrepareTablesWithNats(t, registry)

	pool := orm.Engine().Nats("nats")
	require.NotNil(t, pool)

	js, err := pool.GetJetStream()
	require.NoError(t, err)

	stream, err := js.Stream(orm.Context(), testJobStream)
	require.NoError(t, err)
	require.NoError(t, stream.Purge(orm.Context()))

	consumer, err := pool.Consumer(testJobDurable)
	require.NoError(t, err)

	return orm, consumer
}

func publishOne(t *testing.T, orm Context, msgID string) {
	t.Helper()

	msg := NewNatsMessage(testJobSubject)
	msg.Data = []byte(`{"n":1}`)

	if msgID != "" {
		msg.Headers.Set("Nats-Msg-Id", msgID)
	}

	require.NoError(t, orm.Engine().Nats("nats").Publish(orm, msg))
}

func fetchOne(t *testing.T, orm Context, consumer NatsConsumer, wait time.Duration) *NatsMessage {
	t.Helper()

	batch := consumer.Fetch(orm, 1, wait)
	require.NoError(t, batch.Error())

	records := batch.Records()
	if len(records) == 0 {
		return nil
	}

	return records[0]
}

// Deliveries is what the job runner counts attempts with. Without it every
// redelivery looks like attempt 1 and no attempt cap can ever trip.
func TestFetchReportsDeliveryCount(t *testing.T) {
	orm, consumer := natsPrimitivesContext(t, 30*time.Second)

	publishOne(t, orm, "")

	first := fetchOne(t, orm, consumer, 5*time.Second)
	require.NotNil(t, first)
	assert.Equal(t, uint64(1), first.Deliveries)

	require.NoError(t, first.Nak())

	second := fetchOne(t, orm, consumer, 5*time.Second)
	require.NotNil(t, second)
	assert.Equal(t, uint64(2), second.Deliveries,
		"a redelivered message must report a growing delivery count")
}

// The backoff ladder is NakWithDelay. Plain Nak redelivers immediately, which
// turns a permanently failing task into a hot loop against NATS.
func TestNakWithDelayRedeliversAfterTheDelay(t *testing.T) {
	orm, consumer := natsPrimitivesContext(t, 30*time.Second)

	publishOne(t, orm, "")

	first := fetchOne(t, orm, consumer, 5*time.Second)
	require.NotNil(t, first)

	require.NoError(t, first.NakWithDelay(2*time.Second))

	assert.Nil(t, fetchOne(t, orm, consumer, 500*time.Millisecond),
		"the message must not come back before the delay elapses")

	assert.NotNil(t, fetchOne(t, orm, consumer, 5*time.Second),
		"the message must come back once the delay has elapsed")
}

// Dispatch writes its job_runs row before publishing, so it has to know when
// JetStream collapsed the publish as a duplicate - otherwise that row sits
// pending forever for a message that will never be delivered.
func TestPublishWithAckReportsDuplicate(t *testing.T) {
	orm, _ := natsPrimitivesContext(t, 30*time.Second)

	pool := orm.Engine().Nats("nats")

	// Purging the stream does not clear JetStream's dedup registry, so the id has
	// to be unique per run or a rerun inside the window starts out duplicated.
	msgID := fmt.Sprintf("dedup-%d", time.Now().UnixNano())

	first := NewNatsMessage(testJobSubject)
	first.Data = []byte(`{"n":1}`)
	first.Headers.Set("Nats-Msg-Id", msgID)

	ack, err := pool.PublishWithAck(orm, first)
	require.NoError(t, err)
	assert.False(t, ack.Duplicate)
	assert.Equal(t, testJobStream, ack.Stream)
	assert.NotZero(t, ack.Sequence)

	second := NewNatsMessage(testJobSubject)
	second.Data = []byte(`{"n":1}`)
	second.Headers.Set("Nats-Msg-Id", msgID)

	ack, err = pool.PublishWithAck(orm, second)
	require.NoError(t, err)
	assert.True(t, ack.Duplicate,
		"a repeat Nats-Msg-Id inside the dedup window must be reported as a duplicate")
}
