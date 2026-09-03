package fluxaorm

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// LadderTask is the task these tests drive. Its attempt cap and backoff are
// deliberately tiny: the ladder is what is under test, not how long it waits.
type LadderTask struct {
	Ref string
}

func (LadderTask) Queue() Queue { return "workers" }

// OtherQueueTask exists to prove a consumer only receives the queues it
// declared, and that a task naming no queue lands on the default one.
type OtherQueueTask struct {
	Ref string
}

type taskConsumerBuilder struct {
	*ConsumerBuilder
}

func fastLadder() TaskOptions {
	return TaskOptions{MaxAttempts: 2, BaseBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
}

// taskConsumer stands up a `workers` consumer with one handler for LadderTask.
func taskConsumer(t *testing.T, handler func(Context, *LadderTask) error) (Context, StreamConsumer) {
	t.Helper()

	registry := NewRegistry()
	registry.RegisterTask(LadderTask{}, fastLadder())

	orm := PrepareTablesWithConsumers(t, registry,
		[]ConsumerDef{{Name: "workers", Queues: []Queue{"workers"}, AckWait: time.Second}},
		JobRunEntity{})

	ref := NewConsumerRef("workers", nil, func(core *ConsumerBuilder) *taskConsumerBuilder {
		return &taskConsumerBuilder{ConsumerBuilder: core}
	})

	builder := NewConsumer(orm.Engine(), ref)
	builder.AddDispatch("LadderTask", BuildTaskDispatch(handler))

	return orm, builder.Build()
}

// pump drives the consumer until done() or the deadline, so a test never
// depends on one fetch seeing the message.
func pump(t *testing.T, consumer StreamConsumer, done func() bool, deadline time.Duration) {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if done() {
			return
		}
		_ = consumer.Consume(context.Background(), 10, 200*time.Millisecond)
	}
}

func TestTaskDispatchToSucceeded(t *testing.T) {
	var handled atomic.Int32
	orm, consumer := taskConsumer(t, func(_ Context, task *LadderTask) error {
		assert.Equal(t, "hello", task.Ref)
		handled.Add(1)

		return nil
	})
	defer orm.Engine().Nats("nats").Close()

	runID, err := DispatchTask(orm, &LadderTask{Ref: "hello"})
	require.NoError(t, err)

	pump(t, consumer, func() bool { return handled.Load() > 0 }, 10*time.Second)
	require.Equal(t, int32(1), handled.Load())

	row := loadJobRun(t, orm.Clone(), runID)
	assert.Equal(t, JobRunSucceeded, row.Status)
	assert.Equal(t, "LadderTask", row.Task)
	assert.Equal(t, "workers", row.Queue)
	assert.Equal(t, uint8(1), row.Attempts)
	assert.True(t, row.FinishedAt.Valid)
}

// TestTaskRetriesUnderTheCapThenTerminates is the ladder itself: a failing task
// comes back with backoff until its cap, then is dead-lettered with its error
// recorded. Without the cap it would spin against the same failure forever.
func TestTaskRetriesUnderTheCapThenTerminates(t *testing.T) {
	var attempts atomic.Int32
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error {
		attempts.Add(1)

		return errors.New("always fails")
	})
	defer orm.Engine().Nats("nats").Close()

	runID, err := DispatchTask(orm, &LadderTask{Ref: "doomed"})
	require.NoError(t, err)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), runID).Status == JobRunFailed
	}, 20*time.Second)

	row := loadJobRun(t, orm.Clone(), runID)
	assert.Equal(t, JobRunFailed, row.Status)
	assert.Equal(t, uint8(2), row.Attempts, "the cap is 2, so the second delivery is the last")
	assert.Contains(t, row.LastError, "always fails")
	assert.Equal(t, int32(2), attempts.Load(), "a terminated task must not be delivered again")
}

// TestUndecodablePayloadIsTerminatedOnFirstAttempt: bytes that will never
// decode get no retries, because they are not going to improve.
func TestUndecodablePayloadIsTerminatedOnFirstAttempt(t *testing.T) {
	var attempts atomic.Int32
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error {
		attempts.Add(1)

		return nil
	})
	defer orm.Engine().Nats("nats").Close()

	runID, err := createJobRun(orm, JobRunSpec{Task: "LadderTask", Queue: "workers", Payload: "not json"})
	require.NoError(t, err)

	publishRawTask(t, orm, "fluxa.task.workers.LadderTask", []byte("not json"), runID)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), runID).Status == JobRunFailed
	}, 10*time.Second)

	row := loadJobRun(t, orm.Clone(), runID)
	assert.Equal(t, JobRunFailed, row.Status)
	assert.Equal(t, uint8(1), row.Attempts, "an undecodable payload must not climb the ladder")
	assert.Contains(t, row.LastError, "cannot be decoded")
	assert.Zero(t, attempts.Load(), "the handler must never see it")
}

// TestUnknownTaskIsTerminated covers a rolled back deploy: a message for a task
// this build no longer knows. Terminating stops it spinning, and closing the
// row out stops the backlog gauge reporting work that was dead-lettered.
func TestUnknownTaskIsTerminated(t *testing.T) {
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error { return nil })
	defer orm.Engine().Nats("nats").Close()

	runID, err := createJobRun(orm, JobRunSpec{Task: "GhostTask", Queue: "workers", Payload: "{}"})
	require.NoError(t, err)

	publishRawTask(t, orm, "fluxa.task.workers.GhostTask", []byte("{}"), runID)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), runID).Status == JobRunFailed
	}, 10*time.Second)

	row := loadJobRun(t, orm.Clone(), runID)
	assert.Equal(t, JobRunFailed, row.Status)
	assert.Contains(t, row.LastError, "no handler for task")
}

// TestCancelledContextDoesNotBurnAnAttempt: shutdown mid-message is not a
// failure. The message goes back to JetStream untouched, and the row must not
// record an error for it - otherwise a rolling deploy looks like a bug.
func TestCancelledContextDoesNotBurnAnAttempt(t *testing.T) {
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error {
		return context.Canceled
	})
	defer orm.Engine().Nats("nats").Close()

	runID, err := DispatchTask(orm, &LadderTask{Ref: "shutting-down"})
	require.NoError(t, err)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), runID).Attempts > 0
	}, 10*time.Second)

	row := loadJobRun(t, orm.Clone(), runID)
	assert.Equal(t, JobRunRunning, row.Status, "the run is still in flight, not finished")
	assert.Empty(t, row.LastError, "a shutdown is not a failure to record")
	assert.False(t, row.FinishedAt.Valid)
}

// TestIdempotencyKeyCollapsesDoubleDispatch: the second dispatch is absorbed by
// the stream's dedup window, and its row is closed out as deduplicated rather
// than left pending for a message that will never arrive.
func TestIdempotencyKeyCollapsesDoubleDispatch(t *testing.T) {
	var handled atomic.Int32
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error {
		handled.Add(1)

		return nil
	})
	defer orm.Engine().Nats("nats").Close()

	// Unique per run: Purge does not clear JetStream's dedup registry, so a
	// fixed key would already be duplicated on the second run inside the window.
	key := "collapse-" + time.Now().Format("150405.000000000")

	firstID, err := DispatchTask(orm, &LadderTask{Ref: "once"}, WithIdempotencyKey(key))
	require.NoError(t, err)

	secondID, err := DispatchTask(orm, &LadderTask{Ref: "once"}, WithIdempotencyKey(key))
	require.NoError(t, err)

	assert.Equal(t, JobRunDeduplicated, loadJobRun(t, orm.Clone(), secondID).Status,
		"the collapsed dispatch must be visible, not pending forever")

	pump(t, consumer, func() bool { return handled.Load() > 0 }, 10*time.Second)

	assert.Equal(t, int32(1), handled.Load(), "the work must run once")
	assert.Equal(t, JobRunSucceeded, loadJobRun(t, orm.Clone(), firstID).Status)
}

func TestConsumerPendingCountsWaitingTasks(t *testing.T) {
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error { return nil })
	defer orm.Engine().Nats("nats").Close()

	pending, err := ConsumerPending(orm, "workers")
	require.NoError(t, err)
	assert.Zero(t, pending)

	_, err = DispatchTask(orm, &LadderTask{Ref: "waiting"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		p, pErr := ConsumerPending(orm, "workers")

		return pErr == nil && p == 1
	}, 10*time.Second, 100*time.Millisecond)

	var drained atomic.Bool
	pump(t, consumer, func() bool {
		p, pErr := ConsumerPending(orm, "workers")
		drained.Store(pErr == nil && p == 0)

		return drained.Load()
	}, 10*time.Second)

	assert.True(t, drained.Load(), "a consumed task must leave the pending count")
}

// TestRedispatchJobRunCreatesANewRun is the operator's retry: the original row
// keeps its history, which is the reason the table is worth having.
func TestRedispatchJobRunCreatesANewRun(t *testing.T) {
	var attempts atomic.Int32
	orm, consumer := taskConsumer(t, func(_ Context, _ *LadderTask) error {
		if attempts.Add(1) <= 2 {
			return errors.New("fails until retried")
		}

		return nil
	})
	defer orm.Engine().Nats("nats").Close()

	failedID, err := DispatchTask(orm, &LadderTask{Ref: "retry-me"})
	require.NoError(t, err)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), failedID).Status == JobRunFailed
	}, 20*time.Second)
	require.Equal(t, JobRunFailed, loadJobRun(t, orm.Clone(), failedID).Status)

	newID, err := RedispatchJobRun(orm, failedID)
	require.NoError(t, err)
	assert.NotEqual(t, failedID, newID)

	pump(t, consumer, func() bool {
		return loadJobRun(t, orm.Clone(), newID).Status == JobRunSucceeded
	}, 20*time.Second)

	assert.Equal(t, JobRunSucceeded, loadJobRun(t, orm.Clone(), newID).Status)
	assert.Equal(t, JobRunFailed, loadJobRun(t, orm.Clone(), failedID).Status,
		"the original row is history and must not be rewritten")
}

func TestRedispatchRefusesARunStillInFlight(t *testing.T) {
	orm, _ := taskConsumer(t, func(_ Context, _ *LadderTask) error { return nil })
	defer orm.Engine().Nats("nats").Close()

	runID, err := createJobRun(orm, JobRunSpec{Task: "LadderTask", Queue: "workers", Payload: "{}"})
	require.NoError(t, err)

	_, err = RedispatchJobRun(orm, runID)
	assert.ErrorContains(t, err, "still in flight")
}

func TestDispatchRefusesInsideATransaction(t *testing.T) {
	orm, _ := taskConsumer(t, func(_ Context, _ *LadderTask) error { return nil })
	defer orm.Engine().Nats("nats").Close()

	err := orm.Transaction(func(tx Context) error {
		_, dispatchErr := DispatchTask(tx, &LadderTask{Ref: "nope"})

		return dispatchErr
	})

	assert.ErrorIs(t, err, ErrDispatchInTransaction)
}

func TestDispatchRefusesAnUnregisteredTask(t *testing.T) {
	orm, _ := taskConsumer(t, func(_ Context, _ *LadderTask) error { return nil })
	defer orm.Engine().Nats("nats").Close()

	_, err := DispatchTask(orm, &OtherQueueTask{Ref: "unknown"})
	assert.ErrorContains(t, err, "is not registered")
}

// publishRawTask puts a message on a task subject without going through
// DispatchTask, which is the only way to reach the consumer's defensive paths.
func publishRawTask(t *testing.T, orm Context, subject string, payload []byte, runID uint64) {
	t.Helper()

	msg := NewNatsMessage(subject)
	msg.Data = payload
	msg.Headers.Set(HeaderJobRunID, strconv.FormatUint(runID, 10))
	msg.Headers.Set("Nats-Msg-Id", subject+":"+time.Now().Format("150405.000000000"))

	require.NoError(t, orm.Engine().Nats("nats").Publish(orm, msg))
}
