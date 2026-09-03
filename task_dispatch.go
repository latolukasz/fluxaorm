package fluxaorm

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// ErrDispatchInTransaction is returned when a dispatch is attempted inside an
// open transaction. The publish is not transactional, so a transaction that
// later rolls back would leave a task running against data that never existed.
// Dispatch after the commit instead.
var ErrDispatchInTransaction = errors.New(
	"task dispatch inside a transaction: the publish cannot roll back with it, so dispatch after commit")

type dispatchConfig struct {
	idempotencyKey string
}

type DispatchOption func(*dispatchConfig)

// WithIdempotencyKey collapses repeat dispatches of the same logical work
// inside the task stream's dedup window. The collapsed dispatch's run row is
// closed as deduplicated, so it is visible rather than pending forever.
func WithIdempotencyKey(key string) DispatchOption {
	return func(c *dispatchConfig) { c.idempotencyKey = key }
}

// DispatchTask publishes one task and returns the ID of its run row.
//
// The row is written before the publish so its ID can ride on the message; a
// publish the stream deduplicates away closes that row out as deduplicated
// rather than leaving it pending for a message that will never arrive.
func DispatchTask[T any](ctx Context, task *T, opts ...DispatchOption) (uint64, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, errors.New("task dispatch: unsupported context")
	}
	if orm.InTransaction() {
		return 0, ErrDispatchInTransaction
	}
	if task == nil {
		return 0, errors.New("task dispatch: task is nil")
	}

	schema, has := orm.engine.registry.tasksByType[reflect.TypeOf(*task)]
	if !has {
		return 0, fmt.Errorf(
			"task dispatch: '%s' is not registered; add registry.RegisterTask(%s{}, ...)",
			reflect.TypeOf(*task).String(), reflect.TypeOf(*task).Name())
	}

	config := &dispatchConfig{}
	for _, opt := range opts {
		opt(config)
	}

	payload, err := json.Marshal(task)
	if err != nil {
		return 0, fmt.Errorf("task dispatch: encode '%s': %w", schema.name, err)
	}

	runID, err := createJobRun(orm, JobRunSpec{
		Task:    string(schema.name),
		Queue:   string(schema.queue),
		Payload: string(payload),
	})
	if err != nil {
		return 0, err
	}

	msg := newTaskMessage(schema, payload, runID, config.idempotencyKey)

	ack, err := orm.Engine().Nats(orm.engine.registry.taskStream.NatsPool).PublishWithAck(orm, msg)
	if err != nil {
		// The row stays pending on purpose: it is the only evidence the dispatch
		// was attempted, and the backlog gauge is what surfaces it.
		return runID, fmt.Errorf("task dispatch: publish '%s': %w", schema.name, err)
	}

	if ack.Duplicate {
		if markErr := MarkJobRunFinished(orm, runID, JobRunResult{
			Status: JobRunDeduplicated,
			Err:    "collapsed by the task stream's dedup window",
		}); markErr != nil {
			return runID, markErr
		}
	}

	return runID, nil
}

// RedispatchJobRun publishes a finished run's payload again as a brand new run,
// and returns the new run's ID.
//
// This is the operator's retry, so it works from a task name rather than a Go
// type: an admin screen has the row, not the struct. The original row is left
// exactly as it was, because the attempt history is the reason the table is
// worth keeping.
//
// An unfinished run is refused. Re-dispatching one would duplicate work that is
// still in flight, and JetStream is already redelivering it.
func RedispatchJobRun(ctx Context, runID uint64) (uint64, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, errors.New("task redispatch: unsupported context")
	}
	if orm.InTransaction() {
		return 0, ErrDispatchInTransaction
	}

	// The row's own queue is ignored on purpose: the task may have been moved to
	// a different queue since, and the registry is the current truth.
	task, _, status, payload, err := LoadJobRun(orm, runID)
	if err != nil {
		return 0, err
	}

	if status != JobRunSucceeded && status != JobRunFailed && status != JobRunDeduplicated {
		return 0, fmt.Errorf("task redispatch: run %d is %s, so it is still in flight", runID, status)
	}

	schema := orm.engine.registry.tasks[TaskName(task)]
	if schema == nil {
		return 0, fmt.Errorf("task redispatch: task '%s' is no longer registered", task)
	}

	newRunID, err := createJobRun(orm, JobRunSpec{
		Task:    string(schema.name),
		Queue:   string(schema.queue),
		Payload: payload,
	})
	if err != nil {
		return 0, err
	}

	// Deliberately no Nats-Msg-Id: an operator asking for a retry means it, and a
	// dedup window collapsing it would look like the button did nothing.
	msg := newTaskMessage(schema, []byte(payload), newRunID, "")

	if _, err := orm.Engine().Nats(orm.engine.registry.taskStream.NatsPool).PublishWithAck(orm, msg); err != nil {
		return newRunID, fmt.Errorf("task redispatch: publish '%s': %w", schema.name, err)
	}

	return newRunID, nil
}

// newTaskMessage is the single builder for both dispatch paths, so a retry can
// never drift from the original in a way that changes its routing.
func newTaskMessage(schema *taskSchema, payload []byte, runID uint64, idempotencyKey string) *NatsMessage {
	msg := NewNatsMessage(string(schema.subject))
	msg.Data = payload
	msg.Headers.Set(HeaderJobRunID, strconv.FormatUint(runID, 10))
	if idempotencyKey != "" {
		msg.Headers.Set("Nats-Msg-Id", idempotencyKey)
	}

	return msg
}
