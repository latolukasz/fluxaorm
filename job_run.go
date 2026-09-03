package fluxaorm

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
)

// ErrJobRunEntityNotRegistered is returned by every job-run function when
// JobRunEntity is absent from the registry. Unlike the outbox functions, which
// no-op in that case, these fail loudly: a job runner with no run rows has
// silently lost its entire status history, which is the one thing the table
// exists to provide.
var ErrJobRunEntityNotRegistered = errors.New(
	"fluxaorm.JobRunEntity is not registered; add registry.RegisterEntity(fluxaorm.JobRunEntity{})")

// JobRunEntity records one dispatch of one task: its payload, how many attempts
// it took, and how it ended. The application registers it like any entity and
// gets the table, the provider and the status enum generated from it.
//
// fluxaorm owns this struct because fluxaorm owns the transitions. An
// application that hand-wrote the same table would have to keep its column
// names and status values in step with the consumer's ladder.
type JobRunEntity struct {
	ID         uint64     `orm:"table=job_runs"`
	Task       string     `orm:"required;length=100"`
	Queue      string     `orm:"required;length=50"`
	Status     string     `orm:"enum=pending,running,succeeded,failed,deduplicated;enumName=JobRunStatus;required"`
	Payload    string     `orm:"length=max;required"`
	Attempts   uint8      `orm:"required"`
	LastError  string     `orm:"length=max;required"`
	DurationMs uint32     `orm:"required"`
	StartedAt  *time.Time `orm:"time"`
	FinishedAt *time.Time `orm:"time"`
	CreatedAt  time.Time  `orm:"time"`
}

// Status+CreatedAt serves the purge and the backlog; Task+CreatedAt serves the
// admin screen's per-task history.
func (e JobRunEntity) Indexes() [][]string {
	return [][]string{{"Status", "CreatedAt"}, {"Task", "CreatedAt"}}
}

const (
	JobRunPending      = "pending"
	JobRunRunning      = "running"
	JobRunSucceeded    = "succeeded"
	JobRunFailed       = "failed"
	JobRunDeduplicated = "deduplicated"
)

// jobRunTerminalStatuses is what a run may finish as, and equally what the purge
// may delete. Pending and running are absent from both on purpose.
var jobRunTerminalStatuses = []string{JobRunSucceeded, JobRunFailed, JobRunDeduplicated}

// JobRunSpec is the row a dispatch creates before it publishes.
type JobRunSpec struct {
	Task    string
	Queue   string
	Payload string
}

// JobRunResult is how an attempt ended. Status is one of the terminal statuses,
// or pending to hand the run back for another attempt.
type JobRunResult struct {
	Status   string
	Err      string
	Duration time.Duration
}

// resolvedJobRuns is the registry's handle on the job_runs table, resolved once
// at Validate() time from the registered entity's schema.
type resolvedJobRuns struct {
	tableName string
	poolCode  string
}

// resolveJobRuns records where the job_runs table lives, if the entity is
// registered at all. Registering it is optional for an application that uses no
// job tasks, so absence is not an error here - resolveJobTasks is what rejects
// tasks without it.
func resolveJobRuns(e *engineImplementation) {
	schema, has := e.registry.entitySchemas[reflect.TypeOf(JobRunEntity{})]
	if !has {
		return
	}
	e.registry.jobRuns = &resolvedJobRuns{tableName: schema.tableName, poolCode: schema.mysqlPoolCode}
}

// jobRunsTable resolves the table for a write, or explains what is missing.
func jobRunsTable(ctx Context) (*ormImplementation, *resolvedJobRuns, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return nil, nil, errors.New("job runs: unsupported context")
	}
	runs := orm.engine.registry.jobRuns
	if runs == nil {
		return nil, nil, ErrJobRunEntityNotRegistered
	}
	return orm, runs, nil
}

const jobRunInsertSQL = "INSERT INTO `%s` (`ID`,`Task`,`Queue`,`Status`,`Payload`,`Attempts`,`LastError`," +
	"`DurationMs`,`CreatedAt`) VALUES (?,?,?,?,?,0,'',0,?)"

// createJobRun writes the pending row and returns its ID, which the dispatch
// stamps onto the message so the consumer can find it again. The ID is assigned
// client-side, so it is known before the publish.
func createJobRun(ctx Context, spec JobRunSpec) (uint64, error) {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return 0, err
	}

	id := orm.Engine().NextID()

	_, err = orm.DB(runs.poolCode).Exec(orm, fmt.Sprintf(jobRunInsertSQL, runs.tableName),
		id, spec.Task, spec.Queue, JobRunPending, spec.Payload, time.Now())
	if err != nil {
		return 0, fmt.Errorf("create job run: %w", err)
	}

	return id, nil
}

const jobRunLoadSQL = "SELECT `Task`,`Queue`,`Status`,`Payload` FROM `%s` WHERE `ID` = ?"

// LoadJobRun reads back one run's task, queue, status and payload. It exists so
// an application can re-dispatch a finished run without having to know how the
// columns are named.
func LoadJobRun(ctx Context, id uint64) (task, queue, status, payload string, err error) {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return "", "", "", "", err
	}

	found, err := orm.DB(runs.poolCode).QueryRow(orm,
		NewWhere(fmt.Sprintf(jobRunLoadSQL, runs.tableName), id),
		&task, &queue, &status, &payload)
	if err != nil {
		return "", "", "", "", fmt.Errorf("load job run %d: %w", id, err)
	}
	if !found {
		return "", "", "", "", fmt.Errorf("load job run %d: no such row", id)
	}

	return task, queue, status, payload, nil
}

const jobRunStartSQL = "UPDATE `%s` SET `Status` = '" + JobRunRunning +
	"', `Attempts` = ?, `StartedAt` = ? WHERE `ID` = ?"

// MarkJobRunStarted flips the row to running for the given attempt.
func MarkJobRunStarted(ctx Context, id uint64, attempt uint8) error {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return err
	}

	return execOneJobRun(orm, runs, fmt.Sprintf(jobRunStartSQL, runs.tableName),
		"mark job run started", id, attempt, time.Now(), id)
}

const jobRunFinishSQL = "UPDATE `%s` SET `Status` = ?, `LastError` = ?, `DurationMs` = ?, " +
	"`FinishedAt` = ? WHERE `ID` = ?"

// MarkJobRunFinished records how an attempt ended. A pending status hands the
// run back for another attempt and leaves FinishedAt unset; anything outside the
// terminal set plus pending is rejected rather than written, because a status
// this table does not model would break the purge and the backlog silently.
func MarkJobRunFinished(ctx Context, id uint64, res JobRunResult) error {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return err
	}

	if res.Status != JobRunPending && !slices.Contains(jobRunTerminalStatuses, res.Status) {
		return fmt.Errorf("mark job run finished: status %q is not pending or terminal", res.Status)
	}

	var finishedAt *time.Time
	if res.Status != JobRunPending {
		now := time.Now()
		finishedAt = &now
	}

	return execOneJobRun(orm, runs, fmt.Sprintf(jobRunFinishSQL, runs.tableName),
		"mark job run finished", id, res.Status, res.Err, res.Duration.Milliseconds(), finishedAt, id)
}

// execOneJobRun runs a single-row UPDATE and treats "no row matched" as an
// error. A lost outcome has to surface: the row would otherwise keep whatever
// status it had and the backlog alarm would describe a job nobody is running.
func execOneJobRun(orm *ormImplementation, runs *resolvedJobRuns, query, what string, id uint64, args ...any) error {
	res, err := orm.DB(runs.poolCode).Exec(orm, query, args...)
	if err != nil {
		return fmt.Errorf("%s %d: %w", what, id, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s %d: %w", what, id, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s %d: no such row", what, id)
	}

	return nil
}

const jobRunPurgeSQL = "DELETE FROM `%s` WHERE `Status` = ? AND `CreatedAt` <= ? ORDER BY `ID` LIMIT %d"

// PurgeJobRuns deletes rows of one terminal status older than olderThan and
// reports how many went. The status is a parameter, and only a terminal one is
// accepted, so no caller can reach a pending or running row: one of those is a
// job that has not finished, and deleting it destroys the only record that it
// was ever dispatched.
func PurgeJobRuns(ctx Context, status string, olderThan time.Duration, limit int) (int, error) {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return 0, err
	}

	if !slices.Contains(jobRunTerminalStatuses, status) {
		return 0, fmt.Errorf("purge job runs: status %q is not terminal", status)
	}

	res, err := orm.DB(runs.poolCode).Exec(orm,
		fmt.Sprintf(jobRunPurgeSQL, runs.tableName, limit), status, time.Now().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("purge job runs: %w", err)
	}

	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge job runs: %w", err)
	}

	return int(deleted), nil
}

const jobRunBacklogSQL = "SELECT COUNT(*), COALESCE(MIN(`CreatedAt`), NOW()) FROM `%s` WHERE `Status` = '" +
	JobRunPending + "'"

// JobRunBacklog reports how many runs are still pending and how old the oldest
// is. This is the alarm for the one hole in JetStream-only delivery: a row
// pending for longer than its task's whole backoff ladder means its message was
// never delivered, and nothing else in the system will say so.
//
// It is not a queue-depth gauge - JetStream's NumPending is that. A healthy busy
// queue keeps this near zero, because a running job leaves pending immediately.
func JobRunBacklog(ctx Context) (depth int, oldest time.Duration, err error) {
	orm, runs, err := jobRunsTable(ctx)
	if err != nil {
		return 0, 0, err
	}

	var oldestAt time.Time

	_, err = orm.DB(runs.poolCode).QueryRow(orm,
		NewWhere(fmt.Sprintf(jobRunBacklogSQL, runs.tableName)), &depth, &oldestAt)
	if err != nil {
		return 0, 0, fmt.Errorf("read job run backlog: %w", err)
	}
	if depth == 0 {
		return 0, 0, nil
	}

	return depth, time.Since(oldestAt), nil
}
