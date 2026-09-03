package fluxaorm

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jobRunContext(t *testing.T) Context {
	t.Helper()

	return PrepareTables(t, NewRegistry(), JobRunEntity{})
}

type jobRunRow struct {
	Task       string
	Queue      string
	Status     string
	Payload    string
	Attempts   uint8
	LastError  string
	DurationMs uint32
	StartedAt  sql.NullTime
	FinishedAt sql.NullTime
	CreatedAt  time.Time
}

func loadJobRun(t *testing.T, orm Context, id uint64) jobRunRow {
	t.Helper()

	var row jobRunRow

	found, err := orm.DB(DefaultPoolCode).QueryRow(orm,
		NewWhere("SELECT `Task`,`Queue`,`Status`,`Payload`,`Attempts`,`LastError`,`DurationMs`,"+
			"`StartedAt`,`FinishedAt`,`CreatedAt` FROM `job_runs` WHERE `ID` = ?", id),
		&row.Task, &row.Queue, &row.Status, &row.Payload, &row.Attempts, &row.LastError,
		&row.DurationMs, &row.StartedAt, &row.FinishedAt, &row.CreatedAt)
	require.NoError(t, err)
	require.True(t, found, "job run %d not found", id)

	return row
}

func backdateJobRun(t *testing.T, orm Context, id uint64, age time.Duration) {
	t.Helper()

	_, err := orm.DB(DefaultPoolCode).Exec(orm,
		"UPDATE `job_runs` SET `CreatedAt` = ? WHERE `ID` = ?", time.Now().Add(-age), id)
	require.NoError(t, err)
}

func newJobRun(t *testing.T, orm Context) uint64 {
	t.Helper()

	id, err := createJobRun(orm, JobRunSpec{Task: "SendWelcomeEmail", Queue: "emails", Payload: `{"UserID":7}`})
	require.NoError(t, err)
	require.NotZero(t, id)

	return id
}

// The row has to be born pending and reach a terminal status through running:
// the admin screen and the stranded-row alarm both read that progression.
func TestJobRunLifecycleWritesPendingRunningTerminal(t *testing.T) {
	orm := jobRunContext(t)

	id := newJobRun(t, orm)

	born := loadJobRun(t, orm, id)
	assert.Equal(t, JobRunPending, born.Status)
	assert.Equal(t, "SendWelcomeEmail", born.Task)
	assert.Equal(t, "emails", born.Queue)
	assert.Equal(t, `{"UserID":7}`, born.Payload)
	assert.Zero(t, born.Attempts)
	assert.False(t, born.StartedAt.Valid)
	assert.False(t, born.FinishedAt.Valid)

	require.NoError(t, MarkJobRunStarted(orm, id, 1))

	started := loadJobRun(t, orm, id)
	assert.Equal(t, JobRunRunning, started.Status)
	assert.Equal(t, uint8(1), started.Attempts)
	assert.True(t, started.StartedAt.Valid)
	assert.False(t, started.FinishedAt.Valid)

	require.NoError(t, MarkJobRunFinished(orm, id, JobRunResult{
		Status:   JobRunSucceeded,
		Duration: 25 * time.Millisecond,
	}))

	done := loadJobRun(t, orm, id)
	assert.Equal(t, JobRunSucceeded, done.Status)
	assert.Equal(t, uint32(25), done.DurationMs)
	assert.Empty(t, done.LastError)
	assert.True(t, done.FinishedAt.Valid)
}

// A retry goes back to pending carrying the attempt count and the error, so the
// admin screen shows why it is being retried rather than just that it is.
func TestJobRunRetryReturnsToPendingWithTheError(t *testing.T) {
	orm := jobRunContext(t)

	id := newJobRun(t, orm)
	require.NoError(t, MarkJobRunStarted(orm, id, 2))
	require.NoError(t, MarkJobRunFinished(orm, id, JobRunResult{
		Status: JobRunPending,
		Err:    "smtp timeout",
	}))

	row := loadJobRun(t, orm, id)
	assert.Equal(t, JobRunPending, row.Status)
	assert.Equal(t, uint8(2), row.Attempts)
	assert.Equal(t, "smtp timeout", row.LastError)
	assert.False(t, row.FinishedAt.Valid, "a retry has not finished")
}

func TestMarkJobRunFinishedRejectsANonTerminalStatus(t *testing.T) {
	orm := jobRunContext(t)

	id := newJobRun(t, orm)
	require.NoError(t, MarkJobRunStarted(orm, id, 1))

	for _, status := range []string{JobRunRunning, "", "bogus"} {
		err := MarkJobRunFinished(orm, id, JobRunResult{Status: status})
		require.Error(t, err, "status %q must be rejected", status)
	}

	assert.Equal(t, JobRunRunning, loadJobRun(t, orm, id).Status,
		"a rejected status must not have been written")
}

// A mark that hits no row means an outcome was lost. Reporting it as success -
// the way the outbox dispatch mark deliberately does - would hide that.
func TestMarkJobRunAgainstAMissingRowErrors(t *testing.T) {
	orm := jobRunContext(t)

	require.Error(t, MarkJobRunStarted(orm, 123456, 1))
	require.Error(t, MarkJobRunFinished(orm, 123456, JobRunResult{Status: JobRunSucceeded}))
}

// The purge takes the status it is allowed to delete, so pending is unreachable
// from here by construction rather than by the caller remembering.
func TestPurgeJobRunsRefusesANonTerminalStatus(t *testing.T) {
	orm := jobRunContext(t)

	id := newJobRun(t, orm)
	backdateJobRun(t, orm, id, 90*24*time.Hour)

	for _, status := range []string{JobRunPending, JobRunRunning, "", "bogus"} {
		_, err := PurgeJobRuns(orm, status, time.Hour, 100)
		require.Error(t, err, "status %q must be rejected", status)
	}

	assert.Equal(t, JobRunPending, loadJobRun(t, orm, id).Status,
		"a pending row must survive the purge at any age")
}

func TestPurgeJobRunsRemovesTerminalRowsPastRetention(t *testing.T) {
	orm := jobRunContext(t)

	for _, status := range []string{JobRunSucceeded, JobRunFailed, JobRunDeduplicated} {
		old := newJobRun(t, orm)
		require.NoError(t, MarkJobRunFinished(orm, old, JobRunResult{Status: status}))
		backdateJobRun(t, orm, old, 48*time.Hour)

		fresh := newJobRun(t, orm)
		require.NoError(t, MarkJobRunFinished(orm, fresh, JobRunResult{Status: status}))

		deleted, err := PurgeJobRuns(orm, status, 24*time.Hour, 100)
		require.NoError(t, err)
		assert.Equal(t, 1, deleted, "only the backdated %s row should go", status)

		loadJobRun(t, orm, fresh)
	}
}

// The alarm for the durability hole: a row pending longer than its whole backoff
// ladder means the message was never delivered.
func TestJobRunBacklogReportsOldestPending(t *testing.T) {
	orm := jobRunContext(t)

	depth, oldest, err := JobRunBacklog(orm)
	require.NoError(t, err)
	assert.Zero(t, depth)
	assert.Zero(t, oldest)

	stranded := newJobRun(t, orm)
	backdateJobRun(t, orm, stranded, 2*time.Hour)

	done := newJobRun(t, orm)
	require.NoError(t, MarkJobRunFinished(orm, done, JobRunResult{Status: JobRunSucceeded}))
	backdateJobRun(t, orm, done, 10*time.Hour)

	depth, oldest, err = JobRunBacklog(orm)
	require.NoError(t, err)
	assert.Equal(t, 1, depth, "only pending rows count toward the backlog")
	assert.Greater(t, oldest, 90*time.Minute)
	assert.Less(t, oldest, 150*time.Minute)
}

// Silently no-oping when the entity is missing would make job tracking vanish
// with no signal at all, which is what the table exists to prevent.
func TestJobRunFunctionsErrorWhenEntityUnregistered(t *testing.T) {
	orm := PrepareTables(t, NewRegistry())

	_, err := createJobRun(orm, JobRunSpec{Task: "X", Queue: "default"})
	require.True(t, errors.Is(err, ErrJobRunEntityNotRegistered), "createJobRun: %v", err)

	require.True(t, errors.Is(MarkJobRunStarted(orm, 1, 1), ErrJobRunEntityNotRegistered))
	require.True(t, errors.Is(MarkJobRunFinished(orm, 1, JobRunResult{Status: JobRunSucceeded}), ErrJobRunEntityNotRegistered))

	_, err = PurgeJobRuns(orm, JobRunSucceeded, time.Hour, 10)
	require.True(t, errors.Is(err, ErrJobRunEntityNotRegistered), "PurgeJobRuns: %v", err)

	_, _, err = JobRunBacklog(orm)
	require.True(t, errors.Is(err, ErrJobRunEntityNotRegistered), fmt.Sprintf("JobRunBacklog: %v", err))
}
