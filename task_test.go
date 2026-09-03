package fluxaorm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The structs below exist only to drive Validate; none of them is ever
// dispatched.

type ResolveTaskA struct{ Ref string }

func (ResolveTaskA) Queue() Queue { return "alpha" }

type ResolveTaskB struct{ Ref string }

type unexportedResolveTask struct{ Ref string }

type EmptyResolveTask struct{ ref string } //nolint:unused // the point is that it has no exported field

type BadQueueTask struct{ Ref string }

func (BadQueueTask) Queue() Queue { return "Not-A-Queue" }

type PinnedSubjectTask struct{ Ref string }

func (PinnedSubjectTask) Queue() Queue     { return "alpha" }
func (PinnedSubjectTask) Subject() Subject { return "fluxa.task.alpha.OldName" }

type EscapedSubjectTask struct{ Ref string }

func (EscapedSubjectTask) Queue() Queue     { return "alpha" }
func (EscapedSubjectTask) Subject() Subject { return "somewhere.else.EscapedSubjectTask" }

// validateTasks runs the resolution without touching MySQL's NATS or JetStream,
// so these stay fast unit tests of the declaration rules.
func validateTasks(tasks []any, consumers []ConsumerDef, opts ...TaskOptions) (Engine, error) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterEntity(JobRunEntity{})

	for i, task := range tasks {
		o := TaskOptions{}
		if i < len(opts) {
			o = opts[i]
		}
		registry.RegisterTask(task, o)
	}
	for _, def := range consumers {
		registry.RegisterConsumer(def)
	}

	return registry.Validate()
}

func alphaConsumer() []ConsumerDef {
	return []ConsumerDef{{Name: "alpha-worker", Queues: []Queue{"alpha"}}}
}

func TestTaskFallsThroughToTheDefaultQueue(t *testing.T) {
	engine, err := validateTasks(
		[]any{ResolveTaskB{}},
		[]ConsumerDef{{Name: "default-worker", Queues: []Queue{DefaultQueue}}})
	assert.NoError(t, err)

	reg := engine.Registry().(*engineRegistryImplementation)
	assert.Equal(t, DefaultQueue, reg.tasks["ResolveTaskB"].queue)
	assert.Equal(t, Subject("fluxa.task.default.ResolveTaskB"), reg.tasks["ResolveTaskB"].subject)
}

// A queue nothing drains is the silent failure the fall-through creates: the
// task publishes fine and simply accumulates.
func TestValidateRejectsAQueueNoConsumerDrains(t *testing.T) {
	// `default` is drained, `alpha` is not, so only the undrained queue is at
	// fault - which is what the error has to name.
	_, err := validateTasks(
		[]any{ResolveTaskA{}, ResolveTaskB{}},
		[]ConsumerDef{{Name: "default-worker", Queues: []Queue{DefaultQueue}}})

	assert.ErrorContains(t, err, "has no consumer draining it")
	assert.ErrorContains(t, err, "alpha")
	assert.ErrorContains(t, err, "ResolveTaskA")
}

// Two consumers on one queue would each terminate the other's tasks as
// unknown, so it is work loss rather than a scaling knob.
func TestValidateRejectsTwoConsumersOnOneQueue(t *testing.T) {
	_, err := validateTasks([]any{ResolveTaskA{}}, []ConsumerDef{
		{Name: "alpha-worker", Queues: []Queue{"alpha"}},
		{Name: "alpha-worker-two", Queues: []Queue{"alpha"}},
	})

	assert.ErrorContains(t, err, "exactly one consumer may drain a queue")
}

func TestValidateRejectsAConsumerMixingEntitiesAndQueues(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterEntity(JobRunEntity{})
	registry.RegisterTask(ResolveTaskA{}, TaskOptions{})
	registry.RegisterConsumer(ConsumerDef{
		Name:     "mixed",
		Entities: []any{JobRunEntity{}},
		Queues:   []Queue{"alpha"},
	})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "belongs to one stream")
}

func TestValidateRejectsAQueueWithNoTasks(t *testing.T) {
	_, err := validateTasks(
		[]any{ResolveTaskA{}},
		[]ConsumerDef{
			{Name: "alpha-worker", Queues: []Queue{"alpha"}},
			{Name: "beta-worker", Queues: []Queue{"beta"}},
		})

	assert.ErrorContains(t, err, "no task is assigned to it")
}

func TestValidateRejectsADuplicateTaskName(t *testing.T) {
	_, err := validateTasks([]any{ResolveTaskA{}, ResolveTaskA{}}, alphaConsumer())

	assert.ErrorContains(t, err, "registered twice")
}

// The generator has to name the type from another package, so an unexported
// task would only fail later as a compile error inside generated code.
func TestValidateRejectsAnUnexportedTask(t *testing.T) {
	_, err := validateTasks([]any{unexportedResolveTask{}}, alphaConsumer())

	assert.ErrorContains(t, err, "must be an exported struct name")
}

func TestValidateRejectsATaskWithNoExportedFields(t *testing.T) {
	_, err := validateTasks([]any{EmptyResolveTask{}}, alphaConsumer())

	assert.ErrorContains(t, err, "payload would always be empty")
}

func TestValidateRejectsABadQueueName(t *testing.T) {
	_, err := validateTasks([]any{BadQueueTask{}}, alphaConsumer())

	assert.ErrorContains(t, err, "must match")
}

func TestValidateRejectsABackoffCeilingBelowTheFloor(t *testing.T) {
	_, err := validateTasks([]any{ResolveTaskA{}}, alphaConsumer(),
		TaskOptions{BaseBackoff: time.Minute, MaxBackoff: time.Second})

	assert.ErrorContains(t, err, "below BaseBackoff")
}

// TestTaskSubjectOverrideWins is the rename escape hatch: messages already on
// the stream carry the old subject, and this is what keeps delivering them.
func TestTaskSubjectOverrideWins(t *testing.T) {
	engine, err := validateTasks([]any{PinnedSubjectTask{}}, alphaConsumer())
	assert.NoError(t, err)

	reg := engine.Registry().(*engineRegistryImplementation)
	assert.Equal(t, Subject("fluxa.task.alpha.OldName"), reg.tasks["PinnedSubjectTask"].subject)
}

// An override outside the prefix would publish into no stream at all, and the
// server drops it with no error at the publisher.
func TestValidateRejectsATaskSubjectOutsideThePrefix(t *testing.T) {
	_, err := validateTasks([]any{EscapedSubjectTask{}}, alphaConsumer())

	assert.ErrorContains(t, err, "outside 'fluxa.task.'")
}

// A mechanism that records its work is worthless if the table is missing, and
// degrading quietly to untracked tasks is what the run table exists to prevent.
func TestValidateRejectsATaskWithoutTheJobRunEntity(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", DefaultPoolCode, &MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, DefaultPoolCode, nil)
	registry.RegisterTask(ResolveTaskA{}, TaskOptions{})
	registry.RegisterConsumer(ConsumerDef{Name: "alpha-worker", Queues: []Queue{"alpha"}})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "fluxaorm.JobRunEntity is not")
	assert.ErrorContains(t, err, "ResolveTaskA")
}

// TestTaskNameForMatchesRegistration is what lets a caller drop the task-name
// string argument: if these two ever diverged, a handler's metrics would be
// filed under a name no dispatch ever used.
func TestTaskNameForMatchesRegistration(t *testing.T) {
	schema, err := newTaskSchema(&registeredTask{task: LadderTask{}})
	require.NoError(t, err)

	assert.Equal(t, schema.name, TaskNameFor[LadderTask]())
	assert.Equal(t, schema.name, TaskNameFor[*LadderTask](), "a pointer type must resolve the same")
}
