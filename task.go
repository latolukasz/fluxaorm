package fluxaorm

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// HeaderJobRunID carries the job_runs row id. It is the one header a task
	// message needs: the task's own identity is the subject, but a row id is not
	// derivable from anything on the wire.
	HeaderJobRunID = "Fluxa-Job-Run-ID"

	defaultTaskMaxAttempts = 5
	defaultTaskBaseBackoff = 5 * time.Second
	defaultTaskMaxBackoff  = 10 * time.Minute
)

// A task name is a Go type name because it is one; a queue name reaches a
// subject token, so it stays lowercase and boring.
var (
	taskNameRegexp  = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]*$`)
	queueNameRegexp = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// Queued is the optional interface a task implements to pick its queue.
// Without it the task lands on DefaultQueue.
//
//	func (DownloadSocialAvatar) Queue() fluxaorm.Queue { return topology.QueueMedia }
type Queued interface {
	Queue() Queue
}

// Subjected is the optional full override of a task's wire subject. Its one
// real use is pinning the old subject across a Go rename, because messages
// already on the stream carry the old name.
//
// The subject must stay under `fluxa.task.`; Validate() rejects anything else.
type Subjected interface {
	Subject() Subject
}

// TaskOptions tunes one task's delivery. Every field has a default, so
// RegisterTask(X{}, TaskOptions{}) is a valid registration.
//
// Identity is deliberately absent: the queue comes from Queued and the subject
// from the type name or Subjected, so no two places can disagree about what a
// task is called.
type TaskOptions struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// TaskStreamOptions tunes the single stream that carries dispatched tasks.
type TaskStreamOptions struct {
	Storage         jetstream.StorageType
	Replicas        int
	MaxAge          time.Duration
	MaxBytes        int64
	DuplicateWindow time.Duration
	NatsPool        string
}

type registeredTask struct {
	task any
	opts TaskOptions
}

// taskSchema is a resolved task: everything dispatch, the consumer and the
// generator need, with defaults applied.
type taskSchema struct {
	name    TaskName
	queue   Queue
	subject Subject
	t       reflect.Type
	// pkgPath is the import path and pkgName the identifier that path declares.
	// They differ whenever the last path segment is not the package name, which
	// any /vN module path guarantees, so generated code cannot derive one from
	// the other.
	pkgPath     string
	pkgName     string
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

func (r *registry) RegisterTask(task any, opts TaskOptions) {
	r.tasks = append(r.tasks, &registeredTask{task: task, opts: opts})
}

func (r *registry) RegisterTaskStream(opts TaskStreamOptions) {
	r.taskStream = &opts
}

func applyTaskStreamDefaults(opts TaskStreamOptions) TaskStreamOptions {
	if opts.NatsPool == "" {
		opts.NatsPool = DefaultPoolCode
	}
	if opts.Replicas <= 0 {
		opts.Replicas = 1
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 7 * 24 * time.Hour
	}
	if opts.DuplicateWindow <= 0 {
		opts.DuplicateWindow = 10 * time.Minute
	}

	return opts
}

// resolveTasks turns the registrations into the schemas everything else reads,
// and indexes them by name, by Go type and by queue.
func resolveTasks(r *registry, e *engineImplementation) error {
	reg := e.registry
	reg.taskStream = applyTaskStreamDefaults(taskStreamOptionsOf(r))
	reg.tasks = make(map[TaskName]*taskSchema, len(r.tasks))
	reg.tasksByType = make(map[reflect.Type]*taskSchema, len(r.tasks))
	reg.tasksByQueue = make(map[Queue][]*taskSchema)

	for _, registered := range r.tasks {
		schema, err := newTaskSchema(registered)
		if err != nil {
			return err
		}
		if previous, has := reg.tasks[schema.name]; has {
			return fmt.Errorf(
				"task '%s' is registered twice (as '%s' and '%s'); two tasks sharing a name would share a subject",
				schema.name, previous.t.String(), schema.t.String())
		}
		reg.tasks[schema.name] = schema
		reg.tasksByType[schema.t] = schema
		reg.tasksByQueue[schema.queue] = append(reg.tasksByQueue[schema.queue], schema)
	}

	for queue := range reg.tasksByQueue {
		sort.Slice(reg.tasksByQueue[queue], func(i, j int) bool {
			return reg.tasksByQueue[queue][i].name < reg.tasksByQueue[queue][j].name
		})
	}

	return checkJobRunEntityRegistered(reg)
}

// TaskNameFor is a task's wire name derived from its type, so a caller with a
// generic task parameter never has to repeat it as a string. It matches what
// newTaskSchema registers, which is what makes the two impossible to disagree.
func TaskNameFor[T any]() TaskName {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return TaskName(t.Name())
}

func newTaskSchema(registered *registeredTask) (*taskSchema, error) {
	t := reflect.TypeOf(registered.task)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("task must be a struct, got %T", registered.task)
	}
	if t.Name() == "" {
		return nil, fmt.Errorf("task must be a named struct type, got %s", t.String())
	}

	// The generator writes a package that names this type, so an unexported task
	// would only fail later, as a compile error inside generated code.
	if !taskNameRegexp.MatchString(t.Name()) {
		return nil, fmt.Errorf(
			"task '%s' must be an exported struct name matching %s; generated code has to name it from another package",
			t.Name(), taskNameRegexp)
	}
	if !hasExportedFields(t) {
		return nil, fmt.Errorf("task '%s' has no exported fields, so its payload would always be empty", t.Name())
	}

	name := TaskName(t.Name())
	queue := taskQueueOf(t)
	if !queueNameRegexp.MatchString(string(queue)) {
		return nil, fmt.Errorf("queue '%s' for task '%s' must match %s", queue, name, queueNameRegexp)
	}

	subject := TaskSubject(queue, name)
	if override, has := taskSubjectOf(t); has {
		if !override.isTaskSubject() {
			return nil, fmt.Errorf(
				"task '%s' overrides Subject() with '%s', which is outside '%s'; the task stream would not capture it",
				name, override, taskSubjectPrefix)
		}
		subject = override
	}

	schema := &taskSchema{
		name:        name,
		queue:       queue,
		subject:     subject,
		t:           t,
		pkgPath:     t.PkgPath(),
		pkgName:     taskPackageName(t),
		maxAttempts: registered.opts.MaxAttempts,
		baseBackoff: registered.opts.BaseBackoff,
		maxBackoff:  registered.opts.MaxBackoff,
	}
	if schema.maxAttempts <= 0 {
		schema.maxAttempts = defaultTaskMaxAttempts
	}
	if schema.baseBackoff <= 0 {
		schema.baseBackoff = defaultTaskBaseBackoff
	}
	if schema.maxBackoff <= 0 {
		schema.maxBackoff = defaultTaskMaxBackoff
	}
	if schema.maxBackoff < schema.baseBackoff {
		return nil, fmt.Errorf(
			"task '%s' has MaxBackoff (%s) below BaseBackoff (%s)", name, schema.maxBackoff, schema.baseBackoff)
	}

	return schema, nil
}

// taskQueueOf probes the optional Queued interface on both the pointer and the
// value receiver - the same house pattern as initIndexes - so a task declaring
// `func (T) Queue()` and one declaring `func (*T) Queue()` both work.
func taskQueueOf(t reflect.Type) Queue {
	if queued, ok := reflect.New(t).Interface().(Queued); ok {
		return queued.Queue()
	}
	if queued, ok := reflect.New(t).Elem().Interface().(Queued); ok {
		return queued.Queue()
	}

	return DefaultQueue
}

func taskSubjectOf(t reflect.Type) (Subject, bool) {
	if subjected, ok := reflect.New(t).Interface().(Subjected); ok {
		return subjected.Subject(), true
	}
	if subjected, ok := reflect.New(t).Elem().Interface().(Subjected); ok {
		return subjected.Subject(), true
	}

	return "", false
}

// taskPackageName reads the package identifier off the type's own String(),
// which renders as "<package>.<Type>". Deriving it from the import path breaks
// on module paths ending in a version segment.
func taskPackageName(t reflect.Type) string {
	name, _, found := strings.Cut(t.String(), ".")
	if !found {
		return ""
	}

	return name
}

func hasExportedFields(t reflect.Type) bool {
	for i := range t.NumField() {
		if t.Field(i).IsExported() {
			return true
		}
	}

	return false
}

// checkJobRunEntityRegistered mirrors the outbox rule: a mechanism that records
// its work is worthless if the table is missing, and degrading quietly to
// untracked tasks is the outcome the run table exists to prevent.
func checkJobRunEntityRegistered(reg *engineRegistryImplementation) error {
	if reg.jobRuns != nil || len(reg.tasks) == 0 {
		return nil
	}

	names := make([]string, 0, len(reg.tasks))
	for name := range reg.tasks {
		names = append(names, string(name))
	}
	// Map order is random; sort so restarts name the same task.
	sort.Strings(names)

	return fmt.Errorf(
		"task '%s' is registered but fluxaorm.JobRunEntity is not; add registry.RegisterEntity(fluxaorm.JobRunEntity{})",
		names[0])
}

func taskStreamOptionsOf(r *registry) TaskStreamOptions {
	if r.taskStream == nil {
		return TaskStreamOptions{}
	}

	return *r.taskStream
}
