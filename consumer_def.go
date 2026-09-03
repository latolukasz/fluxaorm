package fluxaorm

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// A consumer name reaches a JetStream durable and a Prometheus label, so it
// stays lowercase and boring.
var consumerNameRegexp = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ConsumerDef declares one consumer: its name, and which subjects it wants.
//
// The direction matters. An entity publishes its own subject and knows nothing
// about who reads it; the reader declares what it needs. That inversion is why
// adding a consumer for `orders` is a change in one place instead of an edit to
// the orders entity tag.
type ConsumerDef struct {
	// Name is the durable name and the metrics label. Must match
	// ^[a-z][a-z0-9-]*$.
	Name ConsumerName

	// Entities are the entity structs whose changes this consumer wants, given
	// as the same values passed to RegisterEntity. Every one must be tagged
	// `orm:"cdc"`.
	//
	// Mutually exclusive with Queues: a JetStream consumer belongs to one
	// stream, and entity changes and tasks are on different ones.
	Entities []any

	// Queues are the task queues this consumer drains. A queue is the unit of
	// consumption - exactly one consumer may drain it - so listing individual
	// tasks would be a second way to say the same thing.
	Queues []Queue

	// AckWait must exceed the slowest handler on this consumer, or JetStream
	// redelivers work that is still running.
	AckWait       time.Duration
	MaxAckPending int
	// MaxDeliver defaults to unlimited: an entity event that keeps failing is a
	// bug to fix, and dropping it silently loses a change no reindex knows about.
	MaxDeliver int
	NatsPool   string
}

// EntityStreamOptions tunes the single stream that carries entity changes and
// replays. Registering it is optional; the stream exists with these defaults
// either way.
type EntityStreamOptions struct {
	Storage  jetstream.StorageType
	Replicas int
	MaxAge   time.Duration
	// MaxBytes bounds the stream. Worth setting explicitly: replay shares this
	// stream, so an unbounded reindex either evicts live events or fills the
	// disk, and the second failure takes the publishers down with it.
	MaxBytes        int64
	DuplicateWindow time.Duration
	NatsPool        string
}

// resolvedConsumer is one declared consumer after validation: everything the
// topology, the runtime and the generator need, with defaults applied.
type resolvedConsumer struct {
	name    ConsumerName
	options ConsumerDef
	// stream is FLUXA_ENTITY or FLUXA_TASK. A consumer belongs to exactly one.
	stream string
	// tasks are the schemas on the declared queues, ordered by name. Empty for
	// an entity consumer.
	tasks []*taskSchema
	// entities are the declared schemas, ordered by table name so subjects,
	// generated code and error messages are stable across runs.
	entities []*entitySchema
	// subjects is the consumer's full JetStream filter list: one per declared
	// entity, plus its private replay wildcard.
	subjects []Subject
	// dispatchKeyBySubject maps a delivered message's subject to the key its
	// handler is registered under - an entity's Go name, or a task's name. An
	// entity subject and that entity's replay subject both land on the same
	// handler, which is what lets a replay drive the identical code path as a
	// real change.
	dispatchKeyBySubject map[Subject]string
}

func (c *resolvedConsumer) durable() string { return string(c.name) }

// isTaskConsumer decides the failure policy. Both kinds share the declaration,
// the subject filter, the generated builder and the fetch loop; what differs is
// what happens when a handler returns an error, and that difference is real - a
// projection refresh should retry forever, a task should exhaust its attempts
// and dead-letter.
func (c *resolvedConsumer) isTaskConsumer() bool { return c.stream == TaskStreamName }

func applyEntityStreamDefaults(opts EntityStreamOptions) EntityStreamOptions {
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

func applyConsumerDefaults(def ConsumerDef) ConsumerDef {
	if def.NatsPool == "" {
		def.NatsPool = DefaultPoolCode
	}
	if def.AckWait <= 0 {
		def.AckWait = 30 * time.Second
	}
	if def.MaxAckPending == 0 {
		def.MaxAckPending = 256
	}
	if def.MaxDeliver == 0 {
		def.MaxDeliver = -1
	}

	return def
}

func (r *registry) RegisterConsumer(def ConsumerDef) {
	r.consumers = append(r.consumers, def)
}

func (r *registry) RegisterEntityStream(opts EntityStreamOptions) {
	r.entityStream = &opts
}

// resolveConsumers turns the declarations into the resolved consumers
// everything else reads.
//
// It runs from both Validate() and ValidateForCodeGen(): the codegen path is a
// reduced Validate, and consumers come from the registry rather than from
// entity tags, so leaving it out there would make the generator silently emit
// nothing.
//
// Every check here covers a mistake whose runtime symptom is silence - a
// handler that never fires, an event nobody reads - which is exactly the class
// of bug that has to fail at boot instead.
func resolveConsumers(r *registry, e *engineImplementation) error {
	reg := e.registry
	reg.entityStream = applyEntityStreamDefaults(entityStreamOptionsOf(r))
	reg.consumers = make(map[ConsumerName]*resolvedConsumer, len(r.consumers))

	schemaByType := make(map[reflect.Type]*entitySchema, len(reg.entitySchemas))
	for t, schema := range reg.entitySchemas {
		schemaByType[t] = schema
	}

	consumedEntities := make(map[string]bool)
	drainedQueues := make(map[Queue]ConsumerName)

	for _, def := range r.consumers {
		if !consumerNameRegexp.MatchString(string(def.Name)) {
			return fmt.Errorf(
				"consumer '%s' must match %s; the name becomes a JetStream durable and a metrics label",
				def.Name, consumerNameRegexp)
		}
		if _, has := reg.consumers[def.Name]; has {
			return fmt.Errorf("consumer '%s' is declared twice; two consumers sharing a durable would split its messages", def.Name)
		}
		if len(def.Entities) > 0 && len(def.Queues) > 0 {
			return fmt.Errorf(
				"consumer '%s' declares both entities and queues, but a JetStream consumer belongs to one stream; split it in two",
				def.Name)
		}
		if len(def.Entities) == 0 && len(def.Queues) == 0 {
			return fmt.Errorf("consumer '%s' declares neither entities nor queues, so it would drain nothing", def.Name)
		}

		resolved := &resolvedConsumer{
			name:                 def.Name,
			options:              applyConsumerDefaults(def),
			dispatchKeyBySubject: make(map[Subject]string, len(def.Entities)+len(def.Queues)),
		}

		var err error
		if len(def.Queues) > 0 {
			err = resolveTaskConsumer(reg, resolved, def, drainedQueues)
		} else {
			err = resolveEntityConsumer(schemaByType, resolved, def, consumedEntities)
		}
		if err != nil {
			return err
		}

		reg.consumers[def.Name] = resolved
		seedConsumerSettings(r, resolved)
	}

	if err := checkEveryQueueIsDrained(reg, drainedQueues); err != nil {
		return err
	}

	return checkEveryCdcEntityIsConsumed(reg, consumedEntities)
}

func resolveEntityConsumer(
	schemaByType map[reflect.Type]*entitySchema, resolved *resolvedConsumer,
	def ConsumerDef, consumed map[string]bool,
) error {
	resolved.stream = EntityStreamName

	seen := make(map[string]bool, len(def.Entities))
	for _, entity := range def.Entities {
		schema, err := consumerEntitySchema(schemaByType, def.Name, entity)
		if err != nil {
			return err
		}
		if seen[schema.tableName] {
			return fmt.Errorf(
				"consumer '%s' declares entity '%s' twice; JetStream rejects a consumer whose filter subjects overlap",
				def.Name, schema.tableName)
		}
		seen[schema.tableName] = true
		consumed[schema.tableName] = true
		resolved.entities = append(resolved.entities, schema)
	}

	// Sorted so subjects, generated code and error messages are stable.
	sort.Slice(resolved.entities, func(i, j int) bool {
		return resolved.entities[i].tableName < resolved.entities[j].tableName
	})

	for _, schema := range resolved.entities {
		entityName := generatedEntityName(schema.tableName)
		entitySubject := EntitySubject(schema.tableName)
		resolved.subjects = append(resolved.subjects, entitySubject)
		resolved.dispatchKeyBySubject[entitySubject] = entityName
		resolved.dispatchKeyBySubject[ReplaySubject(def.Name, schema.tableName)] = entityName
	}
	resolved.subjects = append(resolved.subjects, replayWildcard(def.Name))

	return nil
}

func resolveTaskConsumer(
	reg *engineRegistryImplementation, resolved *resolvedConsumer,
	def ConsumerDef, drained map[Queue]ConsumerName,
) error {
	resolved.stream = TaskStreamName

	seen := make(map[Queue]bool, len(def.Queues))
	queues := make([]Queue, 0, len(def.Queues))
	for _, queue := range def.Queues {
		if seen[queue] {
			return fmt.Errorf(
				"consumer '%s' declares queue '%s' twice; JetStream rejects a consumer whose filter subjects overlap",
				def.Name, queue)
		}
		if other, taken := drained[queue]; taken {
			// Two consumers on one queue would each terminate the other's tasks
			// as unknown, so this is a silent-work-loss bug, not a scaling knob.
			return fmt.Errorf(
				"queue '%s' is drained by both '%s' and '%s'; exactly one consumer may drain a queue",
				queue, other, def.Name)
		}
		if len(reg.tasksByQueue[queue]) == 0 {
			return fmt.Errorf(
				"consumer '%s' declares queue '%s' but no task is assigned to it; check the Queue() on your task structs",
				def.Name, queue)
		}
		seen[queue] = true
		drained[queue] = def.Name
		queues = append(queues, queue)
	}

	sort.Slice(queues, func(i, j int) bool { return queues[i] < queues[j] })

	for _, queue := range queues {
		resolved.subjects = append(resolved.subjects, queueWildcard(queue))
		for _, schema := range reg.tasksByQueue[queue] {
			resolved.tasks = append(resolved.tasks, schema)
			resolved.dispatchKeyBySubject[schema.subject] = string(schema.name)
		}
	}

	return nil
}

// checkEveryQueueIsDrained is the fall-through guard: a task with no Queue()
// lands on DefaultQueue, and if nothing drains that queue the task simply
// accumulates. Failing at boot is the only way that surfaces.
func checkEveryQueueIsDrained(reg *engineRegistryImplementation, drained map[Queue]ConsumerName) error {
	var orphans []string
	for queue, tasks := range reg.tasksByQueue {
		if drained[queue] == "" {
			orphans = append(orphans, fmt.Sprintf("%s (task '%s')", queue, tasks[0].name))
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	sort.Strings(orphans)

	return fmt.Errorf(
		"queue %s has no consumer draining it, so its tasks would queue forever; declare a fluxaorm.ConsumerDef with that queue",
		orphans[0])
}

// checkEveryCdcEntityIsConsumed is the inverse of the old orphan check. An
// entity tagged `cdc` publishes on every write, so one no consumer filters is
// pure cost: bytes on the stream that nothing will ever read.
func checkEveryCdcEntityIsConsumed(reg *engineRegistryImplementation, consumed map[string]bool) error {
	var orphans []string
	for _, schema := range reg.entitySchemas {
		if schema.cdc && !consumed[schema.tableName] {
			orphans = append(orphans, schema.tableName)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	// Map order is random; sort so a restart names the same entity.
	sort.Strings(orphans)

	return fmt.Errorf(
		"entity '%s' is tagged `orm:\"cdc\"` but no consumer declares it, so its events would be published and never read; add it to a ConsumerDef or drop the tag",
		orphans[0])
}

func consumerEntitySchema(
	schemaByType map[reflect.Type]*entitySchema, consumer ConsumerName, entity any,
) (*entitySchema, error) {
	t := reflect.TypeOf(entity)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("consumer '%s' declares %T, which is not an entity struct", consumer, entity)
	}

	schema, has := schemaByType[t]
	if !has {
		return nil, fmt.Errorf(
			"consumer '%s' declares entity '%s' which is not registered; add registry.RegisterEntity(%s{})",
			consumer, t.Name(), t.Name())
	}
	if !schema.cdc {
		return nil, fmt.Errorf(
			"consumer '%s' declares entity '%s' which is not tagged `orm:\"cdc\"`, so it never publishes",
			consumer, t.Name())
	}

	return schema, nil
}

// seedConsumerSettings registers the durable's settings on the pool so a
// runtime pool.Consumer(name) lookup resolves without the application making a
// separate RegisterNatsConsumer call. The JetStream consumer itself is
// reconciled by GetNatsAlters.
func seedConsumerSettings(r *registry, resolved *resolvedConsumer) {
	pool, has := r.natsPools[resolved.options.NatsPool]
	if !has {
		return
	}
	if _, taken := pool.consumers[resolved.durable()]; taken {
		return
	}

	pool.consumers[resolved.durable()] = &NatsConsumerSettings{
		Name:           resolved.durable(),
		FilterSubjects: subjectStrings(resolved.subjects),
		AckWait:        resolved.options.AckWait,
		MaxAckPending:  resolved.options.MaxAckPending,
		MaxDeliver:     resolved.options.MaxDeliver,
	}
}

func entityStreamOptionsOf(r *registry) EntityStreamOptions {
	if r.entityStream == nil {
		return EntityStreamOptions{}
	}

	return *r.entityStream
}

func subjectStrings(subjects []Subject) []string {
	out := make([]string, len(subjects))
	for i, s := range subjects {
		out[i] = string(s)
	}

	return out
}
