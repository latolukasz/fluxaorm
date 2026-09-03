package fluxaorm

// Subject is the identity of something that happened. Entity changes get one
// subject per table; a consumer selects the subset it wants with a subject
// filter. That is what makes one write one message however many consumers read
// it - fan-out happens at subscribe time, not by duplicating bytes at publish
// time.
type Subject string

// ConsumerName names a declared consumer. It doubles as the JetStream durable
// name, so the two can never drift and an operator reading `nats consumer ls`
// sees the same names the code declares.
type ConsumerName string

// Queue routes tasks to the consumer that drains them. It is a routing label
// and nothing else: the transport tuning belongs to the consumer that declares
// the queue, and the retention to the one task stream.
type Queue string

// TaskName is a task's wire name, which is its Go type name.
type TaskName string

const (
	// EntityStreamName carries every entity change and every replay. One stream
	// rather than one per consumer: the subject already identifies the entity, so
	// per-consumer streams would only re-add the duplication.
	EntityStreamName = "FLUXA_ENTITY"
	// TaskStreamName carries dispatched tasks. Separate from entity changes
	// because the two need different retention, and because a task backlog must
	// not evict a change nobody has consumed yet.
	TaskStreamName = "FLUXA_TASK"

	// DefaultQueue carries every task that does not declare a Queue().
	DefaultQueue Queue = "default"

	entitySubjectPrefix = "fluxa.entity."
	replaySubjectPrefix = "fluxa.replay."
	taskSubjectPrefix   = "fluxa.task."

	entitySubjectWildcard = entitySubjectPrefix + ">"
	replaySubjectWildcard = replaySubjectPrefix + ">"
	taskSubjectWildcard   = taskSubjectPrefix + ">"
)

func (s Subject) String() string { return string(s) }

// EntitySubject is where an entity's changes are published. The token is the
// table name, not the Go type name, so renaming the entity struct does not move
// the subject and orphan a consumer's filter.
func EntitySubject(table string) Subject {
	return Subject(entitySubjectPrefix + table)
}

// ReplaySubject addresses a synthetic event to one consumer only.
//
// This is the reason replay gets its own subject space rather than reusing the
// entity subject: republishing an order onto fluxa.entity.orders would reach
// every consumer of orders, so reindexing Elasticsearch would re-fire the
// customer notifications that another consumer sends. A private subject makes
// replay per-consumer by construction.
func ReplaySubject(consumer ConsumerName, table string) Subject {
	return Subject(replaySubjectPrefix + string(consumer) + "." + table)
}

// replayWildcard is the one replay filter a consumer subscribes to, covering
// every entity it declared without needing a filter per entity.
func replayWildcard(consumer ConsumerName) Subject {
	return Subject(replaySubjectPrefix + string(consumer) + ".>")
}

// TaskSubject is where one task's dispatches live. Task names are Go type names
// and NATS subject tokens are case-sensitive, so no transform is needed.
func TaskSubject(queue Queue, task TaskName) Subject {
	return Subject(taskSubjectPrefix + string(queue) + "." + string(task))
}

// queueWildcard is the one filter a task consumer needs per queue it drains,
// covering every task on that queue without a filter each.
func queueWildcard(queue Queue) Subject {
	return Subject(taskSubjectPrefix + string(queue) + ".>")
}

// isTaskSubject guards the Subjected() override: a subject outside this prefix
// would not be captured by the task stream, and the server would drop the
// message with no error at the publisher.
func (s Subject) isTaskSubject() bool {
	return len(s) > len(taskSubjectPrefix) && string(s[:len(taskSubjectPrefix)]) == taskSubjectPrefix
}
