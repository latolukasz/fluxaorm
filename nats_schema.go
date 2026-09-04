package fluxaorm

import (
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type NatsStreamBuilder struct {
	poolCode        string
	streamName      string
	subjects        []string
	retention       jetstream.RetentionPolicy
	storage         jetstream.StorageType
	maxAge          time.Duration
	maxBytes        int64
	maxMsgSize      int32
	replicas        int
	duplicateWindow time.Duration
	overridden      map[string]bool
}

func NewNatsStream(streamName, poolCode string) *NatsStreamBuilder {
	return &NatsStreamBuilder{
		poolCode:   poolCode,
		streamName: streamName,
		retention:  jetstream.LimitsPolicy,
		storage:    jetstream.FileStorage,
		replicas:   1,
		overridden: make(map[string]bool),
	}
}

func (b *NatsStreamBuilder) Subjects(subjects ...string) *NatsStreamBuilder {
	b.subjects = subjects
	b.overridden["subjects"] = true
	return b
}

func (b *NatsStreamBuilder) Retention(r jetstream.RetentionPolicy) *NatsStreamBuilder {
	b.retention = r
	return b
}

func (b *NatsStreamBuilder) Storage(s jetstream.StorageType) *NatsStreamBuilder {
	b.storage = s
	return b
}

func (b *NatsStreamBuilder) MaxAge(d time.Duration) *NatsStreamBuilder {
	b.maxAge = d
	return b
}

func (b *NatsStreamBuilder) MaxBytes(n int64) *NatsStreamBuilder {
	b.maxBytes = n
	return b
}

func (b *NatsStreamBuilder) MaxMsgSize(n int32) *NatsStreamBuilder {
	b.maxMsgSize = n
	return b
}

func (b *NatsStreamBuilder) Replicas(n int) *NatsStreamBuilder {
	b.replicas = n
	return b
}

func (b *NatsStreamBuilder) Duplicates(d time.Duration) *NatsStreamBuilder {
	b.duplicateWindow = d
	return b
}

func (b *NatsStreamBuilder) validate() error {
	if b.streamName == "" {
		return fmt.Errorf("nats stream name is required")
	}
	if b.poolCode == "" {
		return fmt.Errorf("nats pool code is required for stream '%s'", b.streamName)
	}
	if len(b.subjects) == 0 {
		return fmt.Errorf("nats stream '%s' must have at least one subject", b.streamName)
	}
	return nil
}

func (b *NatsStreamBuilder) toConfig() jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:      b.streamName,
		Subjects:  b.subjects,
		Retention: b.retention,
		Storage:   b.storage,
		Replicas:  b.replicas,
	}
	if b.maxAge > 0 {
		cfg.MaxAge = b.maxAge
	}
	// JetStream reports "unlimited" as -1, so a desired 0 would compare unequal
	// to the server's own view and make every reconcile issue an UpdateStream.
	cfg.MaxBytes = -1
	if b.maxBytes > 0 {
		cfg.MaxBytes = b.maxBytes
	}
	cfg.MaxMsgSize = -1
	if b.maxMsgSize > 0 {
		cfg.MaxMsgSize = b.maxMsgSize
	}
	if b.duplicateWindow > 0 {
		cfg.Duplicates = b.duplicateWindow
	}
	return cfg
}

type NatsConsumerBuilder struct {
	poolCode       string
	name           string
	streamName     string
	filterSubjects []string
	ackWait        time.Duration
	maxAckPending  int
	maxDeliver     int
	deliverPolicy  jetstream.DeliverPolicy
}

func NewNatsConsumer(name, poolCode string) *NatsConsumerBuilder {
	return &NatsConsumerBuilder{
		poolCode:      poolCode,
		name:          name,
		ackWait:       30 * time.Second,
		maxAckPending: 1,
		maxDeliver:    -1,
		deliverPolicy: jetstream.DeliverAllPolicy,
	}
}

// Stream pins the consumer to a stream by name. Without it the reconciler has
// to guess from the first filter subject, which stops working the moment a
// consumer filters more than one.
func (b *NatsConsumerBuilder) Stream(name string) *NatsConsumerBuilder {
	b.streamName = name

	return b
}

func (b *NatsConsumerBuilder) FilterSubjects(subjects ...string) *NatsConsumerBuilder {
	b.filterSubjects = subjects
	return b
}

func (b *NatsConsumerBuilder) AckWait(d time.Duration) *NatsConsumerBuilder {
	b.ackWait = d
	return b
}

func (b *NatsConsumerBuilder) MaxAckPending(n int) *NatsConsumerBuilder {
	b.maxAckPending = n
	return b
}

func (b *NatsConsumerBuilder) MaxDeliver(n int) *NatsConsumerBuilder {
	b.maxDeliver = n
	return b
}

func (b *NatsConsumerBuilder) DeliverPolicy(p jetstream.DeliverPolicy) *NatsConsumerBuilder {
	b.deliverPolicy = p
	return b
}

func (b *NatsConsumerBuilder) validate() error {
	if b.name == "" {
		return fmt.Errorf("nats consumer name is required")
	}
	if b.poolCode == "" {
		return fmt.Errorf("nats pool code is required for consumer '%s'", b.name)
	}
	return nil
}

func (b *NatsConsumerBuilder) toSettings() *NatsConsumerSettings {
	return &NatsConsumerSettings{
		Name:           b.name,
		FilterSubjects: append([]string(nil), b.filterSubjects...),
		AckWait:        b.ackWait,
		MaxAckPending:  b.maxAckPending,
		MaxDeliver:     b.maxDeliver,
		DeliverPolicy:  b.deliverPolicy,
	}
}

func (b *NatsConsumerBuilder) toJetStreamConfig() jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		Durable:       b.name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.ackWait,
		MaxAckPending: b.maxAckPending,
		DeliverPolicy: b.deliverPolicy,
	}
	if b.maxDeliver != 0 {
		cfg.MaxDeliver = b.maxDeliver
	}
	switch len(b.filterSubjects) {
	case 0:
	case 1:
		cfg.FilterSubject = b.filterSubjects[0]
	default:
		cfg.FilterSubjects = b.filterSubjects
	}
	return cfg
}

type NatsAlter struct {
	Description string
	PoolCode    string
	Kind        AlterKind
	Safety      AlterSafety
	execFunc    func(ctx Context) error
}

func (a NatsAlter) IsSafe() bool { return a.Safety == AlterSafe }

func (a NatsAlter) Exec(ctx Context) error {
	return a.execFunc(ctx)
}

// GetNatsAlters compares registered NATS stream/consumer definitions with the actual broker state
// and returns the operations needed to synchronize them. Manages two categories of streams:
//  1. Async-flush stream (FLUXA_ASYNC_SQL)
//  2. User-registered streams from RegisterNatsStream
func GetNatsAlters(ctx Context) ([]NatsAlter, error) {
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)

	desired := collectDesiredStreams(ctx, reg)
	consumersByPool := collectDesiredConsumers(reg)

	var alters []NatsAlter
	for poolCode, streams := range desired {
		pool := ctx.Engine().Nats(poolCode)
		if pool == nil {
			return nil, fmt.Errorf("nats pool '%s' not registered", poolCode)
		}
		js, err := pool.GetJetStream()
		if err != nil {
			return nil, err
		}

		existing, err := listExistingStreams(ctx, js)
		if err != nil {
			return nil, err
		}

		for name, wanted := range streams {
			if existingCfg, ok := existing[name]; ok {
				if !streamConfigEqual(existingCfg, wanted) {
					alters = append(alters, buildUpdateStreamAlter(js, name, existingCfg, wanted, poolCode))
				}
				delete(existing, name)
				continue
			}
			alters = append(alters, buildCreateStreamAlter(js, name, wanted, poolCode))
		}

		ignoredSubjects := reg.natsIgnoredSubjects[poolCode]
		for name, existingCfg := range existing {
			if isFluxaManaged(name) && !subjectsIgnored(existingCfg.Subjects, ignoredSubjects) {
				alters = append(alters, buildDeleteStreamAlter(js, name, poolCode))
			}
		}

		for _, cons := range consumersByPool[poolCode] {
			alters = append(alters, buildEnsureConsumerAlter(js, cons, poolCode))
		}
	}

	sort.Slice(alters, func(i, j int) bool {
		return alters[i].Description < alters[j].Description
	})
	return alters, nil
}

func collectDesiredStreams(ctx Context, reg *engineRegistryImplementation) map[string]map[string]jetstream.StreamConfig {
	out := make(map[string]map[string]jetstream.StreamConfig)

	for _, b := range reg.natsStreams {
		if out[b.poolCode] == nil {
			out[b.poolCode] = make(map[string]jetstream.StreamConfig)
		}
		out[b.poolCode][b.streamName] = b.toConfig()
	}

	// One stream for every entity change and every replay. The subject
	// identifies the entity, so a stream per consumer would only re-add the
	// duplication this replaced.
	if len(reg.consumers) > 0 {
		opts := reg.entityStream
		builder := NewNatsStream(EntityStreamName, opts.NatsPool).
			Subjects(entitySubjectWildcard, replaySubjectWildcard).
			Duplicates(opts.DuplicateWindow).
			Replicas(opts.Replicas).
			MaxAge(opts.MaxAge).
			MaxBytes(opts.MaxBytes)
		if opts.Storage != 0 {
			builder.Storage(opts.Storage)
		}
		if out[opts.NatsPool] == nil {
			out[opts.NatsPool] = make(map[string]jetstream.StreamConfig)
		}
		out[opts.NatsPool][EntityStreamName] = builder.toConfig()
	}
	// One stream for every dispatched task. Separate from entity changes so a
	// task backlog cannot evict a change nobody has consumed yet.
	if len(reg.tasks) > 0 {
		opts := reg.taskStream
		builder := NewNatsStream(TaskStreamName, opts.NatsPool).
			Subjects(taskSubjectWildcard).
			Duplicates(opts.DuplicateWindow).
			Replicas(opts.Replicas).
			MaxAge(opts.MaxAge).
			MaxBytes(opts.MaxBytes)
		if opts.Storage != 0 {
			builder.Storage(opts.Storage)
		}
		if out[opts.NatsPool] == nil {
			out[opts.NatsPool] = make(map[string]jetstream.StreamConfig)
		}
		out[opts.NatsPool][TaskStreamName] = builder.toConfig()
	}

	_ = ctx
	return out
}

func collectDesiredConsumers(reg *engineRegistryImplementation) map[string][]*NatsConsumerBuilder {
	out := make(map[string][]*NatsConsumerBuilder)

	for _, b := range reg.natsConsumers {
		out[b.poolCode] = append(out[b.poolCode], b)
	}

	// One durable per declared consumer, filtering its entities plus its own
	// replay wildcard. This is the plural-FilterSubjects path, and it is what
	// lets a consumer read several entities from one stream.
	for _, consumer := range sortedConsumers(reg) {
		b := NewNatsConsumer(consumer.durable(), consumer.options.NatsPool).
			Stream(consumer.stream).
			FilterSubjects(subjectStrings(consumer.subjects)...).
			MaxAckPending(consumer.options.MaxAckPending).
			AckWait(consumer.options.AckWait).
			MaxDeliver(consumer.options.MaxDeliver)
		out[consumer.options.NatsPool] = append(out[consumer.options.NatsPool], b)
	}

	return out
}

// sortedConsumers returns the declared consumers by name, so the alter list and
// its log lines are stable across runs.
func sortedConsumers(reg *engineRegistryImplementation) []*resolvedConsumer {
	out := make([]*resolvedConsumer, 0, len(reg.consumers))
	for _, consumer := range reg.consumers {
		out = append(out, consumer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })

	return out
}

func listExistingStreams(ctx Context, js jetstream.JetStream) (map[string]jetstream.StreamConfig, error) {
	result := make(map[string]jetstream.StreamConfig)
	infos := js.ListStreams(ctx.Context())
	for info := range infos.Info() {
		if info == nil {
			continue
		}
		result[info.Config.Name] = info.Config
	}
	if err := infos.Err(); err != nil {
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}
	return result, nil
}

func streamConfigEqual(existing, wanted jetstream.StreamConfig) bool {
	if existing.Retention != wanted.Retention {
		return false
	}
	if existing.Storage != wanted.Storage {
		return false
	}
	if existing.Replicas != wanted.Replicas {
		return false
	}
	if existing.MaxAge != wanted.MaxAge {
		return false
	}
	if existing.MaxBytes != wanted.MaxBytes {
		return false
	}
	if existing.MaxMsgSize != wanted.MaxMsgSize {
		return false
	}
	if existing.Duplicates != wanted.Duplicates {
		return false
	}
	if len(existing.Subjects) != len(wanted.Subjects) {
		return false
	}
	existingSubs := append([]string(nil), existing.Subjects...)
	wantedSubs := append([]string(nil), wanted.Subjects...)
	sort.Strings(existingSubs)
	sort.Strings(wantedSubs)
	for i := range existingSubs {
		if existingSubs[i] != wantedSubs[i] {
			return false
		}
	}
	return true
}

func isFluxaManaged(streamName string) bool {
	return len(streamName) >= 6 && (streamName[:6] == "FLUXA_" || streamName[:6] == "fluxa_")
}

func subjectsIgnored(subjects []string, ignored map[string]bool) bool {
	if len(ignored) == 0 {
		return false
	}
	for _, s := range subjects {
		if ignored[s] {
			return true
		}
	}
	return false
}

func buildCreateStreamAlter(js jetstream.JetStream, name string, cfg jetstream.StreamConfig, poolCode string) NatsAlter {
	return NatsAlter{
		Description: fmt.Sprintf("CREATE nats stream '%s'", name),
		PoolCode:    poolCode,
		Kind:        AlterKindCreateTable,
		Safety:      AlterSafe,
		execFunc: func(ctx Context) error {
			_, err := js.CreateStream(ctx.Context(), cfg)
			return err
		},
	}
}

// buildUpdateStreamAlter pushes the whole wanted config, so an update that drops a subject stops
// the previous version's publishes without looking like a deletion. Only a widening update is safe.
func buildUpdateStreamAlter(js jetstream.JetStream, name string, existing, cfg jetstream.StreamConfig, poolCode string) NatsAlter {
	safety := AlterSafe
	if !isSubjectSuperset(cfg.Subjects, existing.Subjects) {
		safety = AlterDestructive
	}

	return NatsAlter{
		Description: fmt.Sprintf("UPDATE nats stream '%s'", name),
		PoolCode:    poolCode,
		Kind:        AlterKindModifyTable,
		Safety:      safety,
		execFunc: func(ctx Context) error {
			_, err := js.UpdateStream(ctx.Context(), cfg)
			return err
		},
	}
}

func buildDeleteStreamAlter(js jetstream.JetStream, name, poolCode string) NatsAlter {
	return NatsAlter{
		Description: fmt.Sprintf("DELETE nats stream '%s'", name),
		PoolCode:    poolCode,
		Kind:        AlterKindDropTable,
		Safety:      AlterDestructive,
		execFunc: func(ctx Context) error {
			return js.DeleteStream(ctx.Context(), name)
		},
	}
}

// buildEnsureConsumerAlter is safe so declaring a consumer still needs no human step. Nothing diffs
// consumers yet, so a change narrowing FilterSubjects would stop the previous version's deliveries.
func buildEnsureConsumerAlter(js jetstream.JetStream, b *NatsConsumerBuilder, poolCode string) NatsAlter {
	return NatsAlter{
		Description: fmt.Sprintf("ENSURE nats consumer '%s'", b.name),
		PoolCode:    poolCode,
		Kind:        AlterKindCreateTable,
		Safety:      AlterSafe,
		execFunc: func(ctx Context) error {
			streamName := b.streamName
			if streamName == "" {
				resolved, err := streamCoveringSubject(ctx, js, b)
				if err != nil {
					return err
				}
				streamName = resolved
			}
			_, err := js.CreateOrUpdateConsumer(ctx.Context(), streamName, b.toJetStreamConfig())

			return err
		},
	}
}

// streamCoveringSubject finds the stream carrying a consumer's first filter
// subject. Only reached by consumers registered without Stream(), where a
// single filter makes the lookup unambiguous.
func streamCoveringSubject(ctx Context, js jetstream.JetStream, b *NatsConsumerBuilder) (string, error) {
	subj := ""
	if len(b.filterSubjects) > 0 {
		subj = b.filterSubjects[0]
	}

	// Drain the lister fully before calling Err() to avoid a race with the
	// jetstream lister's background goroutine that writes the error field.
	type streamRef struct {
		name     string
		subjects []string
	}
	var collected []streamRef
	infos := js.ListStreams(ctx.Context())
	for info := range infos.Info() {
		if info == nil {
			continue
		}
		collected = append(collected, streamRef{name: info.Config.Name, subjects: info.Config.Subjects})
	}
	if err := infos.Err(); err != nil {
		return "", err
	}
	for _, ref := range collected {
		if subj == "" || subjectMatches(ref.subjects, subj) {
			return ref.name, nil
		}
	}

	return "", fmt.Errorf("no stream found that covers consumer '%s' filter subject '%s'", b.name, subj)
}

func subjectMatches(streamSubjects []string, subject string) bool {
	for _, s := range streamSubjects {
		if s == subject {
			return true
		}
		// Naive wildcard match: ends with ".>" and subject starts with prefix.
		if len(s) >= 2 && s[len(s)-2:] == ".>" {
			prefix := s[:len(s)-1]
			if len(subject) >= len(prefix) && subject[:len(prefix)] == prefix {
				return true
			}
		}
	}
	return false
}

// isSubjectSuperset reports whether wanted still covers everything existing covered.
func isSubjectSuperset(wanted, existing []string) bool {
	have := make(map[string]bool, len(wanted))
	for _, s := range wanted {
		have[s] = true
	}
	for _, s := range existing {
		if !have[s] {
			return false
		}
	}

	return true
}
