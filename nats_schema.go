package fluxaorm

import (
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	AsyncSQLStreamName = "FLUXA_ASYNC_SQL"
	AsyncSQLSubject    = "fluxa.async.sql"
	AsyncSQLDLQSubject = "fluxa.async.sql.failed"
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
	if b.maxBytes > 0 {
		cfg.MaxBytes = b.maxBytes
	}
	if b.maxMsgSize > 0 {
		cfg.MaxMsgSize = b.maxMsgSize
	}
	if b.duplicateWindow > 0 {
		cfg.Duplicates = b.duplicateWindow
	}
	return cfg
}

type NatsConsumerBuilder struct {
	poolCode            string
	name                string
	filterSubjects      []string
	ackWait             time.Duration
	maxAckPending       int
	maxDeliver          int
	deliverPolicy       jetstream.DeliverPolicy
	debeziumEntityTypes []any
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

// DebeziumEntities registers Debezium-emitting entities for this consumer.
// During registry Validate(), each entity's Debezium subject is resolved and added to FilterSubjects.
func (b *NatsConsumerBuilder) DebeziumEntities(entities ...any) *NatsConsumerBuilder {
	b.debeziumEntityTypes = append(b.debeziumEntityTypes, entities...)
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
	execFunc    func(ctx Context) error
}

func (a NatsAlter) Exec(ctx Context) error {
	return a.execFunc(ctx)
}

// GetNatsAlters compares registered NATS stream/consumer definitions with the actual broker state
// and returns the operations needed to synchronize them. Manages three categories of streams:
//  1. Async-flush stream (FLUXA_ASYNC_SQL)
//  2. Per-MySQL-pool Debezium CDC streams (FLUXA_DBZ_<mysqlPool>)
//  3. User-registered streams from RegisterNatsStream
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
					alters = append(alters, buildUpdateStreamAlter(js, name, wanted, poolCode))
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

	if reg.asyncFlushNatsPool != "" {
		opts := reg.asyncFlushOptions
		builder := NewNatsStream(AsyncSQLStreamName, reg.asyncFlushNatsPool).
			Subjects(AsyncSQLSubject, AsyncSQLDLQSubject).
			Duplicates(10 * time.Minute)
		if opts != nil {
			if opts.StreamReplicas > 0 {
				builder.Replicas(opts.StreamReplicas)
			}
			if opts.DuplicateWindow > 0 {
				builder.Duplicates(opts.DuplicateWindow)
			}
		}
		if out[reg.asyncFlushNatsPool] == nil {
			out[reg.asyncFlushNatsPool] = make(map[string]jetstream.StreamConfig)
		}
		out[reg.asyncFlushNatsPool][AsyncSQLStreamName] = builder.toConfig()
	}

	debeziumPoolsForMySQL := make(map[string]string)
	for _, schema := range reg.entitySchemas {
		if schema.debeziumNatsPool == "" {
			continue
		}
		debeziumPoolsForMySQL[schema.mysqlPoolCode] = schema.debeziumNatsPool
	}
	for mysqlPool, natsPool := range debeziumPoolsForMySQL {
		streamName := "FLUXA_DBZ_" + mysqlPool
		subject := "fluxa_" + mysqlPool + ".>"
		streamBuilder := NewNatsStream(streamName, natsPool).Subjects(subject)
		if opts, ok := reg.debeziumOptions[natsPool]; ok && opts != nil && opts.StreamConfig != nil {
			merged := opts.StreamConfig
			merged.poolCode = natsPool
			merged.streamName = streamName
			if !merged.overridden["subjects"] {
				merged.subjects = []string{subject}
			}
			streamBuilder = merged
		}
		if out[natsPool] == nil {
			out[natsPool] = make(map[string]jetstream.StreamConfig)
		}
		out[natsPool][streamName] = streamBuilder.toConfig()
	}

	for _, b := range reg.natsStreams {
		if out[b.poolCode] == nil {
			out[b.poolCode] = make(map[string]jetstream.StreamConfig)
		}
		out[b.poolCode][b.streamName] = b.toConfig()
	}
	_ = ctx
	return out
}

func collectDesiredConsumers(reg *engineRegistryImplementation) map[string][]*NatsConsumerBuilder {
	out := make(map[string][]*NatsConsumerBuilder)

	if reg.asyncFlushNatsPool != "" {
		opts := reg.asyncFlushOptions
		b := NewNatsConsumer(AsyncSQLStreamName, reg.asyncFlushNatsPool).
			FilterSubjects(AsyncSQLSubject)
		if opts != nil {
			if opts.MaxAckPending > 0 {
				b.MaxAckPending(opts.MaxAckPending)
			}
			if opts.AckWait > 0 {
				b.AckWait(opts.AckWait)
			}
			if opts.MaxDeliver != 0 {
				b.MaxDeliver(opts.MaxDeliver)
			}
		}
		out[reg.asyncFlushNatsPool] = append(out[reg.asyncFlushNatsPool], b)
	}

	for _, b := range reg.natsConsumers {
		out[b.poolCode] = append(out[b.poolCode], b)
	}
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
		execFunc: func(ctx Context) error {
			_, err := js.CreateStream(ctx.Context(), cfg)
			return err
		},
	}
}

func buildUpdateStreamAlter(js jetstream.JetStream, name string, cfg jetstream.StreamConfig, poolCode string) NatsAlter {
	return NatsAlter{
		Description: fmt.Sprintf("UPDATE nats stream '%s'", name),
		PoolCode:    poolCode,
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
		execFunc: func(ctx Context) error {
			return js.DeleteStream(ctx.Context(), name)
		},
	}
}

func buildEnsureConsumerAlter(js jetstream.JetStream, b *NatsConsumerBuilder, poolCode string) NatsAlter {
	return NatsAlter{
		Description: fmt.Sprintf("ENSURE nats consumer '%s'", b.name),
		PoolCode:    poolCode,
		execFunc: func(ctx Context) error {
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
				return err
			}
			streamName := ""
			for _, ref := range collected {
				if subj == "" || subjectMatches(ref.subjects, subj) {
					streamName = ref.name
					break
				}
			}
			if streamName == "" {
				return fmt.Errorf("no stream found that covers consumer '%s' filter subject '%s'", b.name, subj)
			}
			_, err := js.CreateOrUpdateConsumer(ctx.Context(), streamName, b.toJetStreamConfig())
			return err
		},
	}
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
