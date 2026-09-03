package fluxaorm

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const MetricsMetaKey = "MetrictMetaKey"

type metricsRegistry struct {
	queriesDB               *prometheus.HistogramVec
	queriesRedis            *prometheus.HistogramVec
	queriesRedisBlock       *prometheus.CounterVec
	queriesDBErrors         *prometheus.CounterVec
	queriesRedisErrors      *prometheus.CounterVec
	queriesClickhouse       *prometheus.HistogramVec
	queriesClickhouseErrors *prometheus.CounterVec
	queriesNats             *prometheus.HistogramVec
	queriesNatsErrors       *prometheus.CounterVec
	natsPublishBatchSize    *prometheus.HistogramVec
	cdcMessages             *prometheus.CounterVec
	streamLag               *prometheus.HistogramVec
}

func initMetricsRegistry(factory promauto.Factory) *metricsRegistry {
	reg := &metricsRegistry{}
	reg.queriesDB = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_db_queries_seconds",
		Help: "Total number of DB queries executed",
	}, []string{"operation", "pool", "source"})
	reg.queriesRedis = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_redis_queries_seconds",
		Help: "Total number of Redis queries executed",
	}, []string{"operation", "pool", "set", "miss", "pipeline", "source"})
	reg.queriesRedisBlock = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_redis_queries_block",
		Help: "Total number of Redis blocking queries executed",
	}, []string{"operation", "pool", "source"})
	reg.queriesDBErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_db_queries_errors",
		Help: "Total number of DB queries errors",
	}, []string{"pool", "source"})
	reg.queriesRedisErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_redis_queries_errors",
		Help: "Total number of Redis queries errors",
	}, []string{"pool", "source"})
	reg.queriesClickhouse = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_clickhouse_queries_seconds",
		Help: "Total number of ClickHouse queries executed",
	}, []string{"operation", "pool", "source"})
	reg.queriesClickhouseErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_clickhouse_queries_errors",
		Help: "Total number of ClickHouse queries errors",
	}, []string{"pool", "source"})
	reg.queriesNats = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_nats_operations_seconds",
		Help: "Total number of NATS operations executed",
	}, []string{"operation", "pool", "source", "consumer"})
	reg.queriesNatsErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_nats_operations_errors",
		Help: "Total number of NATS operation errors",
	}, []string{"pool", "source", "consumer"})
	// Batch size shows whether PublishBatch is actually amortising round-trips or
	// degenerating into a stream of single-message publishes.
	reg.natsPublishBatchSize = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fluxaorm_nats_publish_batch_size",
		Help:    "Number of messages per batched NATS publish",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"pool", "source"})
	reg.cdcMessages = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_cdc_messages_total",
		Help: "Total number of entity change events fetched from JetStream",
	}, []string{"consumer", "entity", "op"})
	// Lag is dispatch-to-consume; broker-side publish-accept timestamp serves as
	// the dispatch reference. Buckets span 1ms → ~256s to cover both hot streams
	// and worst-case backlog scenarios.
	// Labelled by consumer rather than by stream: every entity consumer now
	// shares one stream, so a stream label would collapse them into one series.
	reg.streamLag = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fluxaorm_stream_consume_lag_seconds",
		Help:    "Time from publish (JetStream broker timestamp) to consume, per consumer",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
	}, []string{"consumer"})
	return reg
}
