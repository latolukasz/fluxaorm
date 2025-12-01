package fluxaorm

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metricsRegistry struct {
	queriesDB          *prometheus.HistogramVec
	queriesRedis       *prometheus.HistogramVec
	queriesRedisBlock  *prometheus.CounterVec
	queriesDBErrors    *prometheus.CounterVec
	queriesRedisErrors *prometheus.CounterVec
}

func initMetricsRegistry(factory promauto.Factory) *metricsRegistry {
	reg := &metricsRegistry{}
	reg.queriesDB = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_db_queries_seconds",
		Help: "Total number of DB queries executed",
	}, []string{"operation", "pool"})
	reg.queriesRedis = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_redis_queries_seconds",
		Help: "Total number of Redis queries executed",
	}, []string{"operation", "pool", "set", "miss", "pipeline"})
	reg.queriesRedisBlock = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_redis_queries_block",
		Help: "Total number of Redis blocking queries executed",
	}, []string{"operation", "pool"})
	reg.queriesDBErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_db_queries_errors",
		Help: "Total number of DB queries errors",
	}, []string{"pool"})
	reg.queriesRedisErrors = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_redis_queries_errors",
		Help: "Total number of Redis queries errors",
	}, []string{"pool"})
	return reg
}
