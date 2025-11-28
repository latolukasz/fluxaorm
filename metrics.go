package fluxaorm

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metricsRegistry struct {
	queriesDB         *prometheus.HistogramVec
	queriesRedis      *prometheus.HistogramVec
	queriesRedisBlock *prometheus.CounterVec
}

func initMetricsRegistry(factory promauto.Factory) *metricsRegistry {
	reg := &metricsRegistry{}
	reg.queriesDB = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_db_queries",
		Help: "Total number of DB queries executed",
	}, []string{"operation", "pool"})
	reg.queriesRedis = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_redis_queries",
		Help: "Total number of Redis queries executed",
	}, []string{"operation", "pool", "set", "miss", "pipeline"})
	reg.queriesRedisBlock = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "fluxaorm_redis_queries_block",
		Help: "Total number of Redis blocking queries executed",
	}, []string{"operation", "pool"})
	return reg
}
