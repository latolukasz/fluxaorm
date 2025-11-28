package fluxaorm

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metricsRegistry struct {
	queriesDBTotal *prometheus.HistogramVec
}

func initMetricsRegistry(factory promauto.Factory) *metricsRegistry {
	reg := &metricsRegistry{}
	reg.queriesDBTotal = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "fluxaorm_db_queries_total",
		Help: "Total number of DB queries executed",
	}, []string{"operation", "pool"})
	return reg
}
