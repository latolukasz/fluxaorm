package fluxaorm

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

type metricsEntity struct {
	ID   uint32
	Name string
}

func TestMetrics(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterEntity(&metricsEntity{})

	registerer := prometheus.WrapRegistererWith(map[string]string{}, prometheus.DefaultRegisterer)
	factory := promauto.With(registerer)
	registry.EnableMetrics(factory)
	orm := PrepareTables(t, registry)

	stat := testMetric(t, "fluxaorm_db_queries")
	assert.NotNil(t, stat)
	initialDBSelectCount := stat.Metric[0].GetHistogram().GetSampleCount()

	db := orm.Engine().DB(DefaultPoolCode)
	var fake int
	_, err := db.QueryRow(orm, NewWhere("SELECT 1"), &fake)
	assert.NoError(t, err)

	stat = testMetric(t, "fluxaorm_db_queries")
	assert.NotNil(t, stat)
	assert.Equal(t, "Total number of DB queries executed", stat.GetHelp())
	assert.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, stat.GetType())
	assert.Len(t, stat.Metric, 1)
	assert.Equal(t, uint64(1), stat.Metric[0].GetHistogram().GetSampleCount()-initialDBSelectCount)
	assert.Len(t, stat.Metric[0].Label, 2)
	testMetricLabel(t, stat, map[string]string{"operation": "select", "pool": DefaultPoolCode}, 0)

	_, cl, err := db.Query(orm, "SELECT 1")
	defer cl()
	assert.NoError(t, err)
	stat = testMetric(t, "fluxaorm_db_queries")
	assert.Len(t, stat.Metric, 1)
	assert.Equal(t, uint64(2), stat.Metric[0].GetHistogram().GetSampleCount()-initialDBSelectCount)
	testMetricLabel(t, stat, map[string]string{"operation": "select", "pool": DefaultPoolCode}, 0)
	cl()

	_, err = db.Exec(orm, "SELECT 1")
	assert.NoError(t, err)
	stat = testMetric(t, "fluxaorm_db_queries")
	assert.Len(t, stat.Metric, 2)
	assert.Equal(t, uint64(2), stat.Metric[1].GetHistogram().GetSampleCount()-initialDBSelectCount)
	testMetricLabel(t, stat, map[string]string{"operation": "select", "pool": DefaultPoolCode}, 1)
	assert.Equal(t, uint64(1), stat.Metric[0].GetHistogram().GetSampleCount())
	testMetricLabel(t, stat, map[string]string{"operation": "exec", "pool": DefaultPoolCode}, 0)

	tx, err := db.Begin(orm)
	assert.NoError(t, err)
	err = tx.Commit(orm)
	assert.NoError(t, err)
	stat = testMetric(t, "fluxaorm_db_queries")
	assert.Len(t, stat.Metric, 3)
}

func testMetric(t *testing.T, name string) *io_prometheus_client.MetricFamily {
	stats, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)
	for _, stat := range stats {
		if stat.GetName() == name {
			return stat
		}
	}
	return nil
}

func testMetricLabel(t *testing.T, stat *io_prometheus_client.MetricFamily, labels map[string]string, index int) {
	assert.Len(t, stat.Metric[0].Label, len(labels))
	valid := 0
	for key, value := range labels {
		for _, value2 := range stat.Metric[index].Label {
			if key == value2.GetName() {
				assert.Equal(t, value, value2.GetValue())
				valid++
			}
		}
	}
	assert.Equal(t, len(labels), valid)
}
