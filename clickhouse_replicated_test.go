package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicatedClickhouseEngine(t *testing.T) {
	cases := map[string]string{
		"MergeTree":                          "ReplicatedMergeTree",
		"MergeTree()":                        "ReplicatedMergeTree()",
		"ReplacingMergeTree(last_viewed)":    "ReplicatedReplacingMergeTree(last_viewed)",
		"AggregatingMergeTree":               "ReplicatedAggregatingMergeTree",
		"ReplicatedMergeTree":                "ReplicatedMergeTree",
		"Memory":                             "Memory",
		"ReplacingMergeTree(ver, is_delete)": "ReplicatedReplacingMergeTree(ver, is_delete)",
	}
	for in, want := range cases {
		assert.Equal(t, want, replicatedClickhouseEngine(in), in)
	}
}

func TestReplicatedClickhousePool(t *testing.T) {
	r := NewRegistry()
	require.NoError(t, r.InitByConfig(&Config{ClickhousePools: []ConfigClickhouse{
		{Code: "replicated", URI: "clickhouse://localhost:1/db", Replicated: true},
		{Code: "plain", URI: "clickhouse://localhost:1/db"},
	}}))
	replicated := NewClickhouseTable("events", "replicated").Column("id", "UInt64").Engine("ReplacingMergeTree(id)").OrderBy("id")
	plain := NewClickhouseTable("events", "plain").Column("id", "UInt64").Engine("MergeTree").OrderBy("id")
	r.RegisterClickhouseTable(replicated)
	r.RegisterClickhouseTable(plain)

	_, err := r.Validate()
	require.NoError(t, err)
	assert.Contains(t, replicated.createTableSQL(), "ENGINE = ReplicatedReplacingMergeTree(id)")
	assert.Contains(t, plain.createTableSQL(), "ENGINE = MergeTree()")
}

func TestInitByYamlReplicatedClickhouse(t *testing.T) {
	r := NewRegistry().(*registry)
	require.NoError(t, r.InitByYaml(map[string]any{"ch": map[string]any{"clickhouse": map[string]any{"uri": "clickhouse://localhost:1/db", "replicated": true}}}))
	assert.True(t, r.clickhousePools["ch"].GetOptions().Replicated)
}
