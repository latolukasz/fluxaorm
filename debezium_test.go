package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseDebeziumEvent(t *testing.T) {
	record := &KafkaRecord{
		Value: []byte(`{"before":null,"after":{"ID":1,"Name":"test","Age":25},"source":{"version":"2.7","connector":"mysql","name":"fluxa_default","ts_ms":1234567890,"db":"test","table":"debezium_test","server_id":1,"file":"mysql-bin.000001","pos":100,"row":0},"op":"c","ts_ms":1234567890}`),
	}
	event, err := ParseDebeziumEvent(record)
	assert.NoError(t, err)
	assert.Equal(t, DebeziumCreate, event.Op)
	assert.Nil(t, event.Before)
	assert.Equal(t, float64(1), event.After["ID"])
	assert.Equal(t, "test", event.After["Name"])
	assert.Equal(t, float64(25), event.After["Age"])
	assert.Equal(t, "test", event.Source.DB)
	assert.Equal(t, "debezium_test", event.Source.Table)
	assert.Equal(t, int64(1234567890), event.TsMs)
}

func TestParseDebeziumEventUpdate(t *testing.T) {
	record := &KafkaRecord{
		Value: []byte(`{"before":{"ID":1,"Name":"old","Age":20},"after":{"ID":1,"Name":"new","Age":30},"source":{"db":"test","table":"t"},"op":"u","ts_ms":100}`),
	}
	event, err := ParseDebeziumEvent(record)
	assert.NoError(t, err)
	assert.Equal(t, DebeziumUpdate, event.Op)
	assert.Equal(t, "old", event.Before["Name"])
	assert.Equal(t, "new", event.After["Name"])
}

func TestParseDebeziumEventDelete(t *testing.T) {
	record := &KafkaRecord{
		Value: []byte(`{"before":{"ID":1,"Name":"deleted","Age":20},"after":null,"source":{"db":"test","table":"t"},"op":"d","ts_ms":100}`),
	}
	event, err := ParseDebeziumEvent(record)
	assert.NoError(t, err)
	assert.Equal(t, DebeziumDelete, event.Op)
	assert.NotNil(t, event.Before)
	assert.Nil(t, event.After)
}

func TestParseDebeziumEventNilValue(t *testing.T) {
	record := &KafkaRecord{}
	_, err := ParseDebeziumEvent(record)
	assert.Error(t, err)
}

func TestParseDebeziumKey(t *testing.T) {
	record := &KafkaRecord{
		Key: []byte(`{"ID":42}`),
	}
	id, err := ParseDebeziumKey(record)
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), id)
}

func TestParseDebeziumKeyLargeID(t *testing.T) {
	record := &KafkaRecord{
		Key: []byte(`{"ID":9999999999}`),
	}
	id, err := ParseDebeziumKey(record)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9999999999), id)
}

func TestParseDebeziumKeyNil(t *testing.T) {
	record := &KafkaRecord{}
	_, err := ParseDebeziumKey(record)
	assert.Error(t, err)
}

func TestParseDebeziumKeyNoIDField(t *testing.T) {
	record := &KafkaRecord{
		Key: []byte(`{"other":1}`),
	}
	_, err := ParseDebeziumKey(record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ID")
}

func makeKgoFetches(records ...*kgo.Record) KafkaFetches {
	fetch := kgo.Fetch{
		Topics: []kgo.FetchTopic{
			{
				Topic: "test-topic",
			},
		},
	}
	for _, r := range records {
		if r.Topic == "" {
			r.Topic = "test-topic"
		}
		fetch.Topics[0].Partitions = append(fetch.Topics[0].Partitions, kgo.FetchPartition{
			Partition: 0,
			Records:   []*kgo.Record{r},
		})
	}
	return KafkaFetches{fetches: kgo.Fetches{fetch}}
}

func TestEachDebeziumEventHappyPath(t *testing.T) {
	fetches := makeKgoFetches(
		&kgo.Record{
			Key:   []byte(`{"ID":1}`),
			Value: []byte(`{"before":null,"after":{"ID":1,"Name":"a"},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`),
		},
		&kgo.Record{
			Key:   []byte(`{"ID":2}`),
			Value: []byte(`{"before":{"ID":2,"Name":"b"},"after":null,"source":{"db":"test","table":"t"},"op":"d","ts_ms":200}`),
		},
	)

	var collected []uint64
	err := fetches.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		collected = append(collected, entityID)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, collected)
}

func TestEachDebeziumEventSkipsTombstones(t *testing.T) {
	fetches := makeKgoFetches(
		&kgo.Record{
			Key:   []byte(`{"ID":1}`),
			Value: []byte(`{"before":null,"after":{"ID":1},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`),
		},
		&kgo.Record{
			Key:   []byte(`{"ID":2}`),
			Value: nil, // tombstone
		},
		&kgo.Record{
			Key:   []byte(`{"ID":3}`),
			Value: []byte(`{"before":null,"after":{"ID":3},"source":{"db":"test","table":"t"},"op":"c","ts_ms":300}`),
		},
	)

	var collected []uint64
	err := fetches.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		collected = append(collected, entityID)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 3}, collected)
}

func TestEachDebeziumEventHandlerError(t *testing.T) {
	fetches := makeKgoFetches(
		&kgo.Record{
			Key:   []byte(`{"ID":1}`),
			Value: []byte(`{"before":null,"after":{"ID":1},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`),
		},
		&kgo.Record{
			Key:   []byte(`{"ID":2}`),
			Value: []byte(`{"before":null,"after":{"ID":2},"source":{"db":"test","table":"t"},"op":"c","ts_ms":200}`),
		},
	)

	callCount := 0
	err := fetches.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		callCount++
		return fmt.Errorf("handler error")
	})
	assert.Error(t, err)
	assert.Equal(t, "handler error", err.Error())
	assert.Equal(t, 1, callCount)
}

func TestEachDebeziumEventParseError(t *testing.T) {
	fetches := makeKgoFetches(
		&kgo.Record{
			Key:   []byte(`{"ID":1}`),
			Value: []byte(`invalid json`),
		},
	)

	err := fetches.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		t.Fatal("handler should not be called")
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test-topic")
}

func TestEachDebeziumEventEmpty(t *testing.T) {
	fetches := KafkaFetches{}
	err := fetches.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		t.Fatal("handler should not be called")
		return nil
	})
	assert.NoError(t, err)
}
