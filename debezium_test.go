package fluxaorm

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

// makeNatsMessage builds a NatsMessage for testing the Debezium parser.
// `keyHeader` is the header name carrying the JSON-encoded CDC key
// (e.g. "Debezium-Key"); pass "" to omit the header entirely (used for
// negative-path tests).
func makeNatsMessage(subject, keyHeader, key string, value []byte) *NatsMessage {
	msg := NewNatsMessage(subject)
	msg.Data = value
	if keyHeader != "" {
		msg.Headers.Set(keyHeader, key)
	}
	return msg
}

func TestParseDebeziumEvent(t *testing.T) {
	msg := makeNatsMessage("fluxa_default.test.debezium_test", "Debezium-Key", `{"ID":1}`,
		[]byte(`{"before":null,"after":{"ID":1,"Name":"test","Age":25},"source":{"version":"2.7","connector":"mysql","name":"fluxa_default","ts_ms":1234567890,"db":"test","table":"debezium_test","server_id":1,"file":"mysql-bin.000001","pos":100,"row":0},"op":"c","ts_ms":1234567890}`),
	)
	event, err := ParseDebeziumEvent(msg)
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
	msg := makeNatsMessage("t", "Debezium-Key", `{"ID":1}`,
		[]byte(`{"before":{"ID":1,"Name":"old","Age":20},"after":{"ID":1,"Name":"new","Age":30},"source":{"db":"test","table":"t"},"op":"u","ts_ms":100}`),
	)
	event, err := ParseDebeziumEvent(msg)
	assert.NoError(t, err)
	assert.Equal(t, DebeziumUpdate, event.Op)
	assert.Equal(t, "old", event.Before["Name"])
	assert.Equal(t, "new", event.After["Name"])
}

func TestParseDebeziumEventDelete(t *testing.T) {
	msg := makeNatsMessage("t", "Debezium-Key", `{"ID":1}`,
		[]byte(`{"before":{"ID":1,"Name":"deleted","Age":20},"after":null,"source":{"db":"test","table":"t"},"op":"d","ts_ms":100}`),
	)
	event, err := ParseDebeziumEvent(msg)
	assert.NoError(t, err)
	assert.Equal(t, DebeziumDelete, event.Op)
	assert.NotNil(t, event.Before)
	assert.Nil(t, event.After)
}

func TestParseDebeziumEventNilValue(t *testing.T) {
	msg := NewNatsMessage("t")
	_, err := ParseDebeziumEvent(msg)
	assert.Error(t, err)
}

func TestParseDebeziumKey(t *testing.T) {
	msg := makeNatsMessage("t", "Debezium-Key", `{"ID":42}`, []byte(`{}`))
	id, err := ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), id)
}

func TestParseDebeziumKeyFallbackHeaders(t *testing.T) {
	// Cdc-Key fallback
	msg := makeNatsMessage("t", "Cdc-Key", `{"ID":7}`, []byte(`{}`))
	id, err := ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), id)
	// ce_id fallback
	msg = makeNatsMessage("t", "ce_id", `{"ID":8}`, []byte(`{}`))
	id, err = ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), id)
	// cdcid (historical Debezium) fallback
	msg = makeNatsMessage("t", "cdcid", `{"ID":9}`, []byte(`{}`))
	id, err = ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), id)
}

func TestParseDebeziumKeyLargeID(t *testing.T) {
	msg := makeNatsMessage("t", "Debezium-Key", `{"ID":9999999999}`, []byte(`{}`))
	id, err := ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9999999999), id)
}

func TestParseDebeziumKeyMissing(t *testing.T) {
	// No headers, empty envelope (no after/before with ID) — should fail
	msg := NewNatsMessage("t")
	msg.Data = []byte(`{"before":null,"after":null}`)
	_, err := ParseDebeziumKey(msg)
	assert.Error(t, err)
}

func TestParseDebeziumKeyFromEnvelopeAfter(t *testing.T) {
	// No header, ID in envelope.after — Debezium Server 2.7 NATS sink behavior
	msg := NewNatsMessage("t")
	msg.Data = []byte(`{"before":null,"after":{"ID":99,"Name":"x"},"op":"c"}`)
	id, err := ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), id)
}

func TestParseDebeziumKeyFromEnvelopeBefore(t *testing.T) {
	// Delete event: after is nil, ID lives in before
	msg := NewNatsMessage("t")
	msg.Data = []byte(`{"before":{"ID":77,"Name":"x"},"after":null,"op":"d"}`)
	id, err := ParseDebeziumKey(msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), id)
}

func TestParseDebeziumKeyNoIDField(t *testing.T) {
	msg := makeNatsMessage("t", "Debezium-Key", `{"other":1}`, []byte(`{}`))
	_, err := ParseDebeziumKey(msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ID")
}

func makeNatsBatch(messages ...*NatsMessage) NatsBatch {
	return NatsBatch{messages: messages}
}

func TestEachDebeziumEventHappyPath(t *testing.T) {
	batch := makeNatsBatch(
		makeNatsMessage("t", "Debezium-Key", `{"ID":1}`,
			[]byte(`{"before":null,"after":{"ID":1,"Name":"a"},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`)),
		makeNatsMessage("t", "Debezium-Key", `{"ID":2}`,
			[]byte(`{"before":{"ID":2,"Name":"b"},"after":null,"source":{"db":"test","table":"t"},"op":"d","ts_ms":200}`)),
	)

	var collected []uint64
	err := batch.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		collected = append(collected, entityID)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, collected)
}

func TestEachDebeziumEventSkipsTombstones(t *testing.T) {
	tombstone := NewNatsMessage("t")
	tombstone.Headers = nats.Header{}
	tombstone.Headers.Set("Debezium-Key", `{"ID":2}`)
	// tombstone has nil Data — should be skipped silently
	batch := makeNatsBatch(
		makeNatsMessage("t", "Debezium-Key", `{"ID":1}`,
			[]byte(`{"before":null,"after":{"ID":1},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`)),
		tombstone,
		makeNatsMessage("t", "Debezium-Key", `{"ID":3}`,
			[]byte(`{"before":null,"after":{"ID":3},"source":{"db":"test","table":"t"},"op":"c","ts_ms":300}`)),
	)

	var collected []uint64
	err := batch.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		collected = append(collected, entityID)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 3}, collected)
}

func TestEachDebeziumEventHandlerError(t *testing.T) {
	batch := makeNatsBatch(
		makeNatsMessage("t", "Debezium-Key", `{"ID":1}`,
			[]byte(`{"before":null,"after":{"ID":1},"source":{"db":"test","table":"t"},"op":"c","ts_ms":100}`)),
		makeNatsMessage("t", "Debezium-Key", `{"ID":2}`,
			[]byte(`{"before":null,"after":{"ID":2},"source":{"db":"test","table":"t"},"op":"c","ts_ms":200}`)),
	)

	callCount := 0
	err := batch.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		callCount++
		return fmt.Errorf("handler error")
	})
	assert.Error(t, err)
	assert.Equal(t, "handler error", err.Error())
	assert.Equal(t, 1, callCount)
}

func TestEachDebeziumEventParseError(t *testing.T) {
	batch := makeNatsBatch(
		makeNatsMessage("fluxa_default.test.t", "Debezium-Key", `{"ID":1}`, []byte(`invalid json`)),
	)
	err := batch.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		t.Fatal("handler should not be called")
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fluxa_default.test.t")
}

func TestEachDebeziumEventEmpty(t *testing.T) {
	batch := NatsBatch{}
	err := batch.EachDebeziumEvent(func(entityID uint64, event *DebeziumEvent) error {
		t.Fatal("handler should not be called")
		return nil
	})
	assert.NoError(t, err)
}
