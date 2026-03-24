package fluxaorm

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// DebeziumOperation represents the type of CDC operation.
type DebeziumOperation string

const (
	DebeziumCreate DebeziumOperation = "c"
	DebeziumUpdate DebeziumOperation = "u"
	DebeziumDelete DebeziumOperation = "d"
	DebeziumRead   DebeziumOperation = "r" // snapshot read
)

// DebeziumEvent represents a parsed Debezium CDC event envelope.
type DebeziumEvent struct {
	Before map[string]any    `json:"before"`
	After  map[string]any    `json:"after"`
	Source DebeziumSource    `json:"source"`
	Op     DebeziumOperation `json:"op"`
	TsMs   int64             `json:"ts_ms"`
}

// DebeziumSource contains Debezium source metadata.
type DebeziumSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	DB        string `json:"db"`
	Table     string `json:"table"`
	ServerID  int64  `json:"server_id"`
	GTID      string `json:"gtid"`
	File      string `json:"file"`
	Pos       int64  `json:"pos"`
	Row       int    `json:"row"`
}

// ParseDebeziumEvent parses a Kafka record value into a DebeziumEvent.
func ParseDebeziumEvent(record *KafkaRecord) (*DebeziumEvent, error) {
	if record.Value == nil {
		return nil, fmt.Errorf("kafka record value is nil")
	}
	var event DebeziumEvent
	err := json.Unmarshal(record.Value, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to parse debezium event: %w", err)
	}
	return &event, nil
}

// ParseDebeziumKey extracts the entity ID from the Debezium event key.
// Debezium encodes single-column primary keys as {"ID": value}.
func ParseDebeziumKey(record *KafkaRecord) (uint64, error) {
	if record.Key == nil {
		return 0, fmt.Errorf("kafka record key is nil")
	}
	var key map[string]any
	decoder := json.NewDecoder(bytes.NewReader(record.Key))
	decoder.UseNumber()
	if err := decoder.Decode(&key); err != nil {
		return 0, fmt.Errorf("failed to parse debezium key: %w", err)
	}
	id, ok := key["ID"]
	if !ok {
		return 0, fmt.Errorf("debezium key does not contain 'ID' field")
	}
	switch v := id.(type) {
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse ID as int64: %w", err)
		}
		return uint64(n), nil
	case float64:
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unexpected ID type: %T", id)
	}
}
