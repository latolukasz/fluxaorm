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

// debeziumKeyHeaders lists the NATS header names that Debezium Server's NATS JetStream sink
// may use to carry the original CDC record key (originally the Kafka message key).
// The first-position name should be confirmed against the actual running Debezium Server version
// (see plan: "Implementation gate — capture the exact NATS header name from `nats sub`").
// The `Nats-` prefix is intentionally avoided: it is reserved by the NATS protocol and
// `Nats-Msg-Id` specifically collides with JetStream's WithMsgID deduplication.
var debeziumKeyHeaders = []string{
	"Debezium-Key",
	"Cdc-Key",
	"ce_id",
	"cdcid",
}

// ParseDebeziumEvent parses a NATS message Data into a DebeziumEvent envelope.
func ParseDebeziumEvent(msg *NatsMessage) (*DebeziumEvent, error) {
	if msg == nil || msg.Data == nil {
		return nil, fmt.Errorf("nats message data is nil")
	}
	var event DebeziumEvent
	err := json.Unmarshal(msg.Data, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to parse debezium event: %w", err)
	}
	return &event, nil
}

// ParseDebeziumKey extracts the entity ID from the Debezium event key.
// The key is expected in one of `debeziumKeyHeaders`, encoded as JSON `{"ID": <num>}`.
// As a fallback, the message subject's last token is tried (if numeric).
func ParseDebeziumKey(msg *NatsMessage) (uint64, error) {
	if msg == nil {
		return 0, fmt.Errorf("nats message is nil")
	}
	// First try NATS headers — some Debezium versions/configs propagate the Kafka-style
	// key as a NATS header.
	for _, name := range debeziumKeyHeaders {
		if vs := msg.Headers.Values(name); len(vs) > 0 && vs[0] != "" {
			if id, err := extractIDFromJSON([]byte(vs[0])); err == nil {
				return id, nil
			}
		}
	}
	// Verified against Debezium Server 2.7's NATS JetStream sink: the key is NOT
	// propagated as a header by default, so read the ID directly from the event envelope's
	// `after.ID` (insert/update) or `before.ID` (delete).
	if msg.Data == nil {
		return 0, fmt.Errorf("debezium key not in headers and message data is nil")
	}
	var envelope struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
	}
	dec := json.NewDecoder(bytes.NewReader(msg.Data))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		return 0, fmt.Errorf("failed to parse debezium envelope for key: %w", err)
	}
	for _, src := range []map[string]any{envelope.After, envelope.Before} {
		if src == nil {
			continue
		}
		if v, ok := src["ID"]; ok {
			switch x := v.(type) {
			case float64:
				return uint64(x), nil
			case int64:
				return uint64(x), nil
			case json.Number:
				n, err := x.Int64()
				if err != nil {
					return 0, fmt.Errorf("failed to parse ID as int64: %w", err)
				}
				return uint64(n), nil
			}
		}
	}
	return 0, fmt.Errorf("debezium key 'ID' not found in headers or event body")
}

func extractIDFromJSON(b []byte) (uint64, error) {
	var key map[string]any
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err := decoder.Decode(&key); err != nil {
		return 0, err
	}
	id, ok := key["ID"]
	if !ok {
		return 0, fmt.Errorf("no ID field")
	}
	switch v := id.(type) {
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, err
		}
		return uint64(n), nil
	case float64:
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unexpected ID type: %T", id)
	}
}
