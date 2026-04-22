package fluxaorm

import (
	"strconv"
	"time"
)

const (
	DeadLetterTopicPrefix         = "_dlq_"
	DeadLetterConsumerGroupPrefix = "_dlq_"
	DefaultDeadLetterMaxAttempts  = 10
)

const (
	HeaderDLQError           = "dlq-error"
	HeaderDLQAttempts        = "dlq-attempts"
	HeaderDLQSourceTopic     = "dlq-source-topic"
	HeaderDLQSourcePartition = "dlq-source-partition"
	HeaderDLQSourceOffset    = "dlq-source-offset"
	HeaderDLQFirstFailedAt   = "dlq-first-failed-at"
	HeaderDLQLastFailedAt    = "dlq-last-failed-at"
)

type DeadLetterOptions struct {
	MaxAttempts     int
	TopicPartitions int32
}

type DeadLetterMetadata struct {
	Error           string
	Attempts        int
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64
	FirstFailedAt   time.Time
	LastFailedAt    time.Time
}

func DeadLetterTopicName(consumerGroupName string) string {
	return DeadLetterTopicPrefix + consumerGroupName
}

func DeadLetterConsumerGroupName(consumerGroupName string) string {
	return DeadLetterConsumerGroupPrefix + consumerGroupName
}

func ReadDeadLetterMetadata(record *KafkaRecord) DeadLetterMetadata {
	md := DeadLetterMetadata{}
	for _, h := range record.Headers {
		switch h.Key {
		case HeaderDLQError:
			md.Error = string(h.Value)
		case HeaderDLQAttempts:
			if n, err := strconv.Atoi(string(h.Value)); err == nil {
				md.Attempts = n
			}
		case HeaderDLQSourceTopic:
			md.SourceTopic = string(h.Value)
		case HeaderDLQSourcePartition:
			if n, err := strconv.ParseInt(string(h.Value), 10, 32); err == nil {
				md.SourcePartition = int32(n)
			}
		case HeaderDLQSourceOffset:
			if n, err := strconv.ParseInt(string(h.Value), 10, 64); err == nil {
				md.SourceOffset = n
			}
		case HeaderDLQFirstFailedAt:
			if t, err := time.Parse(time.RFC3339Nano, string(h.Value)); err == nil {
				md.FirstFailedAt = t
			}
		case HeaderDLQLastFailedAt:
			if t, err := time.Parse(time.RFC3339Nano, string(h.Value)); err == nil {
				md.LastFailedAt = t
			}
		}
	}
	return md
}

// buildInitialDeadLetterRecord wraps a raw source record for first-time dead-lettering.
// Any existing DLQ headers on the source are ignored (first-failure semantics).
func buildInitialDeadLetterRecord(dlqTopic string, source *KafkaRecord, errMsg string, now time.Time) *KafkaRecord {
	nowStr := now.UTC().Format(time.RFC3339Nano)
	headers := copyNonDLQHeaders(source.Headers)
	headers = append(headers,
		KafkaRecordHeader{Key: HeaderDLQError, Value: []byte(errMsg)},
		KafkaRecordHeader{Key: HeaderDLQAttempts, Value: []byte("1")},
		KafkaRecordHeader{Key: HeaderDLQSourceTopic, Value: []byte(source.Topic)},
		KafkaRecordHeader{Key: HeaderDLQSourcePartition, Value: []byte(strconv.FormatInt(int64(source.Partition), 10))},
		KafkaRecordHeader{Key: HeaderDLQSourceOffset, Value: []byte(strconv.FormatInt(source.Offset, 10))},
		KafkaRecordHeader{Key: HeaderDLQFirstFailedAt, Value: []byte(nowStr)},
		KafkaRecordHeader{Key: HeaderDLQLastFailedAt, Value: []byte(nowStr)},
	)
	return &KafkaRecord{
		Topic:   dlqTopic,
		Key:     source.Key,
		Value:   source.Value,
		Headers: headers,
	}
}

// buildRequeueDeadLetterRecord increments the attempt counter and refreshes last-failure timestamp + error,
// preserving source-* headers and dlq-first-failed-at from the original DLQ record.
func buildRequeueDeadLetterRecord(dlqTopic string, source *KafkaRecord, errMsg string, nextAttempts int, now time.Time) *KafkaRecord {
	nowStr := now.UTC().Format(time.RFC3339Nano)
	var srcTopic, srcPartition, srcOffset, firstFailedAt string
	for _, h := range source.Headers {
		switch h.Key {
		case HeaderDLQSourceTopic:
			srcTopic = string(h.Value)
		case HeaderDLQSourcePartition:
			srcPartition = string(h.Value)
		case HeaderDLQSourceOffset:
			srcOffset = string(h.Value)
		case HeaderDLQFirstFailedAt:
			firstFailedAt = string(h.Value)
		}
	}
	if firstFailedAt == "" {
		firstFailedAt = nowStr
	}
	headers := copyNonDLQHeaders(source.Headers)
	headers = append(headers,
		KafkaRecordHeader{Key: HeaderDLQError, Value: []byte(errMsg)},
		KafkaRecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(nextAttempts))},
		KafkaRecordHeader{Key: HeaderDLQSourceTopic, Value: []byte(srcTopic)},
		KafkaRecordHeader{Key: HeaderDLQSourcePartition, Value: []byte(srcPartition)},
		KafkaRecordHeader{Key: HeaderDLQSourceOffset, Value: []byte(srcOffset)},
		KafkaRecordHeader{Key: HeaderDLQFirstFailedAt, Value: []byte(firstFailedAt)},
		KafkaRecordHeader{Key: HeaderDLQLastFailedAt, Value: []byte(nowStr)},
	)
	return &KafkaRecord{
		Topic:   dlqTopic,
		Key:     source.Key,
		Value:   source.Value,
		Headers: headers,
	}
}

func copyNonDLQHeaders(in []KafkaRecordHeader) []KafkaRecordHeader {
	if len(in) == 0 {
		return nil
	}
	out := make([]KafkaRecordHeader, 0, len(in))
	for _, h := range in {
		if isDLQHeader(h.Key) {
			continue
		}
		out = append(out, h)
	}
	return out
}

func isDLQHeader(key string) bool {
	switch key {
	case HeaderDLQError,
		HeaderDLQAttempts,
		HeaderDLQSourceTopic,
		HeaderDLQSourcePartition,
		HeaderDLQSourceOffset,
		HeaderDLQFirstFailedAt,
		HeaderDLQLastFailedAt:
		return true
	}
	return false
}
