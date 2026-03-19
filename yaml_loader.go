package fluxaorm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

func (r *registry) InitByYaml(yaml any) error {
	asMap, err := fixYamlMap(yaml, "")
	if err != nil {
		return errors.New("orm yaml is not valid")
	}

	for key, data := range asMap {
		dataAsMap, err := fixYamlMap(data, key)
		if err != nil {
			return err
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err = validateOrmMysqlURI(r, value, key)
				if err != nil {
					return err
				}
			case "redis":
				asString, ok := value.(string)
				if !ok {
					return fmt.Errorf("redis uri '%v' is not valid", value)
				}
				err = validateRedisURI(r, asString, key)
				if err != nil {
					return err
				}
			case "sentinel":
				err = validateSentinel(r, value, key)
				if err != nil {
					return err
				}
			case "clickhouse":
				err = validateOrmClickhouseURI(r, value, key)
				if err != nil {
					return err
				}
			case "kafka":
				err = validateOrmKafkaConfig(r, value, key)
				if err != nil {
					return err
				}
			case "local_cache":
				limit, err := validateOrmInt(value, key)
				if err != nil {
					return err
				}
				r.RegisterLocalCache(key, limit)
			}
		}
	}
	return nil
}

func validateOrmClickhouseURI(registry *registry, value any, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	uri := ""
	options := &ClickhouseOptions{}
	for k, v := range def {
		switch k {
		case "uri":
			uri, err = validateOrmString(v, "uri")
			if err != nil {
				return err
			}
		case "connMaxLifetime":
			connMaxLifetime, err := validateOrmInt(v, "connMaxLifetime")
			if err != nil {
				return err
			}
			options.ConnMaxLifetime = time.Duration(connMaxLifetime) * time.Second
		case "maxOpenConnections":
			options.MaxOpenConnections, err = validateOrmInt(v, "maxOpenConnections")
			if err != nil {
				return err
			}
		case "maxIdleConnections":
			options.MaxIdleConnections, err = validateOrmInt(v, "maxIdleConnections")
			if err != nil {
				return err
			}
		case "ignoredTables":
			options.IgnoredTables, err = validateOrmStrings(v, "ignoredTables")
			if err != nil {
				return err
			}
		}
	}
	registry.RegisterClickhouse(uri, key, options)
	return nil
}

func validateOrmMysqlURI(registry *registry, value any, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	uri := ""
	options := &MySQLOptions{}
	for k, v := range def {
		switch k {
		case "uri":
			uri, err = validateOrmString(v, "uri")

		case "connMaxLifetime":
			connMaxLifetime, err := validateOrmInt(v, "connMaxLifetime")
			if err != nil {
				return err
			}
			options.ConnMaxLifetime = time.Duration(connMaxLifetime) * time.Second
		case "maxOpenConnections":
			options.MaxOpenConnections, err = validateOrmInt(v, "maxOpenConnections")
			if err != nil {
				return err
			}
		case "maxIdleConnections":
			options.MaxIdleConnections, err = validateOrmInt(v, "maxIdleConnections")
			if err != nil {
				return err
			}
		case "defaultEncoding":
			options.DefaultEncoding, err = validateOrmString(v, "defaultEncoding")
			if err != nil {
				return err
			}
		case "defaultCollate":
			options.DefaultCollate, err = validateOrmString(v, "defaultCollate")
			if err != nil {
				return err
			}
		case "ignoredTables":
			options.IgnoredTables, err = validateOrmStrings(v, "ignoredTables")
			if err != nil {
				return err
			}
		}
	}
	registry.RegisterMySQL(uri, key, options)
	return nil
}

func validateRedisURI(registry *registry, value string, key string) error {
	parts := strings.Split(value, "?")
	elements := strings.Split(parts[0], ":")
	dbNumber := ""
	uri := ""
	isSocket := strings.Index(parts[0], ".sock") > 0
	l := len(elements)
	switch l {
	case 2:
		dbNumber = elements[1]
		uri = elements[0]
	case 3:
		if isSocket {
			dbNumber = elements[1]
			uri = elements[0]
		} else {
			dbNumber = elements[2]
			uri = elements[0] + ":" + elements[1]
		}
	case 4:
		dbNumber = elements[2]
		uri = elements[0] + ":" + elements[1]
	default:
		return fmt.Errorf("redis uri '%v' is not valid", value)
	}
	db, err := strconv.ParseUint(dbNumber, 10, 64)
	if err != nil {
		return fmt.Errorf("redis uri '%v' is not valid", value)
	}
	var options *RedisOptions
	if len(parts) == 2 && parts[1] != "" {
		values, err := url.ParseQuery(parts[1])
		if err != nil {
			return fmt.Errorf("redis uri '%v' is not valid", value)
		}
		if values.Has("user") && values.Has("password") {
			options = &RedisOptions{User: values.Get("user"), Password: values.Get("password")}
		}
	}
	registry.RegisterRedis(uri, int(db), key, options)
	return nil
}

func validateSentinel(registry *registry, value any, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	for master, values := range def {
		asSlice, ok := values.([]any)
		if !ok {
			return fmt.Errorf("sentinel '%v' is not valid", value)
		}
		asStrings := make([]string, len(asSlice))
		for i, v := range asSlice {
			asStrings[i] = fmt.Sprintf("%v", v)
		}
		db := 0
		parts := strings.Split(master, "?")
		elements := strings.Split(parts[0], ":")
		l := len(elements)
		if l >= 2 {
			master = elements[0]
			nr, err := strconv.ParseUint(elements[1], 10, 64)
			if err != nil {
				return fmt.Errorf("sentinel db '%v' is not valid", value)
			}
			db = int(nr)
		}
		options := &RedisOptions{Master: master, Sentinels: asStrings}
		if len(parts) == 2 && parts[1] != "" {
			extra, err := url.ParseQuery(parts[1])
			if err != nil {
				return fmt.Errorf("sentinel uri '%v' is not valid", master)
			}
			if extra.Has("user") && extra.Has("password") {
				options.User = extra.Get("user")
				options.Password = extra.Get("password")
			}
		}
		registry.RegisterRedis("", db, key, options)
	}
	return nil
}

func fixYamlMap(value any, key string) (map[string]any, error) {
	def, ok := value.(map[string]any)
	if !ok {
		def2, ok := value.(map[any]any)
		if !ok {
			return nil, fmt.Errorf("orm yaml key %s is not valid", key)
		}
		def = make(map[string]any)
		for k, v := range def2 {
			def[fmt.Sprintf("%v", k)] = v
		}
	}
	return def, nil
}

func validateOrmKafkaConfig(registry *registry, value any, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	var brokers []string
	options := &KafkaPoolOptions{}
	var consumerGroupBuilders []*KafkaConsumerGroupBuilder
	for k, v := range def {
		switch k {
		case "brokers":
			brokers, err = validateOrmStrings(v, "brokers")
			if err != nil {
				return err
			}
		case "clientID":
			options.ClientID, err = validateOrmString(v, "clientID")
			if err != nil {
				return err
			}
		case "requiredAcks":
			options.RequiredAcks, err = validateOrmInt(v, "requiredAcks")
			if err != nil {
				return err
			}
		case "producerLingerMs":
			ms, err := validateOrmInt(v, "producerLingerMs")
			if err != nil {
				return err
			}
			options.ProducerLinger = time.Duration(ms) * time.Millisecond
		case "maxBufferedRecords":
			options.MaxBufferedRecords, err = validateOrmInt(v, "maxBufferedRecords")
			if err != nil {
				return err
			}
		case "saslMechanism":
			mechanism, err := validateOrmString(v, "saslMechanism")
			if err != nil {
				return err
			}
			if options.SASL == nil {
				options.SASL = &KafkaSASLConfig{}
			}
			options.SASL.Mechanism = mechanism
		case "saslUser":
			user, err := validateOrmString(v, "saslUser")
			if err != nil {
				return err
			}
			if options.SASL == nil {
				options.SASL = &KafkaSASLConfig{}
			}
			options.SASL.User = user
		case "saslPassword":
			password, err := validateOrmString(v, "saslPassword")
			if err != nil {
				return err
			}
			if options.SASL == nil {
				options.SASL = &KafkaSASLConfig{}
			}
			options.SASL.Password = password
		case "consumerGroups":
			consumerGroupBuilders, err = validateOrmKafkaConsumerGroupBuilders(v, key)
			if err != nil {
				return err
			}
		case "ignoredTopics":
			options.IgnoredTopics, err = validateOrmStrings(v, "ignoredTopics")
			if err != nil {
				return err
			}
		case "ignoredConsumerGroups":
			options.IgnoredConsumerGroups, err = validateOrmStrings(v, "ignoredConsumerGroups")
			if err != nil {
				return err
			}
		case "topics":
			topics, err := validateOrmKafkaTopics(v)
			if err != nil {
				return err
			}
			for _, topic := range topics {
				registry.RegisterKafkaTopic(topic)
			}
		}
	}
	if len(brokers) == 0 {
		return fmt.Errorf("kafka pool '%s': brokers are required", key)
	}
	registry.RegisterKafka(brokers, key, options)
	for _, cgBuilder := range consumerGroupBuilders {
		registry.RegisterKafkaConsumerGroup(cgBuilder)
	}
	return nil
}

func validateOrmKafkaTopics(value any) ([]*KafkaTopicBuilder, error) {
	asSlice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("orm value for topics is not valid: expected a list")
	}
	var topics []*KafkaTopicBuilder
	for i, item := range asSlice {
		itemMap, err := fixYamlMap(item, fmt.Sprintf("topics[%d]", i))
		if err != nil {
			return nil, err
		}
		name := ""
		poolCode := ""
		var partitions int32
		var replicationFactor int16
		configs := make(map[string]string)
		for k, v := range itemMap {
			switch k {
			case "name":
				name, err = validateOrmString(v, "name")
				if err != nil {
					return nil, err
				}
			case "poolCode":
				poolCode, err = validateOrmString(v, "poolCode")
				if err != nil {
					return nil, err
				}
			case "partitions":
				p, err := validateOrmInt(v, "partitions")
				if err != nil {
					return nil, err
				}
				partitions = int32(p)
			case "replicationFactor":
				rf, err := validateOrmInt(v, "replicationFactor")
				if err != nil {
					return nil, err
				}
				replicationFactor = int16(rf)
			case "configs":
				configMap, err := fixYamlMap(v, "configs")
				if err != nil {
					return nil, err
				}
				for ck, cv := range configMap {
					configs[ck] = fmt.Sprintf("%v", cv)
				}
			}
		}
		if name == "" {
			return nil, fmt.Errorf("kafka topic at index %d: name is required", i)
		}
		builder := NewKafkaTopic(name, poolCode)
		if partitions > 0 {
			builder.Partitions(partitions)
		}
		if replicationFactor > 0 {
			builder.ReplicationFactor(replicationFactor)
		}
		for ck, cv := range configs {
			builder.Config(ck, cv)
		}
		topics = append(topics, builder)
	}
	return topics, nil
}

func validateOrmKafkaConsumerGroupBuilders(value any, poolCode string) ([]*KafkaConsumerGroupBuilder, error) {
	asSlice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("orm value for consumerGroups is not valid: expected a list")
	}
	var builders []*KafkaConsumerGroupBuilder
	for i, item := range asSlice {
		itemMap, err := fixYamlMap(item, fmt.Sprintf("consumerGroups[%d]", i))
		if err != nil {
			return nil, err
		}
		var name string
		var topics []string
		var sessionTimeoutMs, rebalanceTimeoutMs, fetchMaxBytes, autoCommitIntervalMs int
		for k, v := range itemMap {
			switch k {
			case "name":
				name, err = validateOrmString(v, "name")
				if err != nil {
					return nil, err
				}
			case "topics":
				topics, err = validateOrmStrings(v, "topics")
				if err != nil {
					return nil, err
				}
			case "sessionTimeoutMs":
				sessionTimeoutMs, err = validateOrmInt(v, "sessionTimeoutMs")
				if err != nil {
					return nil, err
				}
			case "rebalanceTimeoutMs":
				rebalanceTimeoutMs, err = validateOrmInt(v, "rebalanceTimeoutMs")
				if err != nil {
					return nil, err
				}
			case "fetchMaxBytes":
				fetchMaxBytes, err = validateOrmInt(v, "fetchMaxBytes")
				if err != nil {
					return nil, err
				}
			case "autoCommitIntervalMs":
				autoCommitIntervalMs, err = validateOrmInt(v, "autoCommitIntervalMs")
				if err != nil {
					return nil, err
				}
			}
		}
		if name == "" {
			return nil, fmt.Errorf("consumer group at index %d: name is required", i)
		}
		if len(topics) == 0 {
			return nil, fmt.Errorf("consumer group '%s': topics are required", name)
		}
		builder := NewKafkaConsumerGroup(name, poolCode).Topics(topics...)
		if sessionTimeoutMs > 0 {
			builder.SessionTimeout(time.Duration(sessionTimeoutMs) * time.Millisecond)
		}
		if rebalanceTimeoutMs > 0 {
			builder.RebalanceTimeout(time.Duration(rebalanceTimeoutMs) * time.Millisecond)
		}
		if fetchMaxBytes > 0 {
			builder.FetchMaxBytes(int32(fetchMaxBytes))
		}
		if autoCommitIntervalMs > 0 {
			builder.AutoCommitInterval(time.Duration(autoCommitIntervalMs) * time.Millisecond)
		}
		builders = append(builders, builder)
	}
	return builders, nil
}

func validateOrmInt(value any, key string) (int, error) {
	asInt, ok := value.(int)
	if !ok {
		return 0, fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	return asInt, nil
}

func validateOrmString(value any, key string) (string, error) {
	asString, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	return asString, nil
}

func validateOrmStrings(value any, key string) ([]string, error) {
	asSlice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	asStrings := make([]string, len(asSlice))
	for i, val := range asSlice {
		asString, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("orm value for %s: %v is not valid", key, value)
		}
		asStrings[i] = asString
	}
	return asStrings, nil
}
