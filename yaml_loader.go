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
			case "nats":
				err = validateOrmNatsConfig(r, value, key)
				if err != nil {
					return err
				}
			case "kafka":
				return fmt.Errorf("kafka pool '%s' is no longer supported; rename to 'nats' and migrate keys (see documentation/MIGRATION-kafka-to-nats.md)", key)
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

func validateOrmNatsConfig(registry *registry, value any, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	var urls []string
	options := &NatsPoolOptions{}
	var consumerBuilders []*NatsConsumerBuilder
	for k, v := range def {
		switch k {
		case "urls":
			urls, err = validateOrmStrings(v, "urls")
			if err != nil {
				return err
			}
		case "clientID":
			options.ClientID, err = validateOrmString(v, "clientID")
			if err != nil {
				return err
			}
		case "maxReconnects":
			options.MaxReconnects, err = validateOrmInt(v, "maxReconnects")
			if err != nil {
				return err
			}
		case "reconnectWaitMs":
			ms, err := validateOrmInt(v, "reconnectWaitMs")
			if err != nil {
				return err
			}
			options.ReconnectWait = time.Duration(ms) * time.Millisecond
		case "reconnectBufSize":
			options.ReconnectBufSize, err = validateOrmInt(v, "reconnectBufSize")
			if err != nil {
				return err
			}
		case "authToken":
			token, err := validateOrmString(v, "authToken")
			if err != nil {
				return err
			}
			if options.Auth == nil {
				options.Auth = &NatsAuthConfig{}
			}
			options.Auth.Token = token
		case "authUser":
			user, err := validateOrmString(v, "authUser")
			if err != nil {
				return err
			}
			if options.Auth == nil {
				options.Auth = &NatsAuthConfig{}
			}
			options.Auth.User = user
		case "authPassword":
			password, err := validateOrmString(v, "authPassword")
			if err != nil {
				return err
			}
			if options.Auth == nil {
				options.Auth = &NatsAuthConfig{}
			}
			options.Auth.Password = password
		case "authCredsFile":
			creds, err := validateOrmString(v, "authCredsFile")
			if err != nil {
				return err
			}
			if options.Auth == nil {
				options.Auth = &NatsAuthConfig{}
			}
			options.Auth.CredsFile = creds
		case "consumers":
			consumerBuilders, err = validateOrmNatsConsumerBuilders(v, key)
			if err != nil {
				return err
			}
		case "ignoredSubjects":
			options.IgnoredSubjects, err = validateOrmStrings(v, "ignoredSubjects")
			if err != nil {
				return err
			}
		case "ignoredConsumers":
			options.IgnoredConsumers, err = validateOrmStrings(v, "ignoredConsumers")
			if err != nil {
				return err
			}
		case "streams":
			streams, err := validateOrmNatsStreams(v)
			if err != nil {
				return err
			}
			for _, stream := range streams {
				registry.RegisterNatsStream(stream)
			}
		}
	}
	if len(urls) == 0 {
		return fmt.Errorf("nats pool '%s': urls are required", key)
	}
	registry.RegisterNats(urls, key, options)
	for _, cb := range consumerBuilders {
		registry.RegisterNatsConsumer(cb)
	}
	return nil
}

func validateOrmNatsStreams(value any) ([]*NatsStreamBuilder, error) {
	asSlice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("orm value for streams is not valid: expected a list")
	}
	var streams []*NatsStreamBuilder
	for i, item := range asSlice {
		itemMap, err := fixYamlMap(item, fmt.Sprintf("streams[%d]", i))
		if err != nil {
			return nil, err
		}
		name := ""
		poolCode := ""
		var subjects []string
		var maxAgeMs int
		var maxBytes int64
		var maxMsgSize int32
		var replicas int
		var duplicateWindowMs int
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
			case "subjects":
				subjects, err = validateOrmStrings(v, "subjects")
				if err != nil {
					return nil, err
				}
			case "maxAgeMs":
				maxAgeMs, err = validateOrmInt(v, "maxAgeMs")
				if err != nil {
					return nil, err
				}
			case "maxBytes":
				mb, err := validateOrmInt(v, "maxBytes")
				if err != nil {
					return nil, err
				}
				maxBytes = int64(mb)
			case "maxMsgSize":
				mm, err := validateOrmInt(v, "maxMsgSize")
				if err != nil {
					return nil, err
				}
				maxMsgSize = int32(mm)
			case "replicas":
				replicas, err = validateOrmInt(v, "replicas")
				if err != nil {
					return nil, err
				}
			case "duplicateWindowMs":
				duplicateWindowMs, err = validateOrmInt(v, "duplicateWindowMs")
				if err != nil {
					return nil, err
				}
			}
		}
		if name == "" {
			return nil, fmt.Errorf("nats stream at index %d: name is required", i)
		}
		builder := NewNatsStream(name, poolCode).Subjects(subjects...)
		if maxAgeMs > 0 {
			builder.MaxAge(time.Duration(maxAgeMs) * time.Millisecond)
		}
		if maxBytes > 0 {
			builder.MaxBytes(maxBytes)
		}
		if maxMsgSize > 0 {
			builder.MaxMsgSize(maxMsgSize)
		}
		if replicas > 0 {
			builder.Replicas(replicas)
		}
		if duplicateWindowMs > 0 {
			builder.Duplicates(time.Duration(duplicateWindowMs) * time.Millisecond)
		}
		streams = append(streams, builder)
	}
	return streams, nil
}

func validateOrmNatsConsumerBuilders(value any, poolCode string) ([]*NatsConsumerBuilder, error) {
	asSlice, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("orm value for consumers is not valid: expected a list")
	}
	var builders []*NatsConsumerBuilder
	for i, item := range asSlice {
		itemMap, err := fixYamlMap(item, fmt.Sprintf("consumers[%d]", i))
		if err != nil {
			return nil, err
		}
		var name string
		var filterSubjects []string
		var ackWaitMs, maxAckPending, maxDeliver int
		for k, v := range itemMap {
			switch k {
			case "name":
				name, err = validateOrmString(v, "name")
				if err != nil {
					return nil, err
				}
			case "filterSubjects":
				filterSubjects, err = validateOrmStrings(v, "filterSubjects")
				if err != nil {
					return nil, err
				}
			case "ackWaitMs":
				ackWaitMs, err = validateOrmInt(v, "ackWaitMs")
				if err != nil {
					return nil, err
				}
			case "maxAckPending":
				maxAckPending, err = validateOrmInt(v, "maxAckPending")
				if err != nil {
					return nil, err
				}
			case "maxDeliver":
				maxDeliver, err = validateOrmInt(v, "maxDeliver")
				if err != nil {
					return nil, err
				}
			}
		}
		if name == "" {
			return nil, fmt.Errorf("nats consumer at index %d: name is required", i)
		}
		builder := NewNatsConsumer(name, poolCode).FilterSubjects(filterSubjects...)
		if ackWaitMs > 0 {
			builder.AckWait(time.Duration(ackWaitMs) * time.Millisecond)
		}
		if maxAckPending > 0 {
			builder.MaxAckPending(maxAckPending)
		}
		if maxDeliver != 0 {
			builder.MaxDeliver(maxDeliver)
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
