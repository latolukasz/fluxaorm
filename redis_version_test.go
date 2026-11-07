package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateRedisVersion(t *testing.T) {
	cases := []struct {
		info   string
		valkey bool
		err    string
	}{
		{info: "# Server\r\nredis_version:8.2.1\r\n", valkey: false},
		{info: "redis_version:9.0.0\n", valkey: false},
		{info: "redis_version:8.0.3\n", err: "minimum required is 8.2"},
		{info: "redis_version:7.2.4\nserver_name:valkey\nvalkey_version:9.1.0\n", valkey: true},
		{info: "redis_version:7.2.4\nvalkey_version:8.1.4\n", valkey: true, err: "Valkey 8.1.4, minimum required is 9.0"},
		{info: "redis_version:x\n", err: "unable to parse version"},
		{info: "# Server\n", err: "unable to determine Redis version"},
	}
	for _, c := range cases {
		valkey, err := validateRedisVersion(c.info, "default")
		assert.Equal(t, c.valkey, valkey, c.info)
		if c.err == "" {
			assert.NoError(t, err, c.info)
		} else {
			assert.ErrorContains(t, err, c.err, c.info)
		}
	}
}
