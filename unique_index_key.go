package fluxaorm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// UniqueIndexKeySegment names an index inside an entity's key space. Columns are folded in because
// the name alone is not unique: repointing "X on (Code)" to "X on (Slug)" would keep the old keys
// addressable.
func UniqueIndexKeySegment(indexName string, columns []string) string {
	sum := sha256.Sum256([]byte(strings.Join(columns, ",")))

	return "u:" + indexName + "@" + hex.EncodeToString(sum[:4]) + ":"
}

// UniqueIndexKeyHash addresses a row by the values looked up. sha256 because the values are
// caller-supplied and fnv32a was grindable in seconds.
func UniqueIndexKeyHash(values ...any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%v", v)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))

	return hex.EncodeToString(sum[:8])
}

// RowCacheRewriteScript replaces a cached row's value list in one step. RedisPipeLine is a batch,
// not a transaction, so two interleaved DEL+RPUSH+EXPIRE runs can leave several lists under one key.
const RowCacheRewriteScript = `
redis.call('DEL', KEYS[1])
redis.call('RPUSH', KEYS[1], unpack(ARGV, 2))
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`
