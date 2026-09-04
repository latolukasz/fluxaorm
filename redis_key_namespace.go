package fluxaorm

import (
	"fmt"
	"sort"
)

// validateRedisKeyNamespaces rejects two entities that would share a Redis key prefix. The prefix
// is a truncated hash, so an overlap means ClearRedisCache's "<prefix>*" SCAN unlinks another
// entity's rows or its FT-indexed hashes — data loss that would read as a cache bug.
func validateRedisKeyNamespaces(schemas map[string]*entitySchema) error {
	type owner struct {
		table string
		kind  string
	}
	seen := make(map[string]owner, len(schemas)*2)

	claim := func(prefix, table, kind string) error {
		if prefix == "" {
			return nil
		}
		if previous, taken := seen[prefix]; taken {
			return fmt.Errorf("redis key prefix %q is claimed by both %s (%s) and %s (%s); "+
				"rename one of the tables", prefix, previous.table, previous.kind, table, kind)
		}
		seen[prefix] = owner{table: table, kind: kind}

		return nil
	}

	indexes := make([]string, 0, len(schemas))
	for index := range schemas {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)

	for _, index := range indexes {
		schema := schemas[index]
		if schema.hasRedisCache || schema.hasCachedUniqueIndexes {
			if err := claim(schema.cacheKey+":", schema.tableName, "row cache"); err != nil {
				return err
			}
		}
		if schema.hasRedisSearch {
			if err := claim(schema.redisSearchPrefix, schema.tableName, "search"); err != nil {
				return err
			}
		}
	}

	return nil
}
