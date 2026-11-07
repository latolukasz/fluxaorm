package test_generate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Only here to drive Validate; not in FixtureEntities.

type oldRedisSearchTagEntity struct {
	ID   uint64 `orm:"redisSearch=default"`
	Name string `orm:"required;length=100"`
}

type oldSearchableTagEntity struct {
	ID   uint64
	Name string `orm:"required;length=100;searchable"`
}

type oldSortableTagEntity struct {
	ID  uint64
	Age uint32 `orm:"sortable"`
}

// TestValidateRejectsTheRemovedRedisSearchTags stops an entity that still
// relies on Redis Search from validating. Accepted-but-ignored tags would leave
// it with a search index that silently stopped being maintained.
func TestValidateRejectsTheRemovedRedisSearchTags(t *testing.T) {
	cases := []struct {
		entity any
		name   string
		tag    string
	}{
		{oldRedisSearchTagEntity{}, "oldRedisSearchTagEntity", "redisSearch"},
		{oldSearchableTagEntity{}, "oldSearchableTagEntity", "searchable"},
		{oldSortableTagEntity{}, "oldSortableTagEntity", "sortable"},
	}
	for _, c := range cases {
		err := validateRegistry([]any{c.entity})

		assert.ErrorContains(t, err, "Redis Search support was removed", c.name)
		assert.ErrorContains(t, err, c.name, c.name)
		assert.ErrorContains(t, err, c.tag, c.name)
	}
}
