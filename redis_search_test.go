package orm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type redisSearchStructEntity struct {
	Name2 string `orm:"redis_search;rs_no-steam"`
	Type  int8   `orm:"redis_search;rs_type=tag"`
}

type redisSearchEntity struct {
	ID          uint64     `orm:"localCache;redisCache"`
	Age         uint8      `orm:"redis_search;rs_sortable"`
	Name        string     `orm:"redis_search"`
	EnumNotNull testEnum   `orm:"required;redis_search"`
	EnumSet     []testEnum `orm:"required;redis_search"`
	Sub         redisSearchStructEntity
	Reference   Reference[redisSearchEntityReference] `orm:"redis_search"`
	IntArray    [2]int                                `orm:"redis_search"`
}

type redisSearchEntityReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	orm := PrepareTables(t, NewRegistry(), entity, redisSearchEntityReference{})
	_ = GetEntitySchema[redisSearchEntity](orm)

	var ids []uint64
	for i := 1; i <= 10; i++ {
		entity = NewEntity[redisSearchEntity](orm)
		entity.Name = fmt.Sprintf("name %d", entity.ID)
		ids = append(ids, entity.ID)
	}
	err := orm.Flush()
	assert.NoError(t, err)

	// Reindex
	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	redisSearchAlters := GetRedisSearchAlters(orm)
	assert.Len(t, redisSearchAlters, 1)
	for _, alter := range redisSearchAlters {
		alter.Exec(orm)
	}

	fmt.Printf("A\n")
	redisSearchAlters = GetRedisSearchAlters(orm)
	assert.Len(t, redisSearchAlters, 0)
}
