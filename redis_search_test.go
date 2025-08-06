package orm

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type redisSearchStructEntity struct {
	Name2 string `orm:"redis_search;rs_no-steam"`
	Type  int8   `orm:"redis_search"`
}

type redisSearchEntity struct {
	ID          uint64     `orm:"localCache;redisCache"`
	Age         uint8      `orm:"redis_search;rs_sortable"`
	Name        string     `orm:"redis_search"`
	NameAsTag   string     `orm:"redis_search;rs_tag"`
	Active      bool       `orm:"redis_search"`
	EnumNotNull testEnum   `orm:"required;redis_search"`
	EnumSet     []testEnum `orm:"required;redis_search"`
	Sub         redisSearchStructEntity
	Reference   Reference[redisSearchEntityReference] `orm:"redis_search;required"`
	IntArray    [2]int                                `orm:"redis_search"`
	Born        time.Time                             `orm:"redis_search;rs_sortable"`
	Created     time.Time                             `orm:"time;redis_search;rs_sortable"`
}

type redisSearchEntityReference struct {
	ID   uint64 `orm:"localCache;redisCache"`
	Name string `orm:"required"`
}

func TestRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	orm := PrepareTables(t, NewRegistry(), entity, redisSearchEntityReference{})
	schema := GetEntitySchema[redisSearchEntity](orm)
	r := orm.Engine().Redis(schema.GetRedisSearchPoolCode())

	var ids []uint64
	var idsReferences []uint64
	now := time.Now().UTC()
	for i := 1; i <= 10; i++ {
		reference := NewEntity[redisSearchEntityReference](orm)
		reference.Name = fmt.Sprintf("reference %d", reference.ID)
		entity = NewEntity[redisSearchEntity](orm)
		entity.Name = fmt.Sprintf("name %d", entity.ID)
		entity.Age = uint8(i)
		if i <= 3 {
			entity.EnumNotNull = testEnumDefinition.A
			entity.EnumSet = []testEnum{testEnumDefinition.A, testEnumDefinition.B}
		} else if i <= 7 {
			entity.Active = true
			entity.EnumNotNull = testEnumDefinition.B
			entity.EnumSet = []testEnum{testEnumDefinition.A, testEnumDefinition.C}
			entity.NameAsTag = "tag1"
		} else {
			entity.EnumNotNull = testEnumDefinition.C
			entity.EnumSet = []testEnum{testEnumDefinition.B, testEnumDefinition.C}
			entity.NameAsTag = "tag2"
		}
		entity.Born = now.AddDate(0, 0, i)
		entity.Created = now.Add(time.Duration(i) * time.Hour * 6)
		entity.Reference = Reference[redisSearchEntityReference](reference.ID)
		ids = append(ids, entity.ID)
		idsReferences = append(idsReferences, reference.ID)
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
	redisSearchAlters = GetRedisSearchAlters(orm)
	assert.Len(t, redisSearchAlters, 0)

	info, found := r.FTInfo(orm, schema.GetRedisSearchIndexName())
	assert.True(t, found)
	assert.Equal(t, 10, info.NumDocs)
	assert.Equal(t, 0, info.IndexErrors.IndexingFailures)
	for _, field := range info.FieldStatistics {
		assert.Equal(t, 0, field.IndexErrors.IndexingFailures)
	}
	res := r.FTSearch(orm, schema.GetRedisSearchIndexName(), "'*'", &redis.FTSearchOptions{NoContent: true})
	assert.NotNil(t, res)
	assert.Equal(t, 10, res.Total)

	retIds, total := RedisSearchIDs[redisSearchEntity](orm, "*", nil)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	for i, id := range ids {
		assert.Equal(t, id, retIds[i])
	}

	options := &RedisSearchOptions{
		Pager: NewPager(1, 5),
	}
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 5)
	for i, id := range ids[0:5] {
		assert.Equal(t, id, retIds[i])
	}
	options.Pager.IncrementPage()
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 5)
	for i, id := range ids[5:] {
		assert.Equal(t, id, retIds[i])
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	for i, id := range ids {
		assert.Equal(t, id, retIds[i])
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", true)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	k := 0
	for i := 9; i > 0; i-- {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddFilter("Age", 8, nil)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddFilter("Age", nil, 3)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilter("Age", 3, 5)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "name 10", options)
	assert.Equal(t, 1, total)
	assert.Len(t, retIds, 1)
	assert.Equal(t, ids[7], retIds[0])

	options = &RedisSearchOptions{}
	options.AddFilter("Age", nil, 3)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilter("Born", now.AddDate(0, 0, 3), now.AddDate(0, 0, 5))
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilter("Created", now.Add(time.Hour*6*3), now.Add(time.Hour*6*5))
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilter("Reference", idsReferences[3], idsReferences[5])
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 3; i < 6; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@EnumNotNull:{b|c}", options)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 3; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@EnumSet:{a}", options)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 0; i < 7; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@EnumSet:{a|b}", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@EnumSet:{a} @EnumSet:{b}", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@Active:{1}", options)
	assert.Equal(t, 4, total)
	assert.Len(t, retIds, 4)
	k = 0
	for i := 3; i < 6; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@Active:{0}", options)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, 6)

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@NameAsTag:{NULL}", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@NameAsTag:{tag2}", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	// TODO nullable enum, nullable set, nullable bool, nullable reference, nullable number

	//  TODO RedisSearch(), RedisSearchOne(), RedisSearchCount()

	assert.PanicsWithError(t, "entity redisSearchEntityReference is not searchable by Redis Search", func() {
		RedisSearchIDs[redisSearchEntityReference](orm, "*", nil)
	})

}
