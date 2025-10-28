package fluxaorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type redisSearchStructEntity struct {
	Name2 string `orm:"redis_search;rs_no-steam"`
	Type  int8   `orm:"redis_search"`
}

type redisSearchEntity struct {
	ID            uint64     `orm:"localCache;redis_search_pool=default"`
	Age           uint8      `orm:"redis_search;rs_sortable"`
	Name          string     `orm:"redis_search"`
	NameAsTag     string     `orm:"redis_search;rs_tag"`
	Active        bool       `orm:"redis_search"`
	ActiveNull    *bool      `orm:"redis_search"`
	EnumNotNull   testEnum   `orm:"required;redis_search"`
	EnumNull      testEnum   `orm:"redis_search"`
	EnumSet       []testEnum `orm:"required;redis_search"`
	Sub           redisSearchStructEntity
	Reference     Reference[redisSearchEntityReference] `orm:"redis_search;required"`
	ReferenceNull Reference[redisSearchEntityReference] `orm:"redis_search"`
	IntArray      [2]int                                `orm:"redis_search"`
	Born          time.Time                             `orm:"redis_search;rs_sortable"`
	Created       time.Time                             `orm:"time;redis_search;rs_sortable"`
	CreatedNull   *time.Time                            `orm:"redis_search"`
}

type redisSearchEntityReference struct {
	ID   uint64 `orm:"localCache"`
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
			entity.EnumNull = testEnumDefinition.B
			entity.EnumSet = []testEnum{testEnumDefinition.A, testEnumDefinition.C}
			entity.NameAsTag = "tag1"
			tr := true
			entity.ActiveNull = &tr
			entity.ReferenceNull = Reference[redisSearchEntityReference](reference.ID)
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
	err := orm.FlushWithCheck()
	assert.NoError(t, err)

	testRedisSearchResults(t, r, orm, schema, ids, now, idsReferences)

	// Reindex
	orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	redisSearchAlters := GetRedisSearchAlters(orm)
	assert.Len(t, redisSearchAlters, 1)
	for _, alter := range redisSearchAlters {
		alter.Exec(orm)
	}
	redisSearchAlters = GetRedisSearchAlters(orm)
	assert.Len(t, redisSearchAlters, 0)

	testRedisSearchResults(t, r, orm, schema, ids, now, idsReferences)

	e, _ := GetByID[redisSearchEntity](orm, ids[0])
	DeleteEntity(orm, e)
	assert.NoError(t, orm.FlushWithCheck())

	res := r.FTSearch(orm, schema.GetRedisSearchIndexName(), "*", &redis.FTSearchOptions{NoContent: true})
	assert.NotNil(t, res)
	assert.Equal(t, 9, res.Total)

	options := &RedisSearchOptions{}
	options.AddFilterNumber("Age", 1)
	_, found := RedisSearchOne[redisSearchEntity](orm, "*", options)
	assert.False(t, found)

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total := RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 9, total)
	assert.Len(t, retIds, 9)
	for i := 1; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[i-1])
	}

	e, _ = GetByID[redisSearchEntity](orm, ids[1])
	e = EditEntity(orm, e)
	e.Age = 100
	assert.NoError(t, orm.FlushWithCheck())

	options = &RedisSearchOptions{}
	options.AddFilterNumber("Age", 100)
	e, found = RedisSearchOne[redisSearchEntity](orm, "*", options)
	assert.True(t, found)
	assert.Equal(t, fmt.Sprintf("name %d", ids[1]), e.Name)

	assert.PanicsWithError(t, "entity redisSearchEntityReference is not searchable by Redis Search", func() {
		RedisSearchIDs[redisSearchEntityReference](orm, "*", nil)
	})

}

func testRedisSearchResults(t *testing.T, r RedisCache, orm Context, schema EntitySchema, ids []uint64, now time.Time, idsReferences []uint64) {
	info, found := r.FTInfo(orm, schema.GetRedisSearchIndexName())
	assert.True(t, found)
	assert.Equal(t, 10, info.NumDocs)
	assert.Equal(t, 0, info.IndexErrors.IndexingFailures)
	for _, field := range info.FieldStatistics {
		assert.Equal(t, 0, field.IndexErrors.IndexingFailures)
	}
	res := r.FTSearch(orm, schema.GetRedisSearchIndexName(), "*", &redis.FTSearchOptions{NoContent: true})
	assert.NotNil(t, res)
	assert.Equal(t, 10, res.Total)
	options := &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total := RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	for i, id := range ids {
		assert.Equal(t, id, retIds[i])
	}

	options = &RedisSearchOptions{
		Pager: NewPager(1, 5),
	}
	options.AddSortBy("Age", false)
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
	options.AddFilterNumberGreaterEqual("Age", 8)
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddFilterNumberLessEqual("Age", 3)
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilterNumberRange("Age", 3, 5)
	options.AddSortBy("Age", false)
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
	options.AddFilterNumberLessEqual("Age", 3)
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilterDateRange("Born", now.AddDate(0, 0, 3), now.AddDate(0, 0, 5))
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilterDateRange("Created", now.Add(time.Hour*6*3), now.Add(time.Hour*6*5))
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddFilterNumberRange("Reference", int64(idsReferences[3]), int64(idsReferences[5]))
	options.AddSortBy("Age", false)
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
	options.AddFilterTag("EnumNotNull", "b", "c")
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 3; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterTag("EnumSet", "a")
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "", options)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 0; i < 7; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterTag("EnumSet", "a", "b")
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "", options)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterTag("EnumSet", "a")
	options.AddFilterTag("EnumSet", "b")
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterBoolean("Active", true)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "", options)
	assert.Equal(t, 4, total)
	assert.Len(t, retIds, 4)
	k = 0
	for i := 3; i < 6; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterBoolean("Active", false)
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
	options.AddFilterTag("NameAsTag", "tag2")
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "", options)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@EnumNull:{NULL}", options)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, 6)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "@ActiveNull:{NULL}", options)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, 6)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	options = &RedisSearchOptions{}
	options.AddSortBy("Age", false)
	options.AddFilterNumber("ReferenceNull", 0)
	retIds, total = RedisSearchIDs[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, 6)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	iterator, total := RedisSearch[redisSearchEntity](orm, "*", options)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, iterator.Len())
	k = 0
	i := 0
	for iterator.Next() {
		row := iterator.Entity()
		assert.Equal(t, ids[i], retIds[k])
		assert.Equal(t, fmt.Sprintf("name %d", ids[i]), row.Name)
		k++
		i++
		if i == 3 {
			i = 7
		}
	}

	options = &RedisSearchOptions{}
	options.AddFilterNumber("Reference", 8)
	e, found := RedisSearchOne[redisSearchEntity](orm, "*", options)
	assert.True(t, found)
	assert.NotNil(t, e)
	assert.Equal(t, "name 8", e.Name)
}
