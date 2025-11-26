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
	ID            uint64     `orm:"localCache;redisSearch=default"`
	Age           uint8      `orm:"searchable;sortable"`
	Name          string     `orm:"searchable"`
	NameAsTag     string     `orm:"searchable;rs_tag"`
	Active        bool       `orm:"searchable"`
	ActiveNull    *bool      `orm:"searchable"`
	EnumNotNull   testEnum   `orm:"required;searchable"`
	EnumNull      testEnum   `orm:"searchable"`
	EnumSet       []testEnum `orm:"required;searchable"`
	Sub           redisSearchStructEntity
	Reference     Reference[redisSearchEntityReference] `orm:"searchable;required"`
	ReferenceNull Reference[redisSearchEntityReference] `orm:"searchable"`
	IntArray      [2]int                                `orm:"searchable"`
	Born          time.Time                             `orm:"searchable;sortable"`
	Created       time.Time                             `orm:"time;searchable;sortable"`
	CreatedNull   *time.Time                            `orm:"searchable"`
}

type redisSearchCustom struct {
	ID   uint64 `orm:"virtual;redisCache;redisSearch=default"`
	Age  uint8  `orm:"searchable;sortable"`
	Name string `orm:"searchable"`
}

type redisSearchEntityReference struct {
	ID   uint64 `orm:"localCache"`
	Name string `orm:"required"`
}

func TestRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	reg := NewRegistry()
	orm := PrepareTables(t, reg, entity, redisSearchEntityReference{}, redisSearchCustom{})
	schema, err := GetEntitySchema[redisSearchEntity](orm)
	assert.NoError(t, err)
	r := orm.Engine().Redis(schema.GetRedisSearchPoolCode())

	var ids []uint64
	var idsReferences []uint64
	now := time.Now().UTC()
	for i := 1; i <= 10; i++ {
		reference, err := NewEntity[redisSearchEntityReference](orm)
		assert.NoError(t, err)
		reference.Name = fmt.Sprintf("reference %d", reference.ID)
		entity, err = NewEntity[redisSearchEntity](orm)
		assert.NoError(t, err)
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
	err = orm.Flush()
	assert.NoError(t, err)

	testRedisSearchResults(t, r, orm, schema, ids, now, idsReferences)

	// Reindex
	err = orm.Engine().Redis(DefaultPoolCode).FlushDB(orm)
	assert.NoError(t, err)
	redisSearchAlters, err := GetRedisSearchAlters(orm)
	assert.NoError(t, err)
	assert.Len(t, redisSearchAlters, 2)
	for _, alter := range redisSearchAlters {
		err = alter.Exec(orm)
		assert.NoError(t, err)
	}
	redisSearchAlters, err = GetRedisSearchAlters(orm)
	assert.NoError(t, err)
	assert.Len(t, redisSearchAlters, 0)

	testRedisSearchResults(t, r, orm, schema, ids, now, idsReferences)

	e, _, err := GetByID[redisSearchEntity](orm, ids[0])
	assert.NoError(t, err)
	err = DeleteEntity(orm, e)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())

	res, err := r.FTSearch(orm, schema.GetRedisSearchIndexName(), "*", &redis.FTSearchOptions{NoContent: true})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 9, res.Total)

	query := NewRedisSearchQuery()
	query.AddFilterNumber("Age", 1)
	_, found, err := RedisSearchOne[redisSearchEntity](orm, query)
	assert.NoError(t, err)
	assert.False(t, found)

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	retIds, total, err := RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 9, total)
	assert.Len(t, retIds, 9)
	for i := 1; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[i-1])
	}

	e, _, err = GetByID[redisSearchEntity](orm, ids[1])
	assert.NoError(t, err)
	e, err = EditEntity(orm, e)
	assert.NoError(t, err)
	e.Age = 100
	assert.NoError(t, orm.Flush())

	query = NewRedisSearchQuery()
	query.AddFilterNumber("Age", 100)
	e, found, err = RedisSearchOne[redisSearchEntity](orm, query)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, fmt.Sprintf("name %d", ids[1]), e.Name)

	_, _, err = RedisSearchIDs[redisSearchEntityReference](orm, query, nil)
	assert.EqualError(t, err, "entity redisSearchEntityReference is not searchable by Redis Search")

	custom, err := NewEntity[redisSearchCustom](orm)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), custom.ID)
	custom.ID = 1
	custom.Age = 18
	custom.Name = "Custom 1"
	assert.NoError(t, orm.Flush())

	query = NewRedisSearchQuery()
	query.AddFilterNumber("Age", 18)
	rows, total, err := RedisSearch[redisSearchCustom](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, 1, rows.Len())
	assert.True(t, rows.Next())
	row, err := rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, uint8(18), row.Age)
	assert.Equal(t, "Custom 1", row.Name)

	custom, err = EditEntity(orm, custom)
	assert.NoError(t, err)
	custom.Age = 20
	assert.NoError(t, orm.Flush())
	query = NewRedisSearchQuery()
	query.AddFilterNumber("Age", 20)
	rows, total, err = RedisSearch[redisSearchCustom](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, 1, rows.Len())
	assert.True(t, rows.Next())
	row, err = rows.Entity()
	assert.NoError(t, err)
	assert.Equal(t, uint8(20), row.Age)
	assert.Equal(t, "Custom 1", row.Name)

	err = DeleteEntity(orm, custom)
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())
	rows, total, err = RedisSearch[redisSearchCustom](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Equal(t, 0, rows.Len())
}

func testRedisSearchResults(t *testing.T, r RedisCache, orm Context, schema EntitySchema, ids []uint64, now time.Time, idsReferences []uint64) {
	info, found, err := r.FTInfo(orm, schema.GetRedisSearchIndexName())
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 10, info.NumDocs)
	assert.Equal(t, 0, info.IndexErrors.IndexingFailures)
	for _, field := range info.FieldStatistics {
		assert.Equal(t, 0, field.IndexErrors.IndexingFailures)
	}
	res, err := r.FTSearch(orm, schema.GetRedisSearchIndexName(), "*", &redis.FTSearchOptions{NoContent: true})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, 10, res.Total)
	query := NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	retIds, total, err := RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	for i, id := range ids {
		assert.Equal(t, id, retIds[i])
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	pager := NewPager(1, 5)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, nil, pager)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 5)
	for i, id := range ids[0:5] {
		assert.Equal(t, id, retIds[i])
	}
	pager.IncrementPage()
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, pager)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 5)
	for i, id := range ids[5:] {
		assert.Equal(t, id, retIds[i])
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	for i, id := range ids {
		assert.Equal(t, id, retIds[i])
	}
	query = NewRedisSearchQuery()
	query.AddSortBy("Age", true)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	k := 0
	for i := 9; i > 0; i-- {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	query = NewRedisSearchQuery()
	query.AddFilterNumberGreaterEqual("Age", 8)
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	query = NewRedisSearchQuery()
	query.AddFilterNumberLessEqual("Age", 3)
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddFilterNumberRange("Age", 3, 5)
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.Query = fmt.Sprintf("name %d", ids[7])
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, retIds, 1)
	assert.Equal(t, ids[7], retIds[0])

	query = NewRedisSearchQuery()
	query.AddFilterNumberLessEqual("Age", 3)
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddFilterDateRange("Born", now.AddDate(0, 0, 3), now.AddDate(0, 0, 5))
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddFilterDateRange("Created", now.Add(time.Hour*6*3), now.Add(time.Hour*6*5))
	query.AddSortBy("Age", false)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 2; i < 5; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterTag("EnumNotNull", "b", "c")
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 3; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterTag("EnumSet", "a")
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 7, total)
	assert.Len(t, retIds, 7)
	k = 0
	for i := 0; i < 7; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterTag("EnumSet", "a", "b")
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, total)
	assert.Len(t, retIds, 10)
	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterTag("EnumSet", "a")
	query.AddFilterTag("EnumSet", "b")
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterBoolean("Active", true)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 4, total)
	assert.Len(t, retIds, 4)
	k = 0
	for i := 3; i < 6; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterBoolean("Active", false)
	query.Query = "@Active:{0}"
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, 6)

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.Query = "@NameAsTag:{NULL}"
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 0; i < 3; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}
	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterTag("NameAsTag", "tag2")
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, retIds, 3)
	k = 0
	for i := 7; i < 10; i++ {
		assert.Equal(t, ids[i], retIds[k])
		k++
	}

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.Query = "@EnumNull:{NULL}"
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
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

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.Query = "@ActiveNull:{NULL}"
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
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

	query = NewRedisSearchQuery()
	query.AddSortBy("Age", false)
	query.AddFilterNumber("ReferenceNull", 0)
	retIds, total, err = RedisSearchIDs[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
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

	iterator, total, err := RedisSearch[redisSearchEntity](orm, query, nil)
	assert.NoError(t, err)
	assert.Equal(t, 6, total)
	assert.Len(t, retIds, iterator.Len())
	k = 0
	i := 0
	for iterator.Next() {
		row, err := iterator.Entity()
		assert.NoError(t, err)
		assert.Equal(t, ids[i], retIds[k])
		assert.Equal(t, fmt.Sprintf("name %d", ids[i]), row.Name)
		k++
		i++
		if i == 3 {
			i = 7
		}
	}

	query = NewRedisSearchQuery()
	query.AddFilterNumber("Reference", int64(idsReferences[7]))
	e, found, err := RedisSearchOne[redisSearchEntity](orm, query)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, e)
	assert.Equal(t, idsReferences[7], e.Reference.GetID())
}
