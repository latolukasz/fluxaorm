package test_generate

import (
	"strconv"
	"testing"

	"github.com/latolukasz/fluxaorm/v2"
	"github.com/latolukasz/fluxaorm/v2/test_generate/entities"
	"github.com/stretchr/testify/assert"
)

// cachedUniqueKey rebuilds the key a cached unique index lookup would use, so a test can point it
// at a row of its choosing — which is what an attacker does by finding a hash collision.
func cachedUniqueKey(prefix, indexName string, columns []string, values ...any) string {
	return prefix + fluxaorm.UniqueIndexKeySegment(indexName, columns) + fluxaorm.UniqueIndexKeyHash(values...)
}

// The key is a 32-bit hash of the queried value, so two values can share one entry. Returning the
// cached row without checking it is how a forget-password flow mails one user's reset token to
// another. A hit that does not match must be treated as a miss.
func TestCachedUniqueIndexRejectsAColludingKey(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedUnique{})

	victim := entities.GenerateEntityCachedUniqueProvider.New(ctx)
	victim.SetName("Victim")
	victim.SetAge(30)
	victim.SetEmail("victim@example.com")
	assert.NoError(t, ctx.Save(victim))

	attacker := entities.GenerateEntityCachedUniqueProvider.New(ctx)
	attacker.SetName("Attacker")
	attacker.SetAge(31)
	attacker.SetEmail("attacker@example.com")
	assert.NoError(t, ctx.Save(attacker))

	// Stand in for a collision: the attacker's key resolves to the victim's ID.
	key := cachedUniqueKey(entities.GenerateEntityCachedUniqueProvider.RedisCachePrefix(), "Email", []string{"Email"}, "attacker@example.com")
	assert.NoError(t, ctx.Engine().Redis(fluxaorm.DefaultPoolCode).Set(ctx, key, strconv.FormatUint(victim.GetID(), 10), fluxaorm.EntityCacheTTL))

	reader := ctx.Clone()
	reader.DisableContextCache()

	found, has, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(reader, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Email.Is("attacker@example.com"),
	))
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, attacker.GetID(), found.GetID(), "a colliding key must not return the other row")
	assert.Equal(t, "attacker@example.com", found.GetEmail())
}

// Verifying only the first column of a composite index would leave the class open — about twenty
// of the live cached indexes are composite.
func TestCachedUniqueIndexVerifiesEveryColumnOfACompositeIndex(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedUnique{})

	alice := entities.GenerateEntityCachedUniqueProvider.New(ctx)
	alice.SetName("Alice")
	alice.SetAge(30)
	alice.SetEmail("alice@example.com")
	assert.NoError(t, ctx.Save(alice))

	// Same first column, different second — a first-column-only check would accept this.
	sameName := entities.GenerateEntityCachedUniqueProvider.New(ctx)
	sameName.SetName("Alice")
	sameName.SetAge(40)
	sameName.SetEmail("alice40@example.com")
	assert.NoError(t, ctx.Save(sameName))

	key := cachedUniqueKey(entities.GenerateEntityCachedUniqueProvider.RedisCachePrefix(), "Name_Age", []string{"Name", "Age"}, "Alice", 40)
	assert.NoError(t, ctx.Engine().Redis(fluxaorm.DefaultPoolCode).Set(ctx, key, strconv.FormatUint(alice.GetID(), 10), fluxaorm.EntityCacheTTL))

	reader := ctx.Clone()
	reader.DisableContextCache()

	found, has, err := entities.GenerateEntityCachedUniqueProvider.SearchOne(reader, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueProvider.Fields.Name.Is("Alice"),
		entities.GenerateEntityCachedUniqueProvider.Fields.Age.Eq(uint64(40)),
	))
	assert.NoError(t, err)
	assert.True(t, has)
	assert.Equal(t, sameName.GetID(), found.GetID(), "the second column of the index must be verified too")
	assert.EqualValues(t, 40, found.GetAge())
}

// The SQL branch filters `FakeDelete` = 0 and the cache branch did not, so a stale key could
// resurrect a soft-deleted row.
func TestCachedUniqueIndexIgnoresAFakeDeletedHit(t *testing.T) {
	ctx := fluxaorm.PrepareTables(t, fluxaorm.NewRegistry(), generateEntityCachedUniqueFakeDelete{})

	e := entities.GenerateEntityCachedUniqueFakeDeleteProvider.New(ctx)
	e.SetName("Gone")
	assert.NoError(t, ctx.Save(e))
	assert.NoError(t, ctx.Delete(e))

	key := cachedUniqueKey(entities.GenerateEntityCachedUniqueFakeDeleteProvider.RedisCachePrefix(), "Name", []string{"Name"}, "Gone")
	assert.NoError(t, ctx.Engine().Redis(fluxaorm.DefaultPoolCode).Set(ctx, key, strconv.FormatUint(e.GetID(), 10), fluxaorm.EntityCacheTTL))

	reader := ctx.Clone()
	reader.DisableContextCache()

	_, has, err := entities.GenerateEntityCachedUniqueFakeDeleteProvider.SearchOne(reader, fluxaorm.NewQuery().Filter(
		entities.GenerateEntityCachedUniqueFakeDeleteProvider.Fields.Name.Is("Gone"),
	))
	assert.NoError(t, err)
	assert.False(t, has, "a fake-deleted row must not be served from the index cache")
}
