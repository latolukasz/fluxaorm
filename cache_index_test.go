package fluxaorm

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/puzpuzpuz/xsync/v2"
	"github.com/stretchr/testify/assert"
)

type cacheIndexEntityA struct {
	ID   uint64
	Name string
}

type cacheIndexEntityB struct {
	ID    uint64
	Other string
}

type cacheIndexEntityC struct {
	ID uint64
}

func validateForCacheIndex(t *testing.T, entities ...any) Engine {
	t.Helper()
	r := NewRegistry()
	r.RegisterMySQL("user:pass@tcp(localhost:3306)/test", DefaultPoolCode, &MySQLOptions{})
	r.RegisterEntity(entities...)
	engine, err := r.ValidateForCodeGen()
	assert.NoError(t, err)
	return engine
}

func entityCacheIndexFor(t *testing.T, engine Engine, entity any) string {
	t.Helper()
	reg := engine.Registry().(*engineRegistryImplementation)
	tp := reflect.TypeOf(entity)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	schema, ok := reg.entitySchemas[tp]
	assert.True(t, ok, "schema not found for %s", tp.String())
	return schema.index
}

// TestCacheIndexIsEntityTypeName: schema.index is the fully qualified Go type name.
func TestCacheIndexIsEntityTypeName(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{})
	assert.Equal(t, "fluxaorm.cacheIndexEntityA", entityCacheIndexFor(t, engine, cacheIndexEntityA{}))
	assert.Equal(t, "fluxaorm.cacheIndexEntityB", entityCacheIndexFor(t, engine, cacheIndexEntityB{}))
}

// TestCacheIndexStableWhenOtherEntitiesAdded: the bug the refactor fixed —
// adding entities must NOT renumber existing ones.
func TestCacheIndexStableWhenOtherEntitiesAdded(t *testing.T) {
	engineSolo := validateForCacheIndex(t, cacheIndexEntityB{})
	soloIdx := entityCacheIndexFor(t, engineSolo, cacheIndexEntityB{})

	engineFull := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{}, cacheIndexEntityC{})
	fullIdx := entityCacheIndexFor(t, engineFull, cacheIndexEntityB{})

	assert.Equal(t, soloIdx, fullIdx, "cacheIndex for B shifted after A and C were added")
}

// TestCacheIndexStableAcrossRegistrationOrder: registration order doesn't matter.
func TestCacheIndexStableAcrossRegistrationOrder(t *testing.T) {
	forward := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{}, cacheIndexEntityC{})
	reverse := validateForCacheIndex(t, cacheIndexEntityC{}, cacheIndexEntityB{}, cacheIndexEntityA{})

	for _, e := range []any{cacheIndexEntityA{}, cacheIndexEntityB{}, cacheIndexEntityC{}} {
		assert.Equal(t,
			entityCacheIndexFor(t, forward, e),
			entityCacheIndexFor(t, reverse, e),
		)
	}
}

// TestCacheIndexUniquePerEntity: distinct entity types yield distinct cacheIndexes.
func TestCacheIndexUniquePerEntity(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{}, cacheIndexEntityC{})
	seen := map[string]string{}
	for _, e := range []any{cacheIndexEntityA{}, cacheIndexEntityB{}, cacheIndexEntityC{}} {
		idx := entityCacheIndexFor(t, engine, e)
		name := reflect.TypeOf(e).String()
		if existing, dup := seen[idx]; dup {
			t.Fatalf("cacheIndex %q collides between %s and %s", idx, existing, name)
		}
		seen[idx] = name
	}
}

// TestCacheIndexSchemasByIndexLookup: entitySchemasByIndex is keyed by cacheIndex
// and resolves back to the right schema.
func TestCacheIndexSchemasByIndexLookup(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{})
	reg := engine.Registry().(*engineRegistryImplementation)

	for _, schema := range reg.entitySchemas {
		got, ok := reg.entitySchemasByIndex[schema.index]
		assert.True(t, ok, "no schema in entitySchemasByIndex for %q", schema.index)
		assert.Same(t, schema, got)
	}
}

// TestRegisterEntityLoaderUsesStringIndex: handlers keyed by string cacheIndex
// land in the correct engine map.
func TestRegisterEntityLoaderUsesStringIndex(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{})
	idxA := entityCacheIndexFor(t, engine, cacheIndexEntityA{})
	idxB := entityCacheIndexFor(t, engine, cacheIndexEntityB{})

	called := ""
	RegisterEntityLoader(engine, idxA, "poolA", func(_ Context, _ uint64) (Entity, bool, error) {
		called = "A"
		return nil, false, nil
	})
	RegisterEntityLoader(engine, idxB, "poolB", func(_ Context, _ uint64) (Entity, bool, error) {
		called = "B"
		return nil, false, nil
	})

	impl := engine.(*engineImplementation)
	assert.Equal(t, "poolA", impl.entityDBPools[idxA])
	assert.Equal(t, "poolB", impl.entityDBPools[idxB])

	_, _, _ = impl.entityLoaders[idxA](nil, 0)
	assert.Equal(t, "A", called)
	_, _, _ = impl.entityLoaders[idxB](nil, 0)
	assert.Equal(t, "B", called)
}

// TestAsyncEntityEventJSONRoundTrip: the wire format carries the string
// cacheIndex under the cache_index JSON field unchanged across encode/decode.
func TestAsyncEntityEventJSONRoundTrip(t *testing.T) {
	in := AsyncEntityEvent{
		CacheIndex: "app/entities.User",
		EntityID:   42,
		FlushType:  2,
		Changes: map[string]AsyncSQLParam{
			"name": {Type: "s", Val: "alice"},
		},
	}
	raw, err := json.Marshal(in)
	assert.NoError(t, err)
	assert.Contains(t, string(raw), `"cache_index":"app/entities.User"`)

	var out AsyncEntityEvent
	assert.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, in.CacheIndex, out.CacheIndex)
	assert.Equal(t, in.EntityID, out.EntityID)
	assert.Equal(t, in.FlushType, out.FlushType)
}

// TestContextCacheKeysByStringIndex: GetFromContextCache / SetInContextCache
// isolate entries per cacheIndex, so two different entity types with the same
// numeric ID do not collide.
func TestContextCacheKeysByStringIndex(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{})
	ctx := engine.NewContext(context.Background())

	idxA := entityCacheIndexFor(t, engine, cacheIndexEntityA{})
	idxB := entityCacheIndexFor(t, engine, cacheIndexEntityB{})

	eA := &stubEntity{id: 1, tag: "A"}
	eB := &stubEntity{id: 1, tag: "B"}

	ctx.SetInContextCache(idxA, 1, eA)
	ctx.SetInContextCache(idxB, 1, eB)

	gotA := ctx.GetFromContextCache(idxA, 1).(*stubEntity)
	gotB := ctx.GetFromContextCache(idxB, 1).(*stubEntity)

	assert.Equal(t, "A", gotA.tag)
	assert.Equal(t, "B", gotB.tag)
	assert.Nil(t, ctx.GetFromContextCache(idxA, 999))
}

// TestTrackKeysByStringIndex: Track groups entities under their cacheIndex
// and Range yields the string key.
func TestTrackKeysByStringIndex(t *testing.T) {
	engine := validateForCacheIndex(t, cacheIndexEntityA{}, cacheIndexEntityB{})
	ctx := engine.NewContext(context.Background())

	idxA := entityCacheIndexFor(t, engine, cacheIndexEntityA{})
	idxB := entityCacheIndexFor(t, engine, cacheIndexEntityB{})

	ctx.Track(&stubEntity{id: 1, tag: "A"}, idxA)
	ctx.Track(&stubEntity{id: 2, tag: "A"}, idxA)
	ctx.Track(&stubEntity{id: 1, tag: "B"}, idxB)

	orm := ctx.(*ormImplementation)
	gotByIndex := map[string]int{}
	orm.trackedEntities.Range(func(idx string, m *xsync.MapOf[uint64, Entity]) bool {
		gotByIndex[idx] = m.Size()
		return true
	})
	assert.Equal(t, map[string]int{idxA: 2, idxB: 1}, gotByIndex)
}

// stubEntity is a minimal Entity used only by the context-cache and Track tests
// above — it never touches MySQL/Redis so we can drive Set/Get/Track directly.
type stubEntity struct {
	id  uint64
	tag string
}

func (s *stubEntity) GetID() uint64                              { return s.id }
func (s *stubEntity) PrivateFlush() error                        { return nil }
func (s *stubEntity) PrivateFlushed()                            {}
func (s *stubEntity) PrivateFlushEvent() (uint8, map[string]any) { return 0, nil }
func (s *stubEntity) PrivateGetDatabaseBind() map[string]any     { return nil }
