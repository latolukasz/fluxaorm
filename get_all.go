package fluxaorm

import (
	"fmt"
	"reflect"
)

const cacheAllFakeReferenceKey = "all"

var allEntitiesWhere = NewWhere("1")

func GetAll[E any](ctx Context) EntityIterator[E] {
	var e E
	schema := ctx.(*ormImplementation).engine.registry.entitySchemas[reflect.TypeOf(e)]
	if schema == nil {
		panic(fmt.Errorf("entity '%T' is not registered", e))
	}
	if !schema.cacheAll {
		return Search[E](ctx, allEntitiesWhere, nil)
	}
	return getCachedByReference[E](ctx, nil, cacheAllFakeReferenceKey, 0, schema)
}
