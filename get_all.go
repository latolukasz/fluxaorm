package fluxaorm

const cacheAllFakeReferenceKey = "all"

var allEntitiesWhere = NewWhere("1")

func GetAll[E any](ctx Context) EntityIterator[E] {
	var e E
	schema, err := getEntitySchemaFromSource(ctx, e)
	checkError(err)
	if !schema.cacheAll {
		return Search[E](ctx, allEntitiesWhere, nil)
	}
	iterator, _ := getCachedByReference[E](ctx, nil, cacheAllFakeReferenceKey, 0, schema, false)
	return iterator
}
