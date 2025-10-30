package fluxaorm

import (
	"database/sql"
	"reflect"
	"strconv"
)

func SearchWithCount[E any](ctx Context, where Where, pager *Pager) (results EntityIterator[E], totalRows int) {
	return search[E](ctx, where, pager, true)
}

func Search[E any](ctx Context, where Where, pager *Pager) EntityIterator[E] {
	results, _ := search[E](ctx, where, pager, false)
	return results
}

func SearchIDsWithCount[E any](ctx Context, where Where, pager *Pager) (results []uint64, totalRows int) {
	return searchIDs(ctx, GetEntitySchema[E](ctx), where, pager, true)
}

func SearchIDs[E any](ctx Context, where Where, pager *Pager) []uint64 {
	ids, _ := searchIDs(ctx, GetEntitySchema[E](ctx), where, pager, false)
	return ids
}

func SearchOne[E any](ctx Context, where Where) (entity *E, found bool) {
	return searchOne[E](ctx, where)
}

func prepareScan(schema *entitySchema) (pointers []any) {
	count := len(schema.GetColumns())
	pointers = make([]any, count)
	prepareScanForFields(schema.fields, 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []any) int {
	for range fields.uIntegers {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.uIntegersArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := uint64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.references {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.referencesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.structJSONs {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.structJSONsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.integers {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.integersArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := int64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.booleans {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.booleansArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := uint64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.floats {
		v := float64(0)
		pointers[start] = &v
		start++
	}
	for _, i := range fields.floatsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := float64(0)
			pointers[start] = &v
			start++
		}
	}
	for range fields.times {
		v := ""
		pointers[start] = &v
		start++
	}
	for _, i := range fields.timesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := ""
			pointers[start] = &v
			start++
		}
	}
	for range fields.dates {
		v := ""
		pointers[start] = &v
		start++
	}
	for _, i := range fields.datesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := ""
			pointers[start] = &v
			start++
		}
	}
	for range fields.strings {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.stringsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.uIntegersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.uIntegersNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.integersNullable {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.integersNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullInt64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.stringsEnums {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.stringsEnumsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.bytes {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.bytesArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.sliceStringsSets {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.sliceStringsSetsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.booleansNullable {
		v := sql.NullBool{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.booleansNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullBool{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.floatsNullable {
		v := sql.NullFloat64{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.floatsNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullFloat64{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.timesNullable {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.timesNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for range fields.datesNullable {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, i := range fields.datesNullableArray {
		for j := 0; j < fields.arrays[i]; j++ {
			v := sql.NullString{}
			pointers[start] = &v
			start++
		}
	}
	for _, subFields := range fields.structsFields {
		start = prepareScanForFields(subFields, start, pointers)
	}
	for k, i := range fields.structsArray {
		for j := 0; j < fields.arrays[i]; j++ {
			start = prepareScanForFields(fields.structsFieldsArray[k], start, pointers)
		}
	}
	return start
}

func searchRow[E any](ctx Context, where Where) (entity *E, found bool) {
	schema := getEntitySchema[E](ctx)
	pool := schema.GetDB()
	whereQuery := where.String()

	if schema.hasLocalCache {
		query := "SELECT ID FROM `" + schema.GetTableName() + "` WHERE "
		if schema.hasFakeDelete && !where.(*BaseWhere).withDeletes {
			query += "`FakeDelete` = 0"
			if whereQuery != "" {
				query += " AND "
			}
		}
		query += whereQuery + " LIMIT 1"
		var id uint64
		if pool.QueryRow(ctx, NewWhere(query, where.GetParameters()...), &id) {
			return GetByID[E](ctx, id)
		}
		return nil, false
	}

	/* #nosec */
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE "
	if schema.hasFakeDelete && !where.(*BaseWhere).withDeletes {
		query += "`FakeDelete` = 0"
		if whereQuery != "" {
			query += " AND "
		}
	}
	query += whereQuery + " LIMIT 1"
	pointers := prepareScan(schema)
	found = pool.QueryRow(ctx, NewWhere(query, where.GetParameters()...), pointers...)
	if !found {
		return nil, false
	}
	value := reflect.New(schema.t)
	entity = value.Interface().(*E)
	deserializeFromDB(schema.fields, value.Elem(), pointers)
	return entity, true
}

func search[E any](ctx Context, where Where, pager *Pager, withCount bool) (results EntityIterator[E], totalRows int) {
	schema := getEntitySchema[E](ctx)
	if schema.hasLocalCache {
		ids, total := SearchIDsWithCount[E](ctx, where, pager)
		if total == 0 {
			return &emptyResultsIterator[E]{}, 0
		}
		return &localCacheIDsIterator[E]{orm: ctx.(*ormImplementation), schema: schema, ids: ids, index: -1}, total
	}
	entities := make([]*E, 0)
	whereQuery := where.String()
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.GetTableName() + "` WHERE "
	if schema.hasFakeDelete && !where.(*BaseWhere).withDeletes {
		query += "`FakeDelete` = 0"
		if whereQuery != "" {
			query += " AND "
		}
	}
	query += whereQuery
	if pager != nil {
		query += " " + pager.String()
	}
	pool := schema.GetDB()

	if withCount && pager != nil && pager.PageSize == 0 {
		totalRows = getTotalRows(ctx, withCount, pager, where, schema, 0)
		return &emptyResultsIterator[E]{}, totalRows
	}
	queryResults, def := pool.Query(ctx, query, where.GetParameters()...)
	defer def()

	i := 0
	for queryResults.Next() {
		pointers := prepareScan(schema)
		queryResults.Scan(pointers...)
		value := reflect.New(schema.t)
		deserializeFromDB(schema.fields, value.Elem(), pointers)
		e := value.Interface().(*E)
		ctx.cacheEntity(schema, *pointers[0].(*uint64), e)
		entities = append(entities, e)
		i++
	}
	def()
	totalRows = i
	if pager != nil {
		totalRows = getTotalRows(ctx, withCount, pager, where, schema, i)
	}
	resultsIterator := &entityIterator[E]{index: -1, schema: schema, orm: ctx.(*ormImplementation)}
	resultsIterator.rows = entities
	return resultsIterator, totalRows
}

func searchOne[E any](ctx Context, where Where) (*E, bool) {
	return searchRow[E](ctx, where)
}

func searchIDs(ctx Context, schema EntitySchema, where Where, pager *Pager, withCount bool) (ids []uint64, total int) {
	whereQuery := where.String()
	/* #nosec */
	query := "SELECT `ID` FROM `" + schema.GetTableName() + "` WHERE "
	if schema.(*entitySchema).hasFakeDelete && !where.(*BaseWhere).withDeletes {
		query += "`FakeDelete` = 0"
		if whereQuery != "" {
			query += " AND "
		}
	}
	query += whereQuery
	if pager != nil {
		query += " " + pager.String()
	}
	pool := schema.GetDB()
	results, def := pool.Query(ctx, query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := len(result)
	if pager != nil {
		totalRows = getTotalRows(ctx, withCount, pager, where, schema, len(result))
	}
	return result, totalRows
}

func getTotalRows(ctx Context, withCount bool, pager *Pager, where Where, schema EntitySchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.GetTableName() + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetDB()
			pool.QueryRow(ctx, NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}
