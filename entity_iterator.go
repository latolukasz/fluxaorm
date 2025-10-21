package fluxaorm

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type iteratorBase interface {
	Len() int
	Index() int
	Next() bool
	id() uint64
	setIndex(index int)
}

type EntityIterator[E any] interface {
	Next() bool
	ID() uint64
	Index() int
	Len() int
	Entity() *E
	All() []*E
	AllIDs() []uint64
	Reset()
	LoadReference(columns ...string)
	setIndex(index int)
}

type EntityAnonymousIterator interface {
	Next() bool
	ID() uint64
	Index() int
	setIndex(index int)
	Len() int
	Entity() any
	Reset()
	LoadReference(columns ...string)
}

type localCacheIDsIterator[E any] struct {
	orm     *ormImplementation
	ids     []uint64
	index   int
	schema  *entitySchema
	hasRows bool
	rows    []*E
}

type localCacheAnonymousIDsIterator struct {
	orm     *ormImplementation
	ids     []uint64
	index   int
	schema  *entitySchema
	hasRows bool
	rows    []any
}

func (lc *localCacheIDsIterator[E]) id() uint64 {
	return lc.ids[lc.index]
}

func (lc *localCacheIDsIterator[E]) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		lc.Reset()
		return false
	}
	lc.index++
	return true
}

func (lca *localCacheAnonymousIDsIterator) Next() bool {
	if lca.index+1 >= len(lca.ids) {
		lca.Reset()
		return false
	}
	lca.index++
	return true
}

func (lca *localCacheAnonymousIDsIterator) id() uint64 {
	return lca.ids[lca.index]
}

func (lca *localCacheAnonymousIDsIterator) LoadReference(columns ...string) {
	loadReference(lca, lca.orm, lca.schema, columns...)
}

func (lc *localCacheIDsIterator[E]) Index() int {
	return lc.index
}

func (lca *localCacheAnonymousIDsIterator) Index() int {
	return lca.index
}

func (lc *localCacheIDsIterator[E]) setIndex(index int) {
	lc.index = index
}

func (lca *localCacheAnonymousIDsIterator) setIndex(index int) {
	lca.index = index
}

func (lc *localCacheIDsIterator[E]) ID() uint64 {
	if lc.index == -1 {
		return 0
	}
	return lc.ids[lc.index]
}

func (lca *localCacheAnonymousIDsIterator) ID() uint64 {
	if lca.index == -1 {
		return 0
	}
	return lca.ids[lca.index]
}

func (lc *localCacheIDsIterator[E]) Len() int {
	return len(lc.ids)
}

func (lca *localCacheAnonymousIDsIterator) Len() int {
	return len(lca.ids)
}

func (lc *localCacheIDsIterator[E]) Reset() {
	lc.index = -1
}

func (lca *localCacheAnonymousIDsIterator) Reset() {
	lca.index = -1
}

func (lc *localCacheIDsIterator[E]) All() []*E {
	if lc.hasRows {
		return lc.rows
	}
	lc.rows = make([]*E, lc.Len())
	i := 0
	for lc.Next() {
		lc.rows[i] = lc.Entity()
		i++
	}
	lc.Reset()
	lc.hasRows = true
	return lc.rows
}

func (lc *localCacheIDsIterator[E]) AllIDs() []uint64 {
	return lc.ids
}

func (lc *localCacheIDsIterator[E]) Entity() *E {
	if lc.index == -1 {
		return nil
	}
	if lc.hasRows {
		return lc.rows[lc.index]
	}
	if lc.index == 0 {
		value, hit := lc.schema.localCache.getEntity(lc.orm, lc.ids[0])
		if hit {
			if value == nil {
				return nil
			}
			return value.(*E)
		}
	}
	value, found := getByID(lc.orm, lc.ids[lc.index], lc.schema)
	if !found {
		return nil
	}
	return value.(*E)
}

func (lca *localCacheAnonymousIDsIterator) Entity() any {
	if lca.index == -1 {
		return nil
	}
	if lca.hasRows {
		return lca.rows[lca.index]
	}
	if lca.index == 0 {
		value, hit := lca.schema.localCache.getEntity(lca.orm, lca.ids[0])
		if hit {
			if value == nil {
				return nil
			}
			return value
		}
		lca.warmup()
	}
	value, found := getByID(lca.orm, lca.ids[lca.index], lca.schema)
	if !found {
		return nil
	}
	return value
}

func (lc *localCacheIDsIterator[E]) LoadReference(columns ...string) {
	loadReference(lc, lc.orm, lc.schema, columns...)
}

func (lca *localCacheAnonymousIDsIterator) warmup() {
	if len(lca.ids)-lca.index <= 2 {
		return
	}
	warmup(lca.orm, lca.schema, lca.ids, "")
}

type emptyResultsIterator[E any] struct{}

func (el *emptyResultsIterator[E]) Next() bool {
	return false
}

func (el *emptyResultsIterator[E]) Index() int {
	return -1
}

func (el *emptyResultsIterator[E]) setIndex(_ int) {}

func (el *emptyResultsIterator[E]) ID() uint64 {
	return 0
}

func (el *emptyResultsIterator[E]) Len() int {
	return 0
}

func (el *emptyResultsIterator[E]) Entity() *E {
	return nil
}

func (el *emptyResultsIterator[E]) Reset() {}

func (el *emptyResultsIterator[E]) All() []*E {
	return nil
}

func (el *emptyResultsIterator[E]) AllIDs() []uint64 {
	return []uint64{}
}

func (el *emptyResultsIterator[E]) LoadReference(_ ...string) {

}

type entityIterator[E any] struct {
	index  int
	ids    []uint64
	rows   []*E
	schema *entitySchema
	orm    *ormImplementation
}

type entityAnonymousIteratorAdvanced struct {
	index  int
	ids    []uint64
	rows   []any
	schema *entitySchema
	orm    *ormImplementation
}

func (ei *entityIterator[E]) Next() bool {
	if ei.index+1 >= len(ei.rows) {
		ei.Reset()
		return false
	}
	ei.index++
	return true
}

func (ei *entityIterator[E]) id() uint64 {
	return ei.ids[ei.index]
}

func (eia *entityAnonymousIteratorAdvanced) Next() bool {
	if eia.index+1 >= len(eia.rows) {
		eia.Reset()
		return false
	}
	eia.index++
	return true
}

func (eia *entityAnonymousIteratorAdvanced) id() uint64 {
	return eia.ids[eia.index]
}

func (ei *entityIterator[E]) ID() uint64 {
	if ei.index == -1 {
		return 0
	}
	return reflect.ValueOf(ei.rows[ei.index]).Elem().FieldByName("ID").Uint()
}

func (eia *entityAnonymousIteratorAdvanced) ID() uint64 {
	if eia.index == -1 {
		return 0
	}
	return reflect.ValueOf(eia.rows[eia.index]).Elem().FieldByName("ID").Uint()
}

func (ei *entityIterator[E]) Index() int {
	return ei.index
}

func (eia *entityAnonymousIteratorAdvanced) Index() int {
	return eia.index
}

func (ei *entityIterator[E]) setIndex(index int) {
	ei.index = index
}

func (eia *entityAnonymousIteratorAdvanced) setIndex(index int) {
	eia.index = index
}

func (ei *entityIterator[E]) Len() int {
	return len(ei.rows)
}

func (eia *entityAnonymousIteratorAdvanced) Len() int {
	return len(eia.rows)
}

func (ei *entityIterator[E]) Entity() *E {
	if ei.index == -1 {
		return nil
	}
	return ei.rows[ei.index]
}

func (eia *entityAnonymousIteratorAdvanced) Entity() any {
	if eia.index == -1 {
		return nil
	}
	return eia.rows[eia.index]
}

func (ei *entityIterator[E]) Reset() {
	ei.index = -1
}

func (eia *entityAnonymousIteratorAdvanced) Reset() {
	eia.index = -1
}

func (ei *entityIterator[E]) All() []*E {
	return ei.rows
}

func (ei *entityIterator[E]) AllIDs() []uint64 {
	if ei.ids == nil {
		ei.ids = make([]uint64, len(ei.rows))
		for i, value := range ei.rows {
			ei.ids[i] = reflect.ValueOf(value).Elem().FieldByName("ID").Uint()
		}
		slices.Sort(ei.ids)
	}
	return ei.ids
}

func (ei *entityIterator[E]) LoadReference(columns ...string) {
	loadReference(ei, ei.orm, ei.schema, columns...)
}

func loadReference(iterator iteratorBase, orm *ormImplementation, schema *entitySchema, columns ...string) {
	if iterator.Len() <= 1 {
		return
	}
	var ids []uint64
	for _, row := range columns {
		fields := strings.Split(row, "/")
		reference, has := schema.references[fields[0]]
		if !has {
			panic(fmt.Errorf("invalid reference name %s", row))
		}
		index := iterator.Index()
		iterator.setIndex(-1)
		for iterator.Next() {
			id := iterator.id()
			//entity := reflect.ValueOf(iterator.Entity()).Elem()
			//field := entity.FieldByName(fields[0])
			//id := field.Uint()
			if id == 0 {
				continue
			}
			has = false
			for _, before := range ids {
				if before == id {
					has = true
					break
				}
			}
			if !has {
				ids = append(ids, id)
			}
		}
		iterator.setIndex(index)
		if len(ids) <= 1 {
			return
		}
		refSchema := orm.Engine().Registry().EntitySchema(reference.Type).(*entitySchema)
		var subRefs string
		if len(fields) > 1 {
			subRefs = strings.Join(fields[1:], "/")
		}
		warmup(orm, refSchema, ids, subRefs)
	}
}

func (eia *entityAnonymousIteratorAdvanced) LoadReference(columns ...string) {
	loadReference(eia, eia.orm, eia.schema, columns...)
}

type entityAnonymousIterator struct {
	index  int
	rows   reflect.Value
	schema *entitySchema
	orm    *ormImplementation
}

func (ea *entityAnonymousIterator) id() uint64 {
	return ea.rows.Index(ea.index).Elem().FieldByName("ID").Uint()
}

func (ea *entityAnonymousIterator) Next() bool {
	if ea.index+1 >= ea.rows.Len() {
		ea.Reset()
		return false
	}
	ea.index++
	return true
}

func (ea *entityAnonymousIterator) LoadReference(columns ...string) {
	loadReference(ea, ea.orm, ea.schema, columns...)
}

func (ea *entityAnonymousIterator) ID() uint64 {
	if ea.index == -1 {
		return 0
	}
	return ea.rows.Index(ea.index).Elem().FieldByName("ID").Uint()
}

func (ea *entityAnonymousIterator) Index() int {
	return ea.index
}

func (ea *entityAnonymousIterator) setIndex(index int) {
	ea.index = index
}

func (ea *entityAnonymousIterator) Len() int {
	return ea.rows.Len()
}

func (ea *entityAnonymousIterator) Entity() any {
	if ea.index == -1 {
		return nil
	}
	return ea.rows.Index(ea.index).Interface()
}

func (ea *entityAnonymousIterator) Reset() {
	ea.index = -1
}

type emptyResultsAnonymousIterator struct{}

func (el *emptyResultsAnonymousIterator) Next() bool {
	return false
}

func (el *emptyResultsAnonymousIterator) ID() uint64 {
	return 0
}

func (el *emptyResultsAnonymousIterator) Index() int {
	return -1
}

func (el *emptyResultsAnonymousIterator) setIndex(_ int) {}

func (el *emptyResultsAnonymousIterator) Len() int {
	return 0
}

func (el *emptyResultsAnonymousIterator) Entity() any {
	return nil
}

func (el *emptyResultsAnonymousIterator) Reset() {}

func (el *emptyResultsAnonymousIterator) LoadReference(_ ...string) {}

var emptyResultsAnonymousIteratorInstance = &emptyResultsAnonymousIterator{}

type localCacheIDsAnonymousIterator struct {
	c      *ormImplementation
	ids    []uint64
	index  int
	schema *entitySchema
}

func (lc *localCacheIDsAnonymousIterator) Next() bool {
	if lc.index+1 >= len(lc.ids) {
		lc.Reset()
		return false
	}
	lc.index++
	return true
}

func (lc *localCacheIDsAnonymousIterator) id() uint64 {
	return lc.ids[lc.index]

}

func (lc *localCacheIDsAnonymousIterator) LoadReference(columns ...string) {
	loadReference(lc, lc.c, lc.schema, columns...)
}

func (lc *localCacheIDsAnonymousIterator) ID() uint64 {
	if lc.index == -1 {
		return 0
	}
	return lc.ids[lc.index]
}

func (lc *localCacheIDsAnonymousIterator) Index() int {
	return lc.index
}

func (lc *localCacheIDsAnonymousIterator) setIndex(index int) {
	lc.index = index
}

func (lc *localCacheIDsAnonymousIterator) Len() int {
	return len(lc.ids)
}

func (lc *localCacheIDsAnonymousIterator) Reset() {
	lc.index = -1
}

func (lc *localCacheIDsAnonymousIterator) Entity() any {
	if lc.index == -1 {
		return nil
	}
	value, found := getByID(lc.c, lc.ids[lc.index], lc.schema)
	if !found {
		return nil
	}
	return value
}
