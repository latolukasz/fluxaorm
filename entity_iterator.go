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
	ID() uint64
	setIndex(index int)
}

type EntityIterator[E any] interface {
	Next() bool
	ID() uint64
	Index() int
	Len() int
	Entity() (*E, error)
	All() ([]*E, error)
	AllIDs() ([]uint64, error)
	Reset()
	LoadReference(columns ...string) error
	setIndex(index int)
}

type EntityAnonymousIterator interface {
	Next() bool
	ID() uint64
	Index() int
	setIndex(index int)
	Len() int
	All() ([]any, error)
	AllIDs() ([]uint64, error)
	Entity() (any, error)
	Reset()
	LoadReference(columns ...string) error
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

func (lca *localCacheAnonymousIDsIterator) All() ([]any, error) {
	if lca.hasRows {
		return lca.rows, nil
	}
	lca.rows = make([]any, len(lca.ids))
	i := 0
	for lca.Next() {
		e, err := lca.Entity()
		if err != nil {
			return nil, err
		}
		lca.rows[i] = e
		i++
	}
	lca.Reset()
	return lca.rows, nil
}

func (lca *localCacheAnonymousIDsIterator) AllIDs() ([]uint64, error) {
	return lca.ids, nil
}

func (lca *localCacheAnonymousIDsIterator) id() uint64 {
	return lca.ids[lca.index]
}

func (lca *localCacheAnonymousIDsIterator) LoadReference(columns ...string) error {
	return loadReference(lca, lca.orm, lca.schema, columns...)
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

func (lc *localCacheIDsIterator[E]) All() ([]*E, error) {
	if lc.hasRows {
		return lc.rows, nil
	}
	lc.rows = make([]*E, lc.Len())
	i := 0
	for lc.Next() {
		e, err := lc.Entity()
		if err != nil {
			return nil, err
		}
		lc.rows[i] = e
		i++
	}
	lc.Reset()
	lc.hasRows = true
	return lc.rows, nil
}

func (lc *localCacheIDsIterator[E]) AllIDs() ([]uint64, error) {
	return lc.ids, nil
}

func (lc *localCacheIDsIterator[E]) Entity() (*E, error) {
	if lc.index == -1 {
		return nil, nil
	}
	if lc.hasRows {
		return lc.rows[lc.index], nil
	}
	if lc.index == 0 {
		value, hit := lc.schema.localCache.getEntity(lc.orm, lc.ids[0])
		if hit {
			if value == nil {
				return nil, nil
			}
			return value.(*E), nil
		}
	}
	value, found, err := getByID(lc.orm, lc.ids[lc.index], lc.schema)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return value.(*E), nil
}

func (lca *localCacheAnonymousIDsIterator) Entity() (any, error) {
	if lca.index == -1 {
		return nil, nil
	}
	if lca.hasRows {
		return lca.rows[lca.index], nil
	}
	if lca.index == 0 {
		value, hit := lca.schema.localCache.getEntity(lca.orm, lca.ids[0])
		if hit {
			if value == nil {
				return nil, nil
			}
			return value, nil
		}
	}
	value, found, err := getByID(lca.orm, lca.ids[lca.index], lca.schema)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return value, nil
}

func (lc *localCacheIDsIterator[E]) LoadReference(columns ...string) error {
	return loadReference(lc, lc.orm, lc.schema, columns...)
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

func (el *emptyResultsIterator[E]) Entity() (*E, error) {
	return nil, nil
}

func (el *emptyResultsIterator[E]) Reset() {}

func (el *emptyResultsIterator[E]) All() ([]*E, error) {
	return nil, nil
}

func (el *emptyResultsIterator[E]) AllIDs() ([]uint64, error) {
	return []uint64{}, nil
}

func (el *emptyResultsIterator[E]) LoadReference(_ ...string) error {
	return nil
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
	return ei.ID()
}

func (eia *entityAnonymousIteratorAdvanced) Next() bool {
	if eia.index+1 >= len(eia.rows) {
		eia.Reset()
		return false
	}
	eia.index++
	return true
}

func (eia *entityAnonymousIteratorAdvanced) All() ([]any, error) {
	return eia.rows, nil
}

func (eia *entityAnonymousIteratorAdvanced) AllIDs() ([]uint64, error) {
	return eia.ids, nil
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

func (ei *entityIterator[E]) Entity() (*E, error) {
	if ei.index == -1 {
		return nil, nil
	}
	return ei.rows[ei.index], nil
}

func (eia *entityAnonymousIteratorAdvanced) Entity() (any, error) {
	if eia.index == -1 {
		return nil, nil
	}
	return eia.rows[eia.index], nil
}

func (ei *entityIterator[E]) Reset() {
	ei.index = -1
}

func (eia *entityAnonymousIteratorAdvanced) Reset() {
	eia.index = -1
}

func (ei *entityIterator[E]) All() ([]*E, error) {
	return ei.rows, nil
}

func (ei *entityIterator[E]) AllIDs() ([]uint64, error) {
	if ei.ids == nil {
		ei.ids = make([]uint64, len(ei.rows))
		for i, value := range ei.rows {
			ei.ids[i] = reflect.ValueOf(value).Elem().FieldByName("ID").Uint()
		}
		slices.Sort(ei.ids)
	}
	return ei.ids, nil
}

func (ei *entityIterator[E]) LoadReference(columns ...string) error {
	return loadReference(ei, ei.orm, ei.schema, columns...)
}

func loadReference(iterator iteratorBase, orm *ormImplementation, schema *entitySchema, columns ...string) error {
	if iterator.Len() <= 1 {
		return nil
	}
	var ids []uint64
	for _, row := range columns {
		fields := strings.Split(row, "/")
		reference, has := schema.references[fields[0]]
		if !has {
			return fmt.Errorf("invalid reference name %s", row)
		}
		index := iterator.Index()
		iterator.setIndex(-1)
		for iterator.Next() {
			id := iterator.ID()
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
			return nil
		}
		s := orm.Engine().Registry().EntitySchema(reference.Type)
		if s == nil {
			return fmt.Errorf("reference not registered %s", reference.Type)
		}
		refSchema := s.(*entitySchema)
		var subRefs string
		if len(fields) > 1 {
			subRefs = strings.Join(fields[1:], "/")
		}
		err := warmup(orm, refSchema, ids, subRefs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (eia *entityAnonymousIteratorAdvanced) LoadReference(columns ...string) error {
	return loadReference(eia, eia.orm, eia.schema, columns...)
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

func (ea *entityAnonymousIterator) All() ([]any, error) {
	rows := make([]any, ea.rows.Len())
	i := 0
	for ea.Next() {
		e, err := ea.Entity()
		if err != nil {
			return nil, err
		}
		rows[i] = e
		i++
	}
	ea.Reset()
	return rows, nil
}

func (ea *entityAnonymousIterator) AllIDs() ([]uint64, error) {
	ids := make([]uint64, ea.rows.Len())
	for i := 0; i < ea.rows.Len(); i++ {
		ids[i] = ea.rows.Index(i).Elem().FieldByName("ID").Uint()
	}
	return ids, nil
}

func (ea *entityAnonymousIterator) LoadReference(columns ...string) error {
	return loadReference(ea, ea.orm, ea.schema, columns...)
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

func (ea *entityAnonymousIterator) Entity() (any, error) {
	if ea.index == -1 {
		return nil, nil
	}
	return ea.rows.Index(ea.index).Interface(), nil
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

func (el *emptyResultsAnonymousIterator) All() ([]any, error) {
	return []any{}, nil
}

func (el *emptyResultsAnonymousIterator) AllIDs() ([]uint64, error) {
	return []uint64{}, nil
}

func (el *emptyResultsAnonymousIterator) setIndex(_ int) {}

func (el *emptyResultsAnonymousIterator) Len() int {
	return 0
}

func (el *emptyResultsAnonymousIterator) Entity() (any, error) {
	return nil, nil
}

func (el *emptyResultsAnonymousIterator) Reset() {}

func (el *emptyResultsAnonymousIterator) LoadReference(_ ...string) error {
	return nil
}

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

func (lc *localCacheIDsAnonymousIterator) All() ([]any, error) {
	rows := make([]any, len(lc.ids))
	i := 0
	for lc.Next() {
		e, err := lc.Entity()
		if err != nil {
			return nil, err
		}
		rows[i] = e
		i++
	}
	lc.Reset()
	return rows, nil
}

func (lc *localCacheIDsAnonymousIterator) AllIDs() ([]uint64, error) {
	return lc.ids, nil
}

func (lc *localCacheIDsAnonymousIterator) id() uint64 {
	return lc.ids[lc.index]

}

func (lc *localCacheIDsAnonymousIterator) LoadReference(columns ...string) error {
	return loadReference(lc, lc.c, lc.schema, columns...)
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

func (lc *localCacheIDsAnonymousIterator) Entity() (any, error) {
	if lc.index == -1 {
		return nil, nil
	}
	value, found, err := getByID(lc.c, lc.ids[lc.index], lc.schema)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return value, nil
}
