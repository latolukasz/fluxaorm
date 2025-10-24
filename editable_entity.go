package fluxaorm

import (
	"reflect"
)

type FlushType int

const (
	Insert FlushType = iota
	Update
	Delete
)

type EntityFlush interface {
	ID() uint64
	Schema() *entitySchema
	flushType() FlushType
}

type entityFlushInsert interface {
	EntityFlush
	getBind() (Bind, error)
	getEntity() any
	getValue() reflect.Value
}

type entityFlushDelete interface {
	EntityFlush
	getOldBind() (Bind, error)
	getValue() reflect.Value
}

type entityFlushUpdate interface {
	EntityFlush
	getBind() (new, old, newForced, oldForced Bind, err error)
	getValue() reflect.Value
	getSourceValue() reflect.Value
	getEntity() any
}

type EntityFlushedEvent interface {
	FlushType() FlushType
}

type writableEntity struct {
	ctx    Context
	schema *entitySchema
}

func (w *writableEntity) Schema() *entitySchema {
	return w.schema
}

type insertableEntity struct {
	writableEntity
	entity any
	id     uint64
	value  reflect.Value
}

func (m *insertableEntity) ID() uint64 {
	return m.id
}

func (m *insertableEntity) getEntity() any {
	return m.entity
}

func (m *insertableEntity) flushType() FlushType {
	return Insert
}

func (m *insertableEntity) getValue() reflect.Value {
	return m.value
}

func (e *editableEntity) flushType() FlushType {
	return Update
}

func (e *editableEntity) getValue() reflect.Value {
	return e.value
}

func (e *editableEntity) getEntity() any {
	return e.entity
}

func (e *editableEntity) getSourceValue() reflect.Value {
	return e.sourceValue
}

type removableEntity struct {
	writableEntity
	id     uint64
	value  reflect.Value
	source any
}

func (r *removableEntity) flushType() FlushType {
	return Delete
}

func (r *removableEntity) SourceEntity() any {
	return r.source
}

func (r *removableEntity) getValue() reflect.Value {
	return r.value
}

type editableEntity struct {
	writableEntity
	entity      any
	id          uint64
	value       reflect.Value
	sourceValue reflect.Value
	source      any
}

type editableFields struct {
	writableEntity
	id      uint64
	value   reflect.Value
	newBind Bind
	oldBind Bind
}

func (f *editableFields) ID() uint64 {
	return f.id
}

func (f *editableFields) flushType() FlushType {
	return Update
}

func (f *editableFields) getBind() (new, old, forcedNew, forcedOld Bind, err error) {
	forcedNew = Bind{}
	forcedOld = Bind{}
	uniqueIndexes := f.schema.GetUniqueIndexes()
	if len(uniqueIndexes) > 0 {
		for _, indexColumns := range uniqueIndexes {
			if len(indexColumns) == 1 {
				continue
			}
			indexChanged := false
			for _, column := range indexColumns {
				_, changed := f.newBind[column]
				if changed {
					indexChanged = true
					break
				}
			}
			if !indexChanged {
				continue
			}
			for _, column := range indexColumns {
				_, changed := f.newBind[column]
				getter := f.schema.fieldGetters[column]
				val := getter(f.value.Elem())
				setter := f.schema.fieldBindSetters[column]
				val, err = setter(val)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				if !changed {
					forcedNew[column] = val
				}
				forcedOld[column] = val
			}
		}
	}
	for _, def := range f.schema.cachedIndexes {
		if len(def.Columns) == 1 {
			continue
		}
		indexChanged := false
		for _, indexColumn := range def.Columns {
			_, has := f.newBind[indexColumn]
			if has {
				indexChanged = true
				break
			}
		}
		if !indexChanged {
			continue
		}
		for _, column := range def.Columns {
			_, changed := f.newBind[column]
			getter := f.schema.fieldGetters[column]
			val := getter(f.value.Elem())
			setter := f.schema.fieldBindSetters[column]
			val, err = setter(val)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			if !changed {
				forcedNew[column] = val
			}
			forcedOld[column] = val
		}
	}
	return f.newBind, f.oldBind, forcedNew, forcedOld, nil
}

func (f *editableFields) getEntity() any {
	return nil
}

func (f *editableFields) getSourceValue() reflect.Value {
	return f.value
}

func (f *editableFields) getValue() reflect.Value {
	return f.value
}

func (e *editableEntity) ID() uint64 {
	return e.id
}

func (r *removableEntity) ID() uint64 {
	return r.id
}

func (e *editableEntity) TrackedEntity() any {
	return e.entity
}

func (e *editableEntity) SourceEntity() any {
	return e.source
}

func NewEntity[E any](ctx Context) *E {
	return newEntity(ctx, getEntitySchema[E](ctx)).(*E)
}

func (orm *ormImplementation) NewEntity(entity any) {
	NewEntityFromSource(orm, entity)
}

func NewEntityFromSource(ctx Context, entity any) {
	schema := getEntitySchemaFromSource(ctx, entity)
	insertable := &insertableEntity{}
	insertable.ctx = ctx
	insertable.schema = schema
	value := reflect.ValueOf(entity)
	elem := value.Elem()
	initNewEntity(elem, schema.fields)
	insertable.entity = value.Interface()
	f := elem.Field(0)
	id := f.Uint()
	if id == 0 {
		id = schema.uuid(ctx)
	}
	insertable.id = id
	f.SetUint(id)
	insertable.value = value
	ctx.trackEntity(insertable)
}

func NewEntityWithID[E any, I ID](ctx Context, id I) *E {
	return newEntityInsertable(ctx, getEntitySchema[E](ctx), uint64(id)).entity.(*E)
}

func newEntityInsertable(ctx Context, schema *entitySchema, id uint64) *insertableEntity {
	entity := &insertableEntity{}
	entity.ctx = ctx
	entity.schema = schema
	value := reflect.New(schema.t)
	elem := value.Elem()
	initNewEntity(elem, schema.fields)
	entity.entity = value.Interface()
	if id == 0 {
		id = schema.uuid(ctx)
	}
	entity.id = id
	elem.Field(0).SetUint(id)
	entity.value = value
	ctx.trackEntity(entity)
	return entity
}

func newEntity(ctx Context, schema *entitySchema) any {
	return newEntityInsertable(ctx, schema, 0).entity
}

func DeleteEntity[E any](ctx Context, source *E) {
	toRemove := &removableEntity{}
	toRemove.ctx = ctx
	toRemove.source = source
	toRemove.value = reflect.ValueOf(source).Elem()
	toRemove.id = toRemove.value.Field(0).Uint()
	schema := getEntitySchema[E](ctx)
	toRemove.schema = schema
	ctx.trackEntity(toRemove)
}

func (orm *ormImplementation) DeleteEntity(entity any) {
	toRemove := &removableEntity{}
	toRemove.ctx = orm
	toRemove.source = entity
	toRemove.value = reflect.ValueOf(entity).Elem()
	toRemove.id = toRemove.value.Field(0).Uint()
	schema := getEntitySchemaFromSource(orm, entity)
	toRemove.schema = schema
	orm.trackEntity(toRemove)
}

func EditEntity[E any](ctx Context, source *E) *E {
	writable := copyToEdit(ctx, source)
	writable.id = writable.value.Elem().Field(0).Uint()
	writable.source = source
	ctx.trackEntity(writable)
	return writable.entity.(*E)
}

func (orm *ormImplementation) EditEntity(entity any) any {
	writable := copyToEdit(orm, entity)
	writable.id = writable.value.Elem().Field(0).Uint()
	writable.source = entity
	orm.trackEntity(writable)
	return writable.entity
}

func initNewEntity(elem reflect.Value, fields *tableFields) {
	for k, i := range fields.stringsEnums {
		def := fields.enums[k]
		f := elem.Field(i)
		if def.required && f.String() == "" {
			f.SetString(def.defaultValue)
		}
	}
	for k, i := range fields.sliceStringsSets {
		def := fields.enums[k]
		if def.required {
			f := elem.Field(i)
			if f.Len() == 0 {
				setValues := reflect.MakeSlice(f.Type(), 1, 1)
				setValues.Index(0).SetString(def.defaultValue)
				f.Set(setValues)
			}
		}
	}
}

func IsDirty[E any, I ID](ctx Context, id I) (oldValues, newValues Bind, hasChanges bool) {
	return isDirty(ctx, getEntitySchema[E](ctx), uint64(id))
}

func isDirty(ctx Context, schema *entitySchema, id uint64) (oldValues, newValues Bind, hasChanges bool) {
	tracked := ctx.(*ormImplementation).trackedEntities
	if tracked == nil {
		return nil, nil, false
	}
	if schema == nil {
		return nil, nil, false
	}
	values, has := tracked.Load(schema.index)
	if !has {
		return nil, nil, false
	}
	row, has := values.Load(id)
	if !has {
		return nil, nil, false
	}
	editable, isUpdate := row.(entityFlushUpdate)
	if !isUpdate {
		insertable, isInsert := row.(entityFlushInsert)
		if isInsert {
			bind, _ := insertable.getBind()
			return nil, bind, true
		}
		return nil, nil, false
	}
	oldValues, newValues, _, _, _ = editable.getBind()
	if len(oldValues) == 0 && len(newValues) == 0 {
		return nil, nil, false
	}
	return oldValues, newValues, true
}
