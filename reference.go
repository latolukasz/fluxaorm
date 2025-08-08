package fluxaorm

import (
	"reflect"
)

type IDGetter interface {
	GetID() uint64
}

type ReferenceInterface interface {
	IDGetter
	Schema(ctx Context) EntitySchema
	getType() reflect.Type
}

type referenceDefinition struct {
	Cached bool
	Type   reflect.Type
}

type Reference[E any] uint64

func (r Reference[E]) GetEntity(ctx Context) *E {
	if r != 0 {
		e, found := GetByID[E](ctx, uint64(r))
		if !found {
			return nil
		}
		return e
	}
	return nil
}

func (r Reference[E]) Schema(ctx Context) EntitySchema {
	return ctx.Engine().Registry().EntitySchema(r.getType())
}

func (r Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}

func (r Reference[E]) GetID() uint64 {
	return uint64(r)
}
