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

func (r Reference[E]) GetEntity(ctx Context) (*E, error) {
	if r != 0 {
		e, found, err := GetByID[E](ctx, uint64(r))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		return e, nil
	}
	return nil, nil
}

func (r Reference[E]) Schema(ctx Context) EntitySchema {
	s, _ := ctx.Engine().Registry().EntitySchema(r.getType())
	return s
}

func (r Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}

func (r Reference[E]) GetID() uint64 {
	return uint64(r)
}
