package fluxaorm

import (
	"reflect"
)

type ReferenceInterface interface {
	Schema(ctx Context) EntitySchema
	getType() reflect.Type
}

type referenceDefinition struct {
	Type reflect.Type
}

type Reference[E any] uint64

func (r Reference[E]) Schema(ctx Context) EntitySchema {
	return ctx.Engine().Registry().EntitySchema(r.getType())
}

func (r Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}
