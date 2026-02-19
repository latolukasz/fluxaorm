package fluxaorm

import (
	"reflect"
)

type referenceInterface interface {
	schema(ctx Context) *entitySchema
	getType() reflect.Type
}

type referenceDefinition struct {
	Type reflect.Type
}

type Reference[E any] uint64

func (r Reference[E]) schema(ctx Context) *entitySchema {
	return ctx.Engine().Registry().(*engineRegistryImplementation).entitySchemas[r.getType()]
}

func (r Reference[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}
