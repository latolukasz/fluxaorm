package fluxaorm

import (
	"reflect"

	jsoniter "github.com/json-iterator/go"
)

type structDefinition struct {
	Type reflect.Type
}

type Struct[E any] struct {
	isSerialized bool
	serialized   string
	unserialized *E
}

type structGetter interface {
	getSerialized() (string, error)
	getType() reflect.Type
}

type structSetter interface {
	setSerialized(string)
}

func (r Struct[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}

func (r *Struct[E]) Get() *E {
	if r.unserialized == nil {
		if r.serialized == "" {
			return nil
		}
		r.unserialized = reflect.New(reflect.TypeOf(r.unserialized)).Elem().Interface().(*E)
		err := jsoniter.ConfigFastest.UnmarshalFromString(r.serialized, &r.unserialized)
		if err != nil {
			return nil
		}
	}
	return r.unserialized
}

func (r *Struct[E]) setSerialized(v string) {
	r.serialized = v
	r.isSerialized = true
	r.unserialized = nil
}

func (r Struct[E]) getSerialized() (string, error) {
	// If we have a decoded object in memory, prefer serializing it.
	// This ensures mutations made through Get() are persisted on flush.
	if r.unserialized != nil || !r.isSerialized {
		asJson, err := jsoniter.ConfigFastest.MarshalToString(r.unserialized)
		if err != nil {
			return "", err
		}
		r.serialized = asJson
		r.isSerialized = true
	}
	return r.serialized, nil
}

func (r *Struct[E]) Set(e *E) {
	r.unserialized = e
	r.isSerialized = false
}
