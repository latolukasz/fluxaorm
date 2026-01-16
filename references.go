package fluxaorm

import (
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

type References[E any] struct {
	isSerialized bool
	serialized   string
	unserialized []uint64
}

type referencesDefinition struct {
	Type reflect.Type
}

func (r References[E]) getSerialized() (string, error) {
	if !r.isSerialized {
		asJson, err := jsoniter.ConfigFastest.MarshalToString(r.unserialized)
		if err != nil {
			return "", err
		}
		r.serialized = asJson
		r.isSerialized = true
	}
	return r.serialized, nil
}

func (References[E]) getType() reflect.Type {
	var e E
	return reflect.TypeOf(e)
}

func (r *References[E]) setSerialized(v string) {
	r.serialized = v
}

func (r *References[E]) Len() int {
	if r.unserialized == nil {
		if r.serialized == "" {
			return 0
		}
		r.unserialized = make([]uint64, strings.Count(r.serialized, ","))
		err := jsoniter.ConfigFastest.UnmarshalFromString(r.serialized, &r.unserialized)
		if err != nil {
			return 0
		}
	}
	return len(r.unserialized)
}

func (r *References[E]) GetIDs() []uint64 {
	r.Len()
	return r.unserialized
}

func (r *References[E]) GetEntity(ctx Context, index int) (*E, error) {
	if index < 0 {
		return nil, nil
	}
	if r.Len()-1 < index {
		return nil, nil
	}
	id := r.unserialized[index]
	if id == 0 {
		return nil, nil
	}
	e, _, err := GetByID[E](ctx, id)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (r *References[E]) GetEntities(ctx Context) (EntityIterator[E], error) {
	if r.Len() == 0 {
		return &emptyResultsIterator[E]{}, nil
	}
	return GetByIDs[E](ctx, r.unserialized...)
}

func (r *References[E]) SetIDs(ids []uint64) {
	r.unserialized = ids
	r.serialized = ""
	r.isSerialized = false
}
