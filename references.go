package fluxaorm

import (
	"reflect"

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
