package fluxaorm

import (
	"reflect"
)

type EnumValues interface {
	EnumValues() any
}

type enumDefinition struct {
	fields       []string
	mapping      map[string]int
	required     bool
	defaultValue string
	name         string
	t            reflect.Type
}

func (d *enumDefinition) GetFields() []string {
	return d.fields
}

func (d *enumDefinition) Has(value string) bool {
	_, has := d.mapping[value]
	return has
}

func (d *enumDefinition) Index(value string) int {
	return d.mapping[value]
}

func initEnumDefinition(name string, def any, required bool) *enumDefinition {
	enum := &enumDefinition{required: required}
	enum.mapping = make(map[string]int)
	enum.name = name
	enum.fields = make([]string, 0)
	asSlice, isSlice := def.([]string)
	if isSlice {
		for i, eName := range asSlice {
			enum.fields = append(enum.fields, eName)
			enum.mapping[eName] = i + 1
		}
		enum.defaultValue = enum.fields[0]
		return enum
	}
	e := reflect.ValueOf(def)
	enum.t = e.Type()
	for i := 0; i < e.Type().NumField(); i++ {
		eName := e.Field(i).String()
		enum.fields = append(enum.fields, eName)
		enum.mapping[eName] = i + 1
	}
	enum.defaultValue = enum.fields[0]
	return enum
}
