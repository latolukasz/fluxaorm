package fluxaorm

import (
	"reflect"
)

func GetEntityFieldDefinition[E any](ctx Context, field string) (t reflect.Type, tags map[string]string, err error) {
	schema := getEntitySchema[E](ctx)
	if schema == nil {
		return nil, nil, nil
	}
	if schema == nil {
		return nil, nil, nil
	}
	d, has := schema.fieldDefinitions[field]
	if !has {
		return nil, nil, nil
	}
	return d.Field.Type, d.Tags, nil
}

func GetEntityField(ctx Context, entity any, field string) (any, error) {
	return getEntityField(ctx, entity, field)
}

func GetEntityFields(ctx Context, entity any, field ...string) (map[string]any, error) {
	schema := getEntitySchemaFromSource(ctx, entity)
	if schema == nil {
		return nil, nil
	}
	reflectValue := reflect.ValueOf(entity)
	elem := reflectValue.Elem()
	result := map[string]any{}
	for _, f := range field {
		getter, has := schema.fieldGetters[f]
		if has {
			result[f] = getter(elem)
		}
	}
	return result, nil
}

func getEntityField(ctx Context, entity any, field string) (any, error) {
	schema := getEntitySchemaFromSource(ctx, entity)
	if schema == nil {
		return nil, nil
	}
	getter, has := schema.fieldGetters[field]
	if !has {
		return nil, nil
	}
	reflectValue := reflect.ValueOf(entity)
	elem := reflectValue.Elem()
	return getter(elem), nil
}
