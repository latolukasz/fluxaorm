package orm

import (
	"reflect"
)

func GetEntityFieldDefinition[E any](ctx Context, field string) (t reflect.Type, tags map[string]string) {
	schema := getEntitySchema[E](ctx)
	if schema == nil {
		return nil, nil
	}
	d, has := schema.fieldDefinitions[field]
	if !has {
		return nil, nil
	}
	return d.Field.Type, d.Tags
}

func GetEntityField(ctx Context, entity any, field string) any {
	return getEntityField(ctx, entity, field)
}

func GetEntityFields(ctx Context, entity any, field ...string) map[string]any {
	schema := getEntitySchemaFromSource(ctx, entity)
	if schema == nil {
		return nil
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
	return result
}

func getEntityField(ctx Context, entity any, field string) any {
	schema := getEntitySchemaFromSource(ctx, entity)
	getter, has := schema.fieldGetters[field]
	if !has {
		return nil
	}
	reflectValue := reflect.ValueOf(entity)
	elem := reflectValue.Elem()
	return getter(elem)
}
