package fluxaorm

import (
	"fmt"
	"reflect"
	"strconv"
)

func PushDirty[E any](ctx Context, entities ...E) error {
	values := make([]any, len(entities))
	for i, entity := range entities {
		values[i] = entity
	}
	return ctx.PushDirty(values...)
}

func (orm *ormImplementation) PushDirty(entities ...any) error {
	if len(entities) == 0 {
		return nil
	}
	schema, err := getEntitySchemaFromEntities(orm, entities)
	if err != nil {
		return err
	}
	actionTag, dirtyDefs := dirtyDefsForEdit(schema)
	if len(dirtyDefs) == 0 {
		return nil
	}
	flusher := orm.GetEventBroker().NewFlusher()
	for i, entity := range entities {
		if entity == nil {
			return fmt.Errorf("entities[%d] is nil", i)
		}
		elem := reflect.ValueOf(entity)
		if elem.Kind() == reflect.Ptr {
			if elem.IsNil() {
				return fmt.Errorf("entities[%d] is nil", i)
			}
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Struct {
			return fmt.Errorf("entities[%d] must be a struct or pointer to struct", i)
		}
		if elem.Type() != schema.t {
			return fmt.Errorf("entities must all be the same type")
		}
		bind := Bind{}
		err = fillBindFromOneSource(orm, bind, elem, schema.fields, "")
		if err != nil {
			return err
		}
		id, ok := bind["ID"].(uint64)
		if !ok || id == 0 {
			return fmt.Errorf("entities[%d] has invalid ID", i)
		}
		values, err := createEventSlice(bind, []string{
			"action", actionTag,
			"entity", schema.t.String(),
			"id", strconv.FormatUint(id, 10),
		})
		if err != nil {
			return err
		}
		for _, dirtyDef := range dirtyDefs {
			streamName := "dirty_" + dirtyDef.Stream
			err = flusher.Publish(streamName, nil, values...)
			if err != nil {
				return err
			}
		}
	}
	return flusher.Flush()
}

func dirtyDefsForEdit(schema *entitySchema) (string, []*dirtyDefinition) {
	return "edit", schema.dirtyUpdated
}

func getEntitySchemaFromEntities(ctx Context, entities []any) (*entitySchema, error) {
	value := reflect.ValueOf(entities[0])
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("entities[0] must be a struct or pointer to struct")
	}
	zero := reflect.New(value.Type()).Interface()
	return getEntitySchemaFromSource(ctx, zero)
}
