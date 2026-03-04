package fluxaorm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSharedEnumResolution(t *testing.T) {
	// Positive test: Entity A defines values, Entity B references them
	schemaA := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SharedStatus", []string{"active", "banned"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				{name: "SharedStatus", required: false}, // reference
			},
		},
		tags: map[string]map[string]string{
			"Status": {"enum": "true", "enumName": "SharedStatus"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)

	// Verify reference was resolved
	resolvedDef := schemaB.fields.enums[0]
	assert.False(t, resolvedDef.isReference())
	assert.Equal(t, []string{"active", "banned"}, resolvedDef.fields)
	assert.Equal(t, 1, resolvedDef.mapping["active"])
	assert.Equal(t, 2, resolvedDef.mapping["banned"])
	assert.Equal(t, "active", resolvedDef.defaultValue)

	// Verify tags were updated for checkColumn compatibility
	assert.Equal(t, "active,banned", schemaB.tags["Status"]["enum"])
}

func TestSharedSetResolution(t *testing.T) {
	// Positive test: Entity A defines set values, Entity B references them via set
	schemaA := &entitySchema{
		fields: &tableFields{
			sets: []*enumDefinition{
				initEnumDefinition("SharedTags", []string{"vip", "premium", "basic"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			sets: []*enumDefinition{
				{name: "SharedTags", required: false}, // reference
			},
		},
		tags: map[string]map[string]string{
			"Tags": {"set": "true", "enumName": "SharedTags"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)

	resolvedDef := schemaB.fields.sets[0]
	assert.False(t, resolvedDef.isReference())
	assert.Equal(t, []string{"vip", "premium", "basic"}, resolvedDef.fields)
	assert.Equal(t, "vip,premium,basic", schemaB.tags["Tags"]["set"])
}

func TestSharedEnumMixedTypes(t *testing.T) {
	// enum definition in entity A, set reference in entity B (mixed types, shared values)
	schemaA := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SharedValues", []string{"x", "y", "z"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			sets: []*enumDefinition{
				{name: "SharedValues", required: false}, // reference via set
			},
		},
		tags: map[string]map[string]string{
			"Values": {"set": "true", "enumName": "SharedValues"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)

	resolvedDef := schemaB.fields.sets[0]
	assert.Equal(t, []string{"x", "y", "z"}, resolvedDef.fields)
	assert.Equal(t, "x,y,z", schemaB.tags["Values"]["set"])
}

func TestSharedEnumDuplicateDefinitionError(t *testing.T) {
	// Two entities both define DIFFERENT values for the same enumName
	schemaA := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("DupEnum", []string{"a", "b"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("DupEnum", []string{"c", "d"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DupEnum")
	assert.Contains(t, err.Error(), "definition must be in only one entity")
}

func TestSharedEnumSameValuesAcrossEntitiesOK(t *testing.T) {
	// Backward compat: two entities define the same enumName with identical values
	schemaA := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SharedEnum", []string{"a", "b", "c"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SharedEnum", []string{"a", "b", "c"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)
}

func TestSharedEnumMissingDefinitionError(t *testing.T) {
	// Reference to an enumName that no entity defines
	schema := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				{name: "NonExistent", required: false}, // reference
			},
		},
		tags: map[string]map[string]string{
			"Status": {"enum": "true", "enumName": "NonExistent"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schema,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistent")
	assert.Contains(t, err.Error(), "no entity defines its values")
}

func TestSharedEnumMissingSetDefinitionError(t *testing.T) {
	// Reference to an enumName via set that no entity defines
	schema := &entitySchema{
		fields: &tableFields{
			sets: []*enumDefinition{
				{name: "NonExistentSet", required: false}, // reference
			},
		},
		tags: map[string]map[string]string{
			"Tags": {"set": "true", "enumName": "NonExistentSet"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schema,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentSet")
	assert.Contains(t, err.Error(), "no entity defines its values")
}

func TestSharedEnumSameEntityMultipleFieldsOK(t *testing.T) {
	// Backward compat: same enumName with values in multiple fields within the SAME entity
	schema := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SameEnum", []string{"a", "b", "c"}, false),
				initEnumDefinition("SameEnum", []string{"a", "b", "c"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schema,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)
}

func TestSharedEnumInStructFields(t *testing.T) {
	// Enum definition in nested struct fields should be found and resolved
	schemaA := &entitySchema{
		fields: &tableFields{
			structsFields: []*tableFields{
				{
					enums: []*enumDefinition{
						initEnumDefinition("NestedEnum", []string{"p", "q"}, false),
					},
				},
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				{name: "NestedEnum", required: true}, // reference
			},
		},
		tags: map[string]map[string]string{
			"Status": {"enum": "true", "enumName": "NestedEnum"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)

	resolvedDef := schemaB.fields.enums[0]
	assert.Equal(t, []string{"p", "q"}, resolvedDef.fields)
	assert.True(t, resolvedDef.required)
}

func TestBuildTableFieldsEnumWithoutValuesAndWithoutEnumName(t *testing.T) {
	// enum without values and without enumName should error during buildTableFields
	schema := &entitySchema{
		t:                reflect.TypeOf(struct{ Status string }{}),
		fieldDefinitions: make(map[string]schemaFieldAttributes),
	}
	tags := map[string]map[string]string{
		"Status": {"enum": "true"},
	}
	_, err := schema.buildTableFields(
		reflect.TypeOf(struct{ Status string }{}),
		&registry{},
		0,
		"",
		nil,
		tags,
		"",
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "enum without values requires enumName")
}

func TestBuildTableFieldsSetWithoutValuesAndWithoutEnumName(t *testing.T) {
	// set without values and without enumName should error during buildTableFields
	schema := &entitySchema{
		t:                reflect.TypeOf(struct{ Tags string }{}),
		fieldDefinitions: make(map[string]schemaFieldAttributes),
	}
	tags := map[string]map[string]string{
		"Tags": {"set": "true"},
	}
	_, err := schema.buildTableFields(
		reflect.TypeOf(struct{ Tags string }{}),
		&registry{},
		0,
		"",
		nil,
		tags,
		"",
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "set without values requires enumName")
}

func TestBuildTableFieldsEnumReferenceCreatesPlaceholder(t *testing.T) {
	// enum with enumName but no values should create a placeholder (reference)
	schema := &entitySchema{
		t:                reflect.TypeOf(struct{ Status string }{}),
		fieldDefinitions: make(map[string]schemaFieldAttributes),
	}
	tags := map[string]map[string]string{
		"Status": {"enum": "true", "enumName": "MyEnum"},
	}
	fields, err := schema.buildTableFields(
		reflect.TypeOf(struct{ Status string }{}),
		&registry{},
		0,
		"",
		nil,
		tags,
		"",
	)
	assert.NoError(t, err)
	assert.Len(t, fields.enums, 1)
	assert.True(t, fields.enums[0].isReference())
	assert.Equal(t, "MyEnum", fields.enums[0].name)
}

func TestBuildTableFieldsSetReferenceCreatesPlaceholder(t *testing.T) {
	// set with enumName but no values should create a placeholder (reference)
	schema := &entitySchema{
		t:                reflect.TypeOf(struct{ Tags string }{}),
		fieldDefinitions: make(map[string]schemaFieldAttributes),
	}
	tags := map[string]map[string]string{
		"Tags": {"set": "true", "enumName": "MySet"},
	}
	fields, err := schema.buildTableFields(
		reflect.TypeOf(struct{ Tags string }{}),
		&registry{},
		0,
		"",
		nil,
		tags,
		"",
	)
	assert.NoError(t, err)
	assert.Len(t, fields.sets, 1)
	assert.True(t, fields.sets[0].isReference())
	assert.Equal(t, "MySet", fields.sets[0].name)
}

func TestBuildTableFieldsEnumNameOnlyCreatesPlaceholder(t *testing.T) {
	// enumName without enum tag should create an enum reference placeholder
	schema := &entitySchema{
		t:                reflect.TypeOf(struct{ Status string }{}),
		fieldDefinitions: make(map[string]schemaFieldAttributes),
	}
	tags := map[string]map[string]string{
		"Status": {"enumName": "MyEnum"},
	}
	fields, err := schema.buildTableFields(
		reflect.TypeOf(struct{ Status string }{}),
		&registry{},
		0,
		"",
		nil,
		tags,
		"",
	)
	assert.NoError(t, err)
	assert.Len(t, fields.enums, 1)
	assert.True(t, fields.enums[0].isReference())
	assert.Equal(t, "MyEnum", fields.enums[0].name)
}

func TestSharedEnumResolutionEnumNameOnly(t *testing.T) {
	// Entity A defines values, Entity B references with enumName only (no enum tag)
	schemaA := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				initEnumDefinition("SharedStatus", []string{"active", "banned"}, false),
			},
		},
		tags: map[string]map[string]string{},
	}
	schemaB := &entitySchema{
		fields: &tableFields{
			enums: []*enumDefinition{
				{name: "SharedStatus", required: false}, // reference (no enum tag)
			},
		},
		tags: map[string]map[string]string{
			"Status": {"enumName": "SharedStatus"},
		},
	}

	schemas := map[reflect.Type]*entitySchema{
		reflect.TypeOf(struct{ A int }{}): schemaA,
		reflect.TypeOf(struct{ B int }{}): schemaB,
	}

	err := resolveSharedEnumDefinitions(schemas)
	assert.NoError(t, err)

	// Verify reference was resolved
	resolvedDef := schemaB.fields.enums[0]
	assert.False(t, resolvedDef.isReference())
	assert.Equal(t, []string{"active", "banned"}, resolvedDef.fields)
	assert.Equal(t, 1, resolvedDef.mapping["active"])
	assert.Equal(t, 2, resolvedDef.mapping["banned"])

	// Verify tags were updated (enum tag added from resolution)
	assert.Equal(t, "active,banned", schemaB.tags["Status"]["enum"])
}
