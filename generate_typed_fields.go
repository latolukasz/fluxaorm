package fluxaorm

import (
	"fmt"
	"strings"
)

func (g *codeGenerator) generateTypedFields(schema *entitySchema, names *entityNames) {
	// Generate the Fields struct type
	g.addLine(fmt.Sprintf("type %sFields struct {", names.entityPrivate))
	for _, columnName := range schema.columnNames {
		if columnName == "ID" {
			g.addLine(fmt.Sprintf("\tID fluxaorm.UintField"))
			continue
		}
		if schema.hasFakeDelete && columnName == "FakeDelete" {
			continue
		}
		fieldType := g.resolveFieldType(schema, columnName)
		if fieldType == "" {
			continue
		}
		g.addLine(fmt.Sprintf("\t%s fluxaorm.%s", columnName, fieldType))
	}
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateTypedFieldsInit(schema *entitySchema, names *entityNames) {
	g.addLine(fmt.Sprintf("\tFields: %sFields{", names.entityPrivate))
	for _, columnName := range schema.columnNames {
		if columnName == "ID" {
			g.addLine(fmt.Sprintf("\t\tID: fluxaorm.UintField{Column: \"ID\"},"))
			continue
		}
		if schema.hasFakeDelete && columnName == "FakeDelete" {
			continue
		}
		fieldType := g.resolveFieldType(schema, columnName)
		if fieldType == "" {
			continue
		}
		g.addLine(fmt.Sprintf("\t\t%s: fluxaorm.%s{Column: \"%s\"},", columnName, fieldType, columnName))
	}
	g.addLine("\t},")
}

func (g *codeGenerator) resolveFieldType(schema *entitySchema, columnName string) string {
	attributes := schema.fieldDefinitions[columnName]
	typeName := attributes.TypeName
	tags := attributes.Tags

	// Check if it's a reference
	if _, isRef := schema.references[columnName]; isRef {
		if tags["required"] == "true" {
			return "ReferenceField"
		}
		return "NullableUintField"
	}

	// Skip bytes
	if typeName == "[]uint8" {
		return ""
	}

	// Skip sets
	if _, hasSet := tags["set"]; hasSet {
		return ""
	}

	// Handle pointer types
	if strings.HasPrefix(typeName, "*") {
		switch {
		case strings.HasPrefix(typeName, "*uint"):
			return "NullableUintField"
		case strings.HasPrefix(typeName, "*int"):
			return "NullableIntField"
		case strings.HasPrefix(typeName, "*float"):
			return "NullableFloatField"
		case typeName == "*bool":
			return "NullableBoolField"
		case typeName == "*time.Time":
			return "NullableTimeField"
		case typeName == "*string":
			return "NullableStringField"
		default:
			// JSON struct - skip
			return ""
		}
	}

	// Handle non-pointer types
	switch {
	case strings.HasPrefix(typeName, "uint"):
		return "UintField"
	case strings.HasPrefix(typeName, "int"):
		return "IntField"
	case strings.HasPrefix(typeName, "float"):
		return "FloatField"
	case typeName == "bool":
		return "BoolField"
	case typeName == "time.Time":
		return "TimeField"
	case typeName == "string":
		_, hasEnum := tags["enum"]
		_, hasEnumName := tags["enumName"]
		if hasEnum || hasEnumName {
			if tags["required"] == "true" {
				return "EnumField"
			}
			return "NullableEnumField"
		}
		if tags["required"] == "true" {
			return "StringField"
		}
		return "NullableStringField"
	}

	return ""
}

func redisSearchGoFieldType(sf searchableFieldDef) string {
	switch sf.redisType {
	case "NUMERIC":
		switch sf.goKind {
		case "uint", "ref":
			return "RedisSearchUintField"
		case "int":
			return "RedisSearchIntField"
		default:
			return "RedisSearchNumericField"
		}
	case "TEXT":
		return "RedisSearchTextField"
	case "TAG":
		return "RedisSearchTagField"
	default:
		return ""
	}
}

func (g *codeGenerator) generateRedisSearchFields(schema *entitySchema, names *entityNames) {
	if !schema.hasRedisSearch {
		return
	}

	// Generate the FieldsRedisSearch struct type
	g.addLine(fmt.Sprintf("type %sFieldsRedisSearch struct {", names.entityPrivate))
	for _, sf := range schema.searchableFields {
		fieldType := redisSearchGoFieldType(sf)
		if fieldType == "" {
			continue
		}
		g.addLine(fmt.Sprintf("\t%s fluxaorm.%s", sf.columnName, fieldType))
	}
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) generateRedisSearchFieldsInit(schema *entitySchema, names *entityNames) {
	if !schema.hasRedisSearch {
		return
	}

	g.addLine(fmt.Sprintf("\tFieldsRedisSearch: %sFieldsRedisSearch{", names.entityPrivate))
	for _, sf := range schema.searchableFields {
		fieldType := redisSearchGoFieldType(sf)
		if fieldType == "" {
			continue
		}
		g.addLine(fmt.Sprintf("\t\t%s: fluxaorm.%s{Column: \"%s\"},", sf.columnName, fieldType, sf.columnName))
	}
	g.addLine("\t},")
}
