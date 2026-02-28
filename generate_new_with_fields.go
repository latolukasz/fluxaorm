package fluxaorm

import "fmt"

func (g *codeGenerator) generateNewWithFields(schema *entitySchema, names *entityNames) {
	fieldsName := names.entityName + "Fields"

	// Generate the Fields struct
	g.addLine(fmt.Sprintf("type %s struct {", fieldsName))
	g.addFieldsStructFields(schema, schema.fields)
	g.addLine("}")
	g.addLine("")

	// Generate the NewWithFields method
	g.addLine(fmt.Sprintf("func (p %s) NewWithFields(ctx fluxaorm.Context, fields *%s) *%s {", names.providerNamePrivate, fieldsName, names.entityName))
	g.addLine("\te := p.New(ctx)")
	g.addLine("\tif fields == nil {")
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	g.addNewWithFieldsSetters(schema, names, schema.fields)
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
}

func (g *codeGenerator) addFieldsStructFields(schema *entitySchema, fields *tableFields) {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			continue
		}
		g.addLine(fmt.Sprintf("\t%s *uint64", fieldName))
	}
	for _, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *uint64", fieldName))
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *int64", fieldName))
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *bool", fieldName))
	}
	for _, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *float64", fieldName))
	}
	for _, i := range fields.times {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t%s *time.Time", fieldName))
	}
	for _, i := range fields.dates {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t%s *time.Time", fieldName))
	}
	for _, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *string", fieldName))
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *uint64", fieldName))
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *int64", fieldName))
	}
	for k, i := range fields.stringsEnums {
		d := fields.enums[k]
		enumName := "enums." + d.name
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport(g.enumsImport)
		g.addLine(fmt.Sprintf("\t%s *%s", fieldName, enumName))
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s []uint8", fieldName))
	}
	for k, i := range fields.sliceStringsSets {
		d := fields.sets[k]
		enumName := "enums." + d.name
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport(g.enumsImport)
		g.addLine(fmt.Sprintf("\t%s []%s", fieldName, enumName))
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *bool", fieldName))
	}
	for _, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\t%s *float64", fieldName))
	}
	for _, i := range fields.timesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t%s *time.Time", fieldName))
	}
	for _, i := range fields.datesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("time")
		g.addLine(fmt.Sprintf("\t%s *time.Time", fieldName))
	}
	for _, subFields := range fields.structsFields {
		g.addFieldsStructFields(schema, subFields)
	}
}

func (g *codeGenerator) addNewWithFieldsSetters(schema *entitySchema, names *entityNames, fields *tableFields) {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			continue
		}
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		_ = fields.referencesRequired[k] // both required and optional take uint64 value
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.times {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.dates {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		_ = fields.stringsRequired[k] // both required and nullable string setters take string value
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *uint64 — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *int64 — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for k, i := range fields.stringsEnums {
		d := fields.enums[k]
		fieldName := fields.prefix + fields.fields[i].Name
		if d.required {
			// Setter takes enums.X — dereference
			g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
			g.addLine(fmt.Sprintf("\t\te.Set%s(*fields.%s)", fieldName, fieldName))
			g.addLine("\t}")
		} else {
			// Setter takes *enums.X — pass pointer directly
			g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
			g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
			g.addLine("\t}")
		}
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes []uint8 — pass slice directly, nil slice = skip
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.sliceStringsSets {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes ...enums.X — expand slice, nil slice = skip
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s...)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *bool — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *float64 — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.timesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *time.Time — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, i := range fields.datesNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		// Setter takes *time.Time — pass pointer directly
		g.addLine(fmt.Sprintf("\tif fields.%s != nil {", fieldName))
		g.addLine(fmt.Sprintf("\t\te.Set%s(fields.%s)", fieldName, fieldName))
		g.addLine("\t}")
	}
	for _, subFields := range fields.structsFields {
		g.addNewWithFieldsSetters(schema, names, subFields)
	}
}
