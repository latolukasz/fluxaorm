package fluxaorm

import (
	"fmt"
	"os"
	"path"
	"strings"
)

func (g *codeGenerator) generateGettersSetters(entityName, providerName string, schema *entitySchema, fields *tableFields) error {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			g.filedIndex++
			continue
		}
		g.createGetterSetterUint64(schema, fieldName, entityName, "", providerName)
	}
	for k, i := range fields.references {
		fieldName := fields.prefix + fields.fields[i].Name
		refTypeName := schema.references[fieldName].Type.String()
		refName := g.capitalizeFirst(refTypeName[strings.LastIndex(refTypeName, ".")+1:])
		required := fields.referencesRequired[k]
		if required {
			g.createGetterSetterUint64(schema, fieldName, entityName, "ID", providerName)
		} else {
			g.createGetterSetterUint64Nullable(schema, fieldName, entityName, "ID", providerName)
		}
		g.addLine(fmt.Sprintf("func (e *%s) Get%s(ctx fluxaorm.Context) (reference *%s, found bool, err error) {", entityName, fieldName, refName))
		g.addLine(fmt.Sprintf("\tid := e.Get%sID()", fieldName))
		if required {
			g.addLine("\tif id == 0 {\n\t\treturn nil, false, nil\n\t}")
			g.addLine(fmt.Sprintf("\treturn %sProvider.GetByID(ctx, id)", refName))
		} else {
			g.addLine("\tif id == nil || *id == 0 {\n\t\treturn nil, false, nil\n\t}")
			g.addLine(fmt.Sprintf("\treturn %sProvider.GetByID(ctx, *id)", refName))
		}
		g.addLine("}")
		g.addLine("")
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterInt64(schema, fieldName, entityName, providerName)
	}
	for _, i := range fields.booleans {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterBool(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.floats {
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterFloat(schema, fieldName, entityName, providerName, fields.floatsPrecision[k], fields.floatsSize[k])
	}
	for _, i := range fields.times {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, false)
	}
	for _, i := range fields.dates {
		g.addImport("time")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTime(schema, fieldName, entityName, true)
	}
	for k, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		if fields.stringsRequired[k] {
			g.createGetterSetterString(schema, fieldName, entityName, providerName)
		} else {
			g.addImport("database/sql")
			g.createGetterSetterStringNullable(schema, fieldName, entityName, providerName)
		}
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterUint64Nullable(schema, fieldName, entityName, "", providerName)
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterInt64Nullable(schema, fieldName, entityName, "", providerName)
	}
	for k, i := range fields.stringsEnums {
		g.addImport(g.enumsImport)
		if g.enums == nil {
			g.enums = make(map[string]bool)
			err := os.MkdirAll(path.Join(g.dir, "enums"), 0755)
			if err != nil {
				return err
			}
		}
		d := fields.enums[k]
		enumName := d.name
		_, enumCreated := g.enums[enumName]
		if !enumCreated {
			err := g.createEnumDefinition(d, enumName)
			if err != nil {
				return err
			}
			g.enums[enumName] = true
		}
		enumFullName := "enums." + enumName
		fieldName := fields.prefix + fields.fields[i].Name
		if d.required {
			g.createGetterSetterEnum(schema, fieldName, entityName, enumFullName, providerName)
		} else {
			g.addImport("database/sql")
			g.createGetterSetterEnumNullable(schema, fieldName, entityName, enumFullName, providerName)
		}
	}
	for _, i := range fields.bytes {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterBytesNullable(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.sliceStringsSets {
		g.addImport(g.enumsImport)
		if g.enums == nil {
			g.enums = make(map[string]bool)
			err := os.MkdirAll(path.Join(g.dir, "enums"), 0755)
			if err != nil {
				return err
			}
		}
		d := fields.sets[k]
		enumName := d.name
		_, enumCreated := g.enums[enumName]
		if !enumCreated {
			err := g.createEnumDefinition(d, enumName)
			if err != nil {
				return err
			}
			g.enums[enumName] = true
		}
		enumFullName := "enums." + enumName
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("strings")
		if d.required {
			g.createGetterSetterSet(schema, fieldName, entityName, enumFullName, providerName)
		} else {
			g.createGetterSetterSetNullable(schema, fieldName, entityName, enumFullName, providerName)
		}
	}
	for _, i := range fields.booleansNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.createGetterSetterBoolNullable(schema, fieldName, entityName, providerName)
	}
	for k, i := range fields.floatsNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		g.addImport("database/sql")
		g.addImport("math")
		g.createGetterSetterFloatNullable(schema, fieldName, entityName, providerName, fields.floatsNullablePrecision[k], fields.floatsNullableSize[k])
	}
	for _, i := range fields.timesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTimeNullable(schema, fieldName, entityName, providerName, false)
	}
	for _, i := range fields.datesNullable {
		g.addImport("time")
		g.addImport("database/sql")
		fieldName := fields.prefix + fields.fields[i].Name
		g.createGetterSetterTimeNullable(schema, fieldName, entityName, providerName, true)
	}
	for _, subFields := range fields.structsFields {
		err := g.generateGettersSetters(entityName, providerName, schema, subFields)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *codeGenerator) addSQLRowLines(fields *tableFields) string {
	result := ""
	for range fields.uIntegers {
		result += fmt.Sprintf("\tF%d uint64\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.references {
		if fields.referencesRequired[k] {
			result += fmt.Sprintf("\tF%d uint64\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.integers {
		result += fmt.Sprintf("\tF%d int64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.booleans {
		result += fmt.Sprintf("\tF%d bool\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floats {
		result += fmt.Sprintf("\tF%d float64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.times {
		result += fmt.Sprintf("\tF%d time.Time\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.dates {
		result += fmt.Sprintf("\tF%d time.Time\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.strings {
		if fields.stringsRequired[k] {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.uIntegersNullable {
		result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.integersNullable {
		result += fmt.Sprintf("\tF%d sql.NullInt64\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.bytes {
		result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		g.filedIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		if d.required {
			result += fmt.Sprintf("\tF%d string\n", g.filedIndex)
		} else {
			result += fmt.Sprintf("\tF%d sql.NullString\n", g.filedIndex)
		}
		g.filedIndex++
	}
	for range fields.booleansNullable {
		result += fmt.Sprintf("\tF%d sql.NullBool\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.floatsNullable {
		result += fmt.Sprintf("\tF%d sql.NullFloat64\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.timesNullable {
		result += fmt.Sprintf("\tF%d sql.NullTime\n", g.filedIndex)
		g.filedIndex++
	}
	for range fields.datesNullable {
		result += fmt.Sprintf("\tF%d sql.NullTime\n", g.filedIndex)
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		result += g.addSQLRowLines(subFields)
	}
	return result
}

func (g *codeGenerator) addRedisBindSetLines(schema *entitySchema, fields *tableFields) {
	for range fields.uIntegers {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for k := range fields.references {
		if fields.referencesRequired[k] {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
			g.addLine("\t} else {")
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.Int64", g.filedIndex+1, g.filedIndex))
			g.addLine("\t}")
		}
		g.filedIndex++
	}
	for range fields.integers {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for range fields.booleans {
		g.addLine(fmt.Sprintf("\tif r.F%d  {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"1\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t} else {"))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"0\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t}"))
		g.filedIndex++
	}
	for k := range fields.floats {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = strconv.FormatFloat(r.F%d, 'f', %d, %d)", g.filedIndex+1, g.filedIndex, fields.floatsPrecision[k], fields.floatsSize[k]))
		g.filedIndex++
	}
	for range fields.times {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.Unix()", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for range fields.dates {
		g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.Unix()", g.filedIndex+1, g.filedIndex))
		g.filedIndex++
	}
	for k := range fields.strings {
		if fields.stringsRequired[k] {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		}
		g.filedIndex++
	}
	for range fields.uIntegersNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = uint64(r.F%d.Int64)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.integersNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.Int64", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.stringsEnums {
		d := fields.enums[k]
		if d.required {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
			g.addLine("\t} else {")
			g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
			g.addLine("\t}")
		}
		g.filedIndex++
	}
	for range fields.bytes {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.sliceStringsSets {
		d := fields.sets[k]
		g.addImport("strings")
		if d.required {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d", g.filedIndex+1, g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tredisListValues[%d] = r.F%d.String", g.filedIndex+1, g.filedIndex))
		}
		g.filedIndex++
	}
	for range fields.booleansNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tif r.F%d.Bool {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tredisListValues[%d] = \"1\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t\t} else {"))
		g.addLine(fmt.Sprintf("\t\t\tredisListValues[%d] = \"0\"", g.filedIndex+1))
		g.addLine(fmt.Sprintf("\t\t}"))
		g.addLine("\t}")
		g.filedIndex++
	}
	for k := range fields.floatsNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid { ", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatFloat(r.F%d.Float64, 'f', %d, %d)", g.filedIndex+1, g.filedIndex, fields.floatsNullablePrecision[k], fields.floatsNullableSize[k]))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.timesNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatInt(r.F%d.Time.Unix(), 10)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for range fields.datesNullable {
		g.addLine(fmt.Sprintf("\tif !r.F%d.Valid {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = \"\"", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tredisListValues[%d] = strconv.FormatInt(r.F%d.Time.Unix(), 10)", g.filedIndex+1, g.filedIndex))
		g.addLine("\t}")
		g.filedIndex++
	}
	for _, subFields := range fields.structsFields {
		g.addRedisBindSetLines(schema, subFields)
	}
}

func (g *codeGenerator) createEnumDefinition(d *enumDefinition, name string) error {
	fileName := path.Join(g.dir, "enums", fmt.Sprintf("%s.go", name))
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	g.writeToFile(f, "package enums\n")
	g.writeToFile(f, "\n")
	g.writeToFile(f, fmt.Sprintf("type %s string\n", name))
	g.writeToFile(f, "")
	g.writeToFile(f, fmt.Sprintf("var %sList = struct {\n", name))
	for _, tName := range d.fieldNames {
		g.writeToFile(f, fmt.Sprintf("\t%s %s\n", tName, name))
	}
	g.writeToFile(f, "}{\n")
	for i, v := range d.fields {
		g.writeToFile(f, fmt.Sprintf("\t%s: \"%s\",\n", d.fieldNames[i], v))
	}
	g.writeToFile(f, "}\n")
	return nil
}
