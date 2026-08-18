package fluxaorm

import (
	"fmt"
)

func (g *codeGenerator) createGetterSetterUint64(schema *entitySchema, fieldName, entityName, getterSuffix, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s%s() uint64 {", entityName, fieldName, getterSuffix))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(uint64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value uint64) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseUint(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterInt64(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() int64 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(int64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value int64) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBool(schema *entitySchema, fieldName, entityName, providerName string) {
	isFakeDelete := schema.hasFakeDelete && fieldName == "FakeDelete"

	g.addLine(fmt.Sprintf("func (e *%s) Get%s() bool {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	if isFakeDelete {
		g.addLine("\t\t\t\treturn v.(uint64) != 0")
	} else {
		g.addLine("\t\t\t\treturn v.(bool)")
	}
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	if isFakeDelete {
		g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d != 0;", g.filedIndex))
	} else {
		g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d;", g.filedIndex))
	}
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value bool) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif e.new {")
	if isFakeDelete {
		g.addLine("\t\tif value {")
		g.addLine(fmt.Sprintf("\t\t\te.originDatabaseValues.F%d = e.id", g.filedIndex))
		g.addLine("\t\t} else {")
		g.addLine(fmt.Sprintf("\t\t\te.originDatabaseValues.F%d = 0", g.filedIndex))
		g.addLine("\t\t}")
	} else {
		g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	}
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis := e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		if isFakeDelete {
			g.addLine(fmt.Sprintf("\t\tsame = (e.originDatabaseValues.F%d != 0) == value", g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		}
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		if isFakeDelete {
			g.addLine(fmt.Sprintf("\tif (e.originDatabaseValues.F%d != 0) == value {", g.filedIndex))
		} else {
			g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		}
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	if isFakeDelete {
		g.addLine("\tif value {")
		g.addLine(fmt.Sprintf("\t\te.addToDatabaseBind(\"%s\", e.id)", fieldName))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToDatabaseBind(\"%s\", uint64(0))", fieldName))
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	}
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterFloat(schema *entitySchema, fieldName, entityName, providerName string, precision, size int) {
	g.addImport("math")
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() float64 {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(float64)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine("\t\t\treturn fromRedis")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value float64) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseFloat(e.originRedisValues[%d], 64)", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(fromRedis*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", precision, precision))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = math.Round(e.originDatabaseValues.F%d*math.Pow10(%d)) == math.Round(value*math.Pow10(%d))", g.filedIndex, precision, precision))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif math.Round(e.originDatabaseValues.F%d*math.Pow10(%d)) == math.Round(value*math.Pow10(%d)) {", g.filedIndex, precision, precision))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterTime(schema *entitySchema, fieldName, entityName string, dateOnly bool) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() time.Time {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(time.Time)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\t\treturn time.Unix(fromRedis, 0).UTC()")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) *%s {", entityName, fieldName, entityName))
	if dateOnly {
		g.addLine("\tvalue = value.Truncate(time.Hour * 24)")
	} else {
		g.addLine("\tvalue = value.Truncate(time.Second)")
	}
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis, _ := strconv.ParseInt(e.originRedisValues[%d], 10, 64)", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value.Unix()")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d.Unix() == value.Unix()", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Unix() == value.Unix() {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterString(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() string {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(string)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value string) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterEnum(schema *entitySchema, fieldName, entityName, enumName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, enumName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine(fmt.Sprintf("\t\t\t\treturn %s(v.(string))", enumName))
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn %s(e.originRedisValues[%d])", enumName, g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn %s(e.originDatabaseValues.F%d)", enumName, g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) *%s {", entityName, fieldName, enumName, entityName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = string(value)", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == string(value)", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == string(value)", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == string(value) {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", string(value))", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterSet(schema *entitySchema, fieldName, entityName, setName, providerName string) {
	g.addImport("sort")
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() []%s {", entityName, fieldName, setName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\tsliced := strings.Split(v.(string), \",\")")
	g.addLine(fmt.Sprintf("\t\t\t\tvalue := make([]%s, len(sliced))", setName))
	g.addLine("\t\t\t\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\t\t\t\tvalue[k] = %s(code)", setName))
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\treturn value")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tif e.originRedisValues[%d] == \"\" {", g.filedIndex))
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine(fmt.Sprintf("\t\t\tsliced := strings.Split(e.originRedisValues[%d], \",\")", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tvalue := make([]%s, len(sliced))", setName))
		g.addLine("\t\t\tfor k, code := range sliced {")
		g.addLine(fmt.Sprintf("\t\t\t\tvalue[k] = %s(code)", setName))
		g.addLine("\t\t\t}")
		g.addLine("\t\t\treturn value")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == \"\" {", g.filedIndex))
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\tsliced := strings.Split(e.originDatabaseValues.F%d, \",\")", g.filedIndex))
	g.addLine(fmt.Sprintf("\tfinalValue := make([]%s, len(sliced))", setName))
	g.addLine("\tfor k, code := range sliced {")
	g.addLine(fmt.Sprintf("\t\tfinalValue[k] = %s(code)", setName))
	g.addLine("\t}")
	g.addLine("\treturn finalValue")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value ...%s) *%s {", entityName, fieldName, setName, entityName))
	g.addLine("\tslice := make([]string, len(value))")
	g.addLine("\tfor k, v := range value {")
	g.addLine("\t\tslice[k] = string(v)")
	g.addLine("\t}")
	g.addLine("\tsort.Strings(slice)")
	g.addLine("\tasString := strings.Join(slice, \",\")")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = asString", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == asString", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == asString", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == asString {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", asString)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterReferencesRequired(schema *entitySchema, fieldName, entityName, providerName, refName string) {
	g.addImport("encoding/json")

	// GetFieldIDs() []uint64
	g.addLine(fmt.Sprintf("func (e *%s) Get%sIDs() []uint64 {", entityName, fieldName))
	g.addLine("\tvar raw string")
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\traw = v.(string)")
	g.addLine("\t\t\t\tvar ids []uint64")
	g.addLine("\t\t\t\t_ = json.Unmarshal([]byte(raw), &ids)")
	g.addLine("\t\t\t\treturn ids")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\traw = e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t\tvar ids []uint64")
		g.addLine("\t\t\t_ = json.Unmarshal([]byte(raw), &ids)")
		g.addLine("\t\t\treturn ids")
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\traw = e.originDatabaseValues.F%d", g.filedIndex))
	g.addLine("\tif raw == \"\" {")
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
	g.addLine("\tvar ids []uint64")
	g.addLine("\t_ = json.Unmarshal([]byte(raw), &ids)")
	g.addLine("\treturn ids")
	g.addLine("}")
	g.addLine("")

	// SetFieldIDs([]uint64)
	g.addLine(fmt.Sprintf("func (e *%s) Set%sIDs(ids []uint64) *%s {", entityName, fieldName, entityName))
	g.addLine("\tvar value string")
	g.addLine("\tif len(ids) == 0 {")
	g.addLine("\t\tvalue = \"[]\"")
	g.addLine("\t} else {")
	g.addLine("\t\tb, _ := json.Marshal(ids)")
	g.addLine("\t\tvalue = string(b)")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame := false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")

	// GetField(ctx) ([]*RefEntity, error)
	g.addLine(fmt.Sprintf("func (e *%s) Get%s(ctx fluxaorm.Context) ([]*%s, error) {", entityName, fieldName, refName))
	g.addLine(fmt.Sprintf("\tids := e.Get%sIDs()", fieldName))
	g.addLine("\tif len(ids) == 0 {")
	g.addLine("\t\treturn nil, nil")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn %sProvider.GetByIDs(ctx, ids...)", refName))
	g.addLine("}")
	g.addLine("")

	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterReferencesNullable(schema *entitySchema, fieldName, entityName, providerName, refName string) {
	g.addImport("encoding/json")
	g.addImport("database/sql")

	// GetFieldIDs() []uint64
	g.addLine(fmt.Sprintf("func (e *%s) Get%sIDs() []uint64 {", entityName, fieldName))
	g.addLine("\tvar raw string")
	g.addLine("\tvar valid bool")
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\t_nv := v.(sql.NullString)")
	g.addLine("\t\t\t\tif !_nv.Valid {")
	g.addLine("\t\t\t\t\treturn nil")
	g.addLine("\t\t\t\t}")
	g.addLine("\t\t\t\traw = _nv.String")
	g.addLine("\t\t\t\tvalid = true")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif !valid && e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\traw = e.originRedisValues[%d]", g.filedIndex))
		g.addLine("\t\t\tif raw == \"\" {")
		g.addLine("\t\t\t\treturn nil")
		g.addLine("\t\t\t}")
		g.addLine("\t\t\tvalid = true")
		g.addLine("\t\t}")
	}
	g.addLine("\t\tif !valid {")
	g.addLine(fmt.Sprintf("\t\t\tif !e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine("\t\t\t\treturn nil")
	g.addLine("\t\t\t}")
	g.addLine(fmt.Sprintf("\t\t\traw = e.originDatabaseValues.F%d.String", g.filedIndex))
	g.addLine("\t\t}")
	g.addLine("\t} else {")
	g.addLine(fmt.Sprintf("\t\tif !e.originDatabaseValues.F%d.Valid {", g.filedIndex))
	g.addLine("\t\t\treturn nil")
	g.addLine("\t\t}")
	g.addLine(fmt.Sprintf("\t\traw = e.originDatabaseValues.F%d.String", g.filedIndex))
	g.addLine("\t}")
	g.addLine("\tif raw == \"\" {")
	g.addLine("\t\treturn nil")
	g.addLine("\t}")
	g.addLine("\tvar ids []uint64")
	g.addLine("\t_ = json.Unmarshal([]byte(raw), &ids)")
	g.addLine("\treturn ids")
	g.addLine("}")
	g.addLine("")

	// SetFieldIDs([]uint64)
	g.addLine(fmt.Sprintf("func (e *%s) Set%sIDs(ids []uint64) *%s {", entityName, fieldName, entityName))
	g.addLine("\tif ids == nil {")
	g.addLine("\t\tif e.new {")
	g.addLine(fmt.Sprintf("\t\t\te.originDatabaseValues.F%d = sql.NullString{}", g.filedIndex))
	g.addLine("\t\t\treturn e")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tsame := false")
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\tsame = e.originRedisValues[%d] == \"\"", g.filedIndex))
		g.addLine("\t\t} else {")
		g.addLine(fmt.Sprintf("\t\t\tsame = !e.originDatabaseValues.F%d.Valid", g.filedIndex))
		g.addLine("\t\t}")
		g.addLine("\t\tif same {")
		g.addLine(fmt.Sprintf("\t\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\t\treturn e")
		g.addLine("\t\t}")
	} else {
		g.addLine(fmt.Sprintf("\t\tif !e.originDatabaseValues.F%d.Valid {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\t\treturn e")
		g.addLine("\t\t}")
	}
	g.addLine(fmt.Sprintf("\t\te.addToDatabaseBind(\"%s\", sql.NullString{})", fieldName))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	g.addLine("\tvar value string")
	g.addLine("\tif len(ids) == 0 {")
	g.addLine("\t\tvalue = \"[]\"")
	g.addLine("\t} else {")
	g.addLine("\t\tb, _ := json.Marshal(ids)")
	g.addLine("\t\tvalue = string(b)")
	g.addLine("\t}")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = sql.NullString{String: value, Valid: true}", g.filedIndex))
	g.addLine("\t\treturn e")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame := false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originRedisValues[%d] == value", g.filedIndex))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d.Valid && e.originDatabaseValues.F%d.String == value", g.filedIndex, g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Valid && e.originDatabaseValues.F%d.String == value {", g.filedIndex, g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn e")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", sql.NullString{String: value, Valid: true})", fieldName))
	g.addLine("\treturn e")
	g.addLine("}")
	g.addLine("")

	// GetField(ctx) ([]*RefEntity, error)
	g.addLine(fmt.Sprintf("func (e *%s) Get%s(ctx fluxaorm.Context) ([]*%s, error) {", entityName, fieldName, refName))
	g.addLine(fmt.Sprintf("\tids := e.Get%sIDs()", fieldName))
	g.addLine("\tif len(ids) == 0 {")
	g.addLine("\t\treturn nil, nil")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn %sProvider.GetByIDs(ctx, ids...)", refName))
	g.addLine("}")
	g.addLine("")

	g.filedIndex++
}
