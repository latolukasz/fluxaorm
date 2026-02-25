package fluxaorm

import "fmt"

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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value uint64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value int64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}

func (g *codeGenerator) createGetterSetterBool(schema *entitySchema, fieldName, entityName, providerName string) {
	g.addLine(fmt.Sprintf("func (e *%s) Get%s() bool {", entityName, fieldName))
	g.addLine("\tif !e.new {")
	g.addLine("\t\tif e.databaseBind != nil {")
	g.addLine(fmt.Sprintf("\t\t\tv, hasInDB := e.databaseBind[\"%s\"]", fieldName))
	g.addLine("\t\t\tif hasInDB {")
	g.addLine("\t\t\t\treturn v.(bool)")
	g.addLine("\t\t\t}")
	g.addLine("\t\t}")
	if schema.hasRedisCache {
		g.addLine("\t\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\t\treturn e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\t}")
	}
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn e.originDatabaseValues.F%d;", g.filedIndex))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value bool) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
	g.addLine("\t}")
	if schema.hasRedisCache {
		g.addLine("\tsame:= false")
		g.addLine("\tif e.originRedisValues != nil {")
		g.addLine(fmt.Sprintf("\t\tfromRedis := e.originRedisValues[%d] == \"1\"", g.filedIndex))
		g.addLine("\t\tsame = fromRedis == value")
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\tsame = e.originDatabaseValues.F%d == value", g.filedIndex))
		g.addLine("\t}")
		g.addLine("\tif same {")
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine("\tif value {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"1\")", g.filedIndex+1))
		g.addLine("\t} else {")
		g.addLine(fmt.Sprintf("\t\te.addToRedisBind(%d, \"0\")", g.filedIndex+1))
		g.addLine("\t}")
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value float64) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif math.Round(e.originDatabaseValues.F%d*math.Pow10(%d)) == math.Round(value*math.Pow10(%d)) {", g.filedIndex, precision, precision))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, strconv.FormatFloat(value, 'f', %d, %d))", g.filedIndex+1, precision, size))
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value time.Time) {", entityName, fieldName))
	if dateOnly {
		g.addLine("\tvalue = value.Truncate(time.Hour * 24)")
	} else {
		g.addLine("\tvalue = value.Truncate(time.Second)")
	}
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d.Unix() == value.Unix() {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value.Unix())", g.filedIndex+1))
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value string) {", entityName, fieldName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = value", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == value {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", value)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, value)", g.filedIndex+1))
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, enumName))
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = string(value)", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == string(value) {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", string(value))", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, string(value))", g.filedIndex+1))
	}
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

	g.addLine(fmt.Sprintf("func (e *%s) Set%s(value ...%s) {", entityName, fieldName, setName))
	g.addLine("\tslice := make([]string, len(value))")
	g.addLine("\tfor k, v := range value {")
	g.addLine("\t\tslice[k] = string(v)")
	g.addLine("\t}")
	g.addLine("\tsort.Strings(slice)")
	g.addLine("\tasString := strings.Join(slice, \",\")")
	g.addLine("\tif e.new {")
	g.addLine(fmt.Sprintf("\t\te.originDatabaseValues.F%d = asString", g.filedIndex))
	g.addLine("\t\treturn")
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
		g.addLine(fmt.Sprintf("\t\tdelete(e.redisBind, %d)", g.filedIndex+1))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	} else {
		g.addLine(fmt.Sprintf("\tif e.originDatabaseValues.F%d == asString {", g.filedIndex))
		g.addLine(fmt.Sprintf("\t\tdelete(e.databaseBind, \"%s\")", fieldName))
		g.addLine("\t\treturn")
		g.addLine("\t}")
	}
	g.addLine(fmt.Sprintf("\te.addToDatabaseBind(\"%s\", asString)", fieldName))
	if schema.hasRedisCache {
		g.addLine(fmt.Sprintf("\te.addToRedisBind(%d, asString)", g.filedIndex+1))
	}
	g.addLine("}")
	g.addLine("")
	g.filedIndex++
}
