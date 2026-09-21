package fluxaorm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestCodeGenerator() *codeGenerator {
	return &codeGenerator{
		enums:   map[string]bool{},
		imports: map[string]bool{},
	}
}

// TestGeneratedNullableSetGetterReadsTheNullString: the pending value in
// databaseBind is the sql.NullString the setter bound, so the getter must split
// its String field. Asserting v.(string) on it panicked at runtime.
func TestGeneratedNullableSetGetterReadsTheNullString(t *testing.T) {
	for _, redis := range []bool{false, true} {
		g := newTestCodeGenerator()
		g.createGetterSetterSetNullable(&entitySchema{hasRedisCache: redis}, "TestSetOptional", "generateEntity", "enums.TestEnum", "GenerateEntityProvider")
		require.Contains(t, g.body, "sliced := strings.Split(vNullable.String, \",\")")
		require.NotContains(t, g.body, "v.(string)", "nullable getter asserts the bind value as string, but it is a sql.NullString")
	}
}

// TestGeneratedNullableGettersNeverAssertBindValueAsPlainType: every nullable
// field binds a sql.Null* value, so no nullable getter may type-assert the
// databaseBind entry to the underlying Go type.
func TestGeneratedNullableGettersNeverAssertBindValueAsPlainType(t *testing.T) {
	for _, redis := range []bool{false, true} {
		g := newTestCodeGenerator()
		schema := &entitySchema{hasRedisCache: redis}
		g.createGetterSetterUint64Nullable(schema, "AgeNullable", "generateEntity", "", "GenerateEntityProvider")
		g.createGetterSetterInt64Nullable(schema, "BalanceNullable", "generateEntity", "", "GenerateEntityProvider")
		g.createGetterSetterStringNullable(schema, "Comment", "generateEntity", "GenerateEntityProvider")
		g.createGetterSetterTimeNullable(schema, "TimeNullable", "generateEntity", "GenerateEntityProvider", false)
		g.createGetterSetterTimeNullable(schema, "DateNullable", "generateEntity", "GenerateEntityProvider", true)
		g.createGetterSetterBoolNullable(schema, "BoolNullable", "generateEntity", "GenerateEntityProvider")
		g.createGetterSetterFloatNullable(schema, "FloatNullable", "generateEntity", "GenerateEntityProvider", 2, 8)
		g.createGetterSetterSetNullable(schema, "TestSetOptional", "generateEntity", "enums.TestEnum", "GenerateEntityProvider")
		g.createGetterSetterBytesNullable(schema, "Byte", "generateEntity", "GenerateEntityProvider")
		g.createGetterSetterJsonStruct(schema, "JsonAddress", "generateEntity", "GenerateEntityProvider", "models.Address")
		g.createGetterSetterEnumNullable(schema, "TestEnumOptional", "generateEntity", "enums.TestEnum", "GenerateEntityProvider")

		for _, line := range strings.Split(g.body, "\n") {
			idx := strings.Index(line, "v.(")
			if idx == -1 {
				continue
			}
			require.Contains(t, line[idx:], "v.(sql.Null", "nullable getter asserts the bind value as a non-nullable type: %s", strings.TrimSpace(line))
		}
	}
}
