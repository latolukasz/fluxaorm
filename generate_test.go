package fluxaorm

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"path/filepath"
)

type generateEntity struct {
	ID   uint64
	Name string
}

func TestGenerate(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), generateEntity{})
	_ = os.MkdirAll("test_output", 0755)
	defer os.RemoveAll("test_output")

	f, _ := os.Create(filepath.Join("test_output", "test_file.txt"))
	_ = f.Close()

	err := Generate(orm.Engine(), "test_output")
	assert.NoError(t, err)

	files, _ := os.ReadDir("test_output")
	assert.Len(t, files, 0, "output directory should be empty")
}
