package fluxaorm

import (
	"os"
	"testing"

	"path/filepath"

	"github.com/stretchr/testify/assert"
)

type generateEntity struct {
	ID   uint64
	Name string
}

func TestGenerate(t *testing.T) {
	orm := PrepareTables(t, NewRegistry(), generateEntity{})
	_ = os.MkdirAll("test_output", 0755)

	f, _ := os.Create(filepath.Join("test_output", "test_file.txt"))
	_ = f.Close()

	err := Generate(orm.Engine(), "test_output")
	assert.NoError(t, err)

	_, err = os.Stat(filepath.Join("test_output", "test_file.txt"))
	assert.True(t, os.IsNotExist(err), "test_file.txt should have been deleted")

	content, err := os.ReadFile(filepath.Join("test_output", "generateEntity.go"))
	assert.NoError(t, err)
	assert.Contains(t, string(content), "package test_output\n")

	//_ = os.RemoveAll("test_output")
}
