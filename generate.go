package fluxaorm

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func Generate(engine Engine, outputDirectory string) error {

	if strings.TrimSpace(outputDirectory) == "" {
		return fmt.Errorf("output directory is empty")
	}

	info, err := os.Stat(outputDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("output directory does not exist: %s", outputDirectory)
		}
		return fmt.Errorf("cannot access output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory: %s", outputDirectory)
	}

	f, err := os.CreateTemp(outputDirectory, ".fluxaorm-writecheck-*")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s: %w", outputDirectory, err)
	}
	tmp := f.Name()
	_ = f.Close()
	_ = os.Remove(tmp)

	files, err := os.ReadDir(outputDirectory)
	if err != nil {
		return fmt.Errorf("cannot read output directory: %w", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			err = os.Remove(filepath.Join(outputDirectory, file.Name()))
			if err != nil {
				return fmt.Errorf("cannot remove file %s: %w", file.Name(), err)
			}
		}
	}

	for _, schema := range engine.Registry().Entities() {
		err = generateCodeForEntity(engine, schema.(*entitySchema), outputDirectory)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateCodeForEntity(engine Engine, schema *entitySchema, dir string) error {

	packageName := filepath.Base(dir)
	fileName := path.Join(dir, fmt.Sprintf("%s.go", schema.GetTableName()))
	f, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("cannot create file %s: %w", fileName, err)
	}
	defer func() {
		_ = f.Close()
	}()

	entityName := schema.GetTableName()

	addLine(f, fmt.Sprintf("package %s", packageName))
	addLine(f, "")
	addLine(f, fmt.Sprintf("type %s struct {", entityName))
	addLine(f, "\tid uint64")
	addLine(f, "}")
	addLine(f, "")
	addLine(f, fmt.Sprintf("func (e *%s) GetID() uint64 {", entityName))
	addLine(f, "\treturn 0")
	addLine(f, "}")
	addLine(f, "")
	addLine(f, fmt.Sprintf("func (e *%s) SetID(id uint64) {", entityName))
	addLine(f, "}")
	addLine(f, "")
	generateGettersSetters(f, entityName, schema, schema.fields)
	return nil
}

func generateGettersSetters(f *os.File, entityName string, schema *entitySchema, fields *tableFields) {
	for _, i := range fields.uIntegers {
		fieldName := fields.prefix + fields.fields[i].Name
		if fieldName == "ID" {
			continue
		}
		addLine(f, fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "\treturn 0")
		addLine(f, "}")
		addLine(f, "")
		addLine(f, fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "}")
		addLine(f, "")
	}
	for _, i := range fields.integers {
		fieldName := fields.prefix + fields.fields[i].Name
		addLine(f, fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "\treturn 0")
		addLine(f, "}")
		addLine(f, "")
		addLine(f, fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "}")
		addLine(f, "")
	}
	for _, i := range fields.uIntegersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		addLine(f, fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "\treturn nil")
		addLine(f, "}")
		addLine(f, "")
		addLine(f, fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "}")
		addLine(f, "")
	}
	for _, i := range fields.integersNullable {
		fieldName := fields.prefix + fields.fields[i].Name
		addLine(f, fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "\treturn nil")
		addLine(f, "}")
		addLine(f, "")
		addLine(f, fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "}")
		addLine(f, "")
	}
	for _, i := range fields.strings {
		fieldName := fields.prefix + fields.fields[i].Name
		addLine(f, fmt.Sprintf("func (e *%s) Get%s() %s {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "\treturn \"\"")
		addLine(f, "}")
		addLine(f, "")
		addLine(f, fmt.Sprintf("func (e *%s) Set%s(value %s) {", entityName, fieldName, fields.fields[i].Type.String()))
		addLine(f, "}")
		addLine(f, "")
	}
}

func addLine(f *os.File, line string) {
	_, _ = f.WriteString(line + "\n")
}
