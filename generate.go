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
	_, err = f.WriteString(fmt.Sprintf("package %s\n", packageName))
	if err != nil {
		return fmt.Errorf("cannot write to file %s: %w", fileName, err)
	}
	fmt.Println(schema.GetTableName())

	return nil
}
