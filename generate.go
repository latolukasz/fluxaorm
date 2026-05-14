package fluxaorm

import (
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type codeGenerator struct {
	engine      *engineImplementation
	dir         string
	enums       map[string]bool
	imports     map[string]bool
	enumsImport string
	body        string
	filedIndex  int
}

type entityNames struct {
	entityName          string
	entityPrivate       string
	providerName        string
	providerNamePrivate string
	sqlRowName          string
}

func Generate(engine Engine, outputDirectory string) error {

	if strings.TrimSpace(outputDirectory) == "" {
		return fmt.Errorf("output directory is empty")
	}

	absOutputDirectory, err := filepath.Abs(outputDirectory)
	if err != nil {
		return fmt.Errorf("cannot get absolute path for output directory: %w", err)
	}
	absOutputDirectory = filepath.Clean(absOutputDirectory)

	info, err := os.Stat(absOutputDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("output directory does not exist: %s", absOutputDirectory)
		}
		return fmt.Errorf("cannot access output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory: %s", absOutputDirectory)
	}

	f, err := os.CreateTemp(absOutputDirectory, ".fluxaorm-writecheck-*")
	if err != nil {
		return fmt.Errorf("directory is not writable: %s: %w", absOutputDirectory, err)
	}
	tmp := f.Name()
	_ = f.Close()
	_ = os.Remove(tmp)

	goModPath, err := findGoMod(absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot find go.mod: %w", err)
	}
	moduleName, err := getModuleName(goModPath)
	if err != nil {
		return fmt.Errorf("cannot get module name from go.mod: %w", err)
	}
	goModDir := filepath.Dir(goModPath)
	relPath, err := filepath.Rel(goModDir, absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot get relative path: %w", err)
	}
	enumsImport := moduleName
	if relPath != "." {
		enumsImport += "/" + filepath.ToSlash(relPath)
	}
	enumsImport += "/enums"

	files, err := os.ReadDir(absOutputDirectory)
	if err != nil {
		return fmt.Errorf("cannot read output directory: %w", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			err = os.Remove(filepath.Join(absOutputDirectory, file.Name()))
			if err != nil {
				return fmt.Errorf("cannot remove file %s: %w", file.Name(), err)
			}
		}
	}

	generator := codeGenerator{engine: engine.(*engineImplementation), dir: absOutputDirectory, enums: nil, enumsImport: enumsImport}

	for _, schema := range engine.Registry().(*engineRegistryImplementation).entitySchemas {
		generator.body = ""
		generator.imports = make(map[string]bool)
		err = generator.generateCodeForEntity(schema)
		if err != nil {
			return err
		}
	}

	// Generate providers registry file
	generator.body = ""
	generator.imports = make(map[string]bool)
	err = generator.generateProvidersFile(engine.Registry().(*engineRegistryImplementation).entitySchemas)
	if err != nil {
		return err
	}

	// Generate dirty_streams.go + per-stream builder files if any entity is tagged.
	err = generator.generateDirtyStreamsFile(engine.Registry().(*engineRegistryImplementation).entitySchemas)
	if err != nil {
		return err
	}

	return nil
}

func findGoMod(dir string) (string, error) {
	dir = filepath.Clean(dir)
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return goModPath, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found")
}

func getModuleName(goModPath string) (string, error) {
	content, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", fmt.Errorf("module name not found in go.mod")
}

func (g *codeGenerator) writeImports(f *os.File) {
	if len(g.imports) == 0 {
		return
	}
	var stdlib, thirdParty []string
	for imp := range g.imports {
		if strings.Contains(strings.Split(imp, "/")[0], ".") {
			thirdParty = append(thirdParty, imp)
		} else {
			stdlib = append(stdlib, imp)
		}
	}
	sort.Strings(stdlib)
	sort.Strings(thirdParty)

	if len(g.imports) == 1 && len(stdlib) == 1 {
		g.writeToFile(f, fmt.Sprintf("import \"%s\"\n\n", stdlib[0]))
		return
	}
	g.writeToFile(f, "import (\n")
	for _, imp := range stdlib {
		g.writeToFile(f, fmt.Sprintf("\t\"%s\"\n", imp))
	}
	if len(stdlib) > 0 && len(thirdParty) > 0 {
		g.writeToFile(f, "\n")
	}
	for _, imp := range thirdParty {
		g.writeToFile(f, fmt.Sprintf("\t\"%s\"\n", imp))
	}
	g.writeToFile(f, ")\n\n")
}

func (g *codeGenerator) addImport(value string) {
	g.imports[value] = true
}

func (g *codeGenerator) addLine(line string) {
	g.body += line + "\n"
}

func (g *codeGenerator) appendToLine(value string) {
	g.body += value
}

func (g *codeGenerator) writeToFile(f *os.File, value string) {
	_, _ = f.WriteString(value)
}

func (g *codeGenerator) capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		b := []byte(part)
		if b[0] >= 'a' && b[0] <= 'z' {
			b[0] = b[0] - ('a' - 'A')
		}
		parts[i] = string(b)
	}
	return strings.Join(parts, "")
}

func (g *codeGenerator) lowerFirst(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		b := []byte(part)
		if i == 0 && b[0] >= 'A' && b[0] <= 'Z' {
			b[0] = b[0] + ('a' - 'A')
		} else if i > 0 && b[0] >= 'a' && b[0] <= 'z' {
			b[0] = b[0] - ('a' - 'A')
		}
		parts[i] = string(b)
	}
	return strings.Join(parts, "")
}

func formatFile(filePath string) error {
	src, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("cannot read file for formatting %s: %w", filePath, err)
	}
	formatted, err := format.Source(src)
	if err != nil {
		return fmt.Errorf("cannot format file %s: %w", filePath, err)
	}
	return os.WriteFile(filePath, formatted, 0644)
}
