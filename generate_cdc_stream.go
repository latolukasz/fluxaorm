package fluxaorm

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

// generateCDCStreamForEntity appends CDC publisher / event-alias / snapshot
// helper lines into the in-progress entity generation buffer. Called from
// generateCodeForEntity when the entity has at least one dirty stream tag.
//
// The snapshot type is `map[string]any` keyed by entity column names. Typed
// snapshot structs are a future enhancement.
func (g *codeGenerator) generateCDCStreamForEntity(schema *entitySchema, names *entityNames) {
	if len(schema.dirtyStreams) == 0 {
		return
	}

	entityName := names.entityName
	snapshotAfterFn := "snapshot" + entityName + "After"
	snapshotBeforeFn := "snapshot" + entityName + "FromOrigin"
	buildEventFn := "build" + entityName + "DirtyEvent"

	g.addImport("encoding/json")
	g.addImport("time")

	// Event type alias.
	g.addLine(fmt.Sprintf("// %sDirtyEvent is the CDC envelope emitted for this entity.", entityName))
	g.addLine(fmt.Sprintf("type %sDirtyEvent = fluxaorm.DirtyEvent[map[string]any]", entityName))
	g.addLine("")

	// Stream list (referenced from the init() block).
	streamRefs := make([]string, 0, len(schema.dirtyStreams))
	for _, s := range schema.dirtyStreams {
		streamRefs = append(streamRefs, "Stream"+g.capitalizeStreamName(string(s))+".Name()")
	}

	// snapshotAfter helper — uses typed getters for current state.
	g.addLine(fmt.Sprintf("func %s(e *%s) map[string]any {", snapshotAfterFn, entityName))
	g.addLine("\tsnap := map[string]any{")
	g.addLine(fmt.Sprintf("\t\t\"ID\": e.GetID(),"))
	for _, col := range g.snapshotColumnNames(schema) {
		getter := "Get" + g.capitalizeFirst(col)
		g.addLine(fmt.Sprintf("\t\t%q: e.%s(),", col, getter))
	}
	g.addLine("\t}")
	g.addLine("\treturn snap")
	g.addLine("}")
	g.addLine("")

	// snapshotFromOrigin helper — reads origin values via privateGetOriginalColumnValue.
	g.addLine(fmt.Sprintf("func %s(e *%s) map[string]any {", snapshotBeforeFn, entityName))
	g.addLine("\tsnap := map[string]any{")
	g.addLine(fmt.Sprintf("\t\t\"ID\": e.GetID(),"))
	for _, col := range g.snapshotColumnNames(schema) {
		g.addLine(fmt.Sprintf("\t\t%q: e.privateGetOriginalColumnValue(%q),", col, col))
	}
	g.addLine("\t}")
	g.addLine("\treturn snap")
	g.addLine("}")
	g.addLine("")

	// buildEvent function: dispatch by op and serialize.
	g.addLine(fmt.Sprintf("func %s(e *%s, op fluxaorm.DirtyOp, beforeOrigin map[string]any) ([]byte, error) {", buildEventFn, entityName))
	g.addLine(fmt.Sprintf("\tev := &%sDirtyEvent{Op: op, ID: e.GetID(), TsMs: time.Now().UnixMilli()}", entityName))
	g.addLine("\tswitch op {")
	g.addLine("\tcase fluxaorm.DirtyInsert:")
	g.addLine(fmt.Sprintf("\t\tsnap := %s(e)", snapshotAfterFn))
	g.addLine("\t\tev.After = &snap")
	g.addLine("\tcase fluxaorm.DirtyUpdate:")
	g.addLine(fmt.Sprintf("\t\tsnapBefore := %s(e)", snapshotBeforeFn))
	g.addLine(fmt.Sprintf("\t\tsnapAfter := %s(e)", snapshotAfterFn))
	g.addLine("\t\tev.Before = &snapBefore")
	g.addLine("\t\tev.After = &snapAfter")
	g.addLine("\tcase fluxaorm.DirtyDelete:")
	g.addLine(fmt.Sprintf("\t\tsnapBefore := %s(e)", snapshotBeforeFn))
	g.addLine("\t\tev.Before = &snapBefore")
	g.addLine("\t}")
	g.addLine("\treturn json.Marshal(ev)")
	g.addLine("}")
	g.addLine("")

	// init() block registers the publisher.
	g.addLine("func init() {")
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterDirtyPublisher[%s](", entityName))
	g.addLine(fmt.Sprintf("\t\t[]fluxaorm.NatsStreamName{%s},", strings.Join(streamRefs, ", ")))
	g.addLine(fmt.Sprintf("\t\t%s,", buildEventFn))
	g.addLine("\t)")
	g.addLine("}")
	g.addLine("")
}

// snapshotColumnNames returns the entity's columns excluding "ID" (which is
// emitted explicitly in every snapshot under the same key).
func (g *codeGenerator) snapshotColumnNames(schema *entitySchema) []string {
	cols := make([]string, 0, len(schema.columnNames))
	for _, c := range schema.columnNames {
		if c == "ID" {
			continue
		}
		cols = append(cols, c)
	}
	return cols
}

// capitalizeStreamName transforms a stream name like "order-indexer" or
// "order_indexer" into a Go identifier suffix like "OrderIndexer", used for
// generated `StreamX` variables and per-stream builder types.
func (g *codeGenerator) capitalizeStreamName(streamName string) string {
	parts := strings.FieldsFunc(streamName, func(r rune) bool { return r == '-' || r == '_' })
	for i, p := range parts {
		if p == "" {
			continue
		}
		b := []byte(p)
		if b[0] >= 'a' && b[0] <= 'z' {
			b[0] = b[0] - ('a' - 'A')
		}
		parts[i] = string(b)
	}
	return strings.Join(parts, "")
}

// cdcSchemaInfo bundles an entity with its display name for per-stream builder
// emission. Hoisted to package-level so the slice type matches across helpers.
type cdcSchemaInfo struct {
	entityName string
	schema     *entitySchema
}

// generateDirtyStreamsFile emits `entities/dirty_streams.go` with typed
// CDCStreamRef variables and per-stream typed builder files. Called from
// Generate() after every per-entity file is written. Walks all schemas to
// collect unique streams and the entities tagged into each.
func (g *codeGenerator) generateDirtyStreamsFile(schemas map[reflect.Type]*entitySchema) error {
	// Collect: streamName → []entityName (in deterministic order).
	streamToEntities := make(map[string][]string)
	streamToInfos := make(map[string][]cdcSchemaInfo)

	for _, schema := range schemas {
		if len(schema.dirtyStreams) == 0 {
			continue
		}
		entityName := g.capitalizeFirst(schema.tableName)
		for _, s := range schema.dirtyStreams {
			name := string(s)
			streamToEntities[name] = append(streamToEntities[name], entityName)
			streamToInfos[name] = append(streamToInfos[name], cdcSchemaInfo{entityName: entityName, schema: schema})
		}
	}
	if len(streamToEntities) == 0 {
		return nil
	}
	for name := range streamToEntities {
		sort.Strings(streamToEntities[name])
		sort.Slice(streamToInfos[name], func(i, j int) bool {
			return streamToInfos[name][i].entityName < streamToInfos[name][j].entityName
		})
	}

	// Sort stream names for stable output.
	streamNames := make([]string, 0, len(streamToEntities))
	for name := range streamToEntities {
		streamNames = append(streamNames, name)
	}
	sort.Strings(streamNames)

	// ===== dirty_streams.go: typed refs =====
	g.body = ""
	g.imports = make(map[string]bool)
	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.addLine("// CDC stream typed handles. Each pairs a logical stream name with the")
	g.addLine("// generated per-stream builder factory so fluxaorm.NewCDCConsumer can")
	g.addLine("// resolve the right typed builder via Go generics.")
	g.addLine("var (")
	for _, name := range streamNames {
		ident := g.capitalizeStreamName(name)
		g.addLine(fmt.Sprintf("\tStream%s = fluxaorm.NewCDCStreamRef[%sCDCBuilder](\"%s\", new%sCDCBuilder)", ident, ident, name, ident))
	}
	g.addLine(")")

	filePath := filepath.Join(g.dir, "dirty_streams.go")
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	g.writeToFile(f, "// Code generated by fluxaorm; DO NOT EDIT.\n\n")
	g.writeToFile(f, "package "+filepath.Base(g.dir)+"\n\n")
	g.writeImports(f)
	g.writeToFile(f, g.body)
	_ = f.Close()
	if err := formatFile(filePath); err != nil {
		return err
	}

	// ===== per-stream builder files =====
	for _, name := range streamNames {
		if err := g.generateCDCStreamBuilderFile(name, streamToInfos[name]); err != nil {
			return err
		}
	}
	return nil
}

// generateCDCStreamBuilderFile emits a per-stream `<stream>_cdc.go` file with
// the typed builder struct and one OnX method per entity tagged into the stream.
func (g *codeGenerator) generateCDCStreamBuilderFile(streamName string, infos []cdcSchemaInfo) error {
	ident := g.capitalizeStreamName(streamName)
	builderTypeName := ident + "CDCBuilder"
	factoryFn := "new" + ident + "CDCBuilder"

	g.body = ""
	g.imports = make(map[string]bool)
	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.addLine(fmt.Sprintf("// %s is the typed builder for the %q CDC stream.", builderTypeName, streamName))
	g.addLine("// Returned by fluxaorm.NewCDCConsumer(engine, Stream" + ident + ", opts...).")
	g.addLine(fmt.Sprintf("type %s struct {", builderTypeName))
	g.addLine("\t*fluxaorm.CDCBuilder")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("// %s is the factory closure stored in the CDCStreamRef; users never call it directly.", factoryFn))
	g.addLine(fmt.Sprintf("func %s(core *fluxaorm.CDCBuilder) *%s {", factoryFn, builderTypeName))
	g.addLine(fmt.Sprintf("\treturn &%s{CDCBuilder: core}", builderTypeName))
	g.addLine("}")
	g.addLine("")

	// One OnX method per entity tagged into this stream.
	for _, info := range infos {
		entityName := info.entityName
		methodName := "On" + entityName
		g.addLine(fmt.Sprintf("// %s registers a typed handler for %s change events in the %q stream.", methodName, entityName, streamName))
		g.addLine("// Returning nil from the handler acks the message; returning err redelivers it.")
		g.addLine(fmt.Sprintf("func (b *%s) %s(", builderTypeName, methodName))
		g.addLine(fmt.Sprintf("\thandler func(ctx fluxaorm.Context, ev *%sDirtyEvent) error,", entityName))
		g.addLine("\topts ...fluxaorm.CDCHandlerOption,")
		g.addLine(fmt.Sprintf(") *%s {", builderTypeName))
		g.addLine(fmt.Sprintf("\tb.AddDispatch(\"%s\", fluxaorm.BuildCDCDispatch[map[string]any](handler, opts...))", entityName))
		g.addLine("\treturn b")
		g.addLine("}")
		g.addLine("")
	}

	filePath := filepath.Join(g.dir, streamName+"_cdc.go")
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	g.writeToFile(f, "// Code generated by fluxaorm; DO NOT EDIT.\n\n")
	g.writeToFile(f, "package "+filepath.Base(g.dir)+"\n\n")
	g.writeImports(f)
	g.writeToFile(f, g.body)
	_ = f.Close()
	return formatFile(filePath)
}
