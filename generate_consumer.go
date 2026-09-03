package fluxaorm

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// generateEntityEventForEntity appends the CDC publisher / event-alias /
// snapshot helper lines into the in-progress entity generation buffer. Called
// from generateCodeForEntity when the entity publishes.
//
// The snapshot type is `map[string]any` keyed by entity column names. Typed
// snapshot structs are a future enhancement.
func (g *codeGenerator) generateEntityEventForEntity(schema *entitySchema, names *entityNames) {
	// An `outbox`-only entity has no consumer to publish to but still needs the
	// snapshot builder, because its outbox row carries the same payload.
	if !schema.cdc && !schema.outbox {
		return
	}

	entityName := names.entityName
	snapshotAfterFn := "snapshot" + entityName + "After"
	snapshotBeforeFn := "snapshot" + entityName + "FromOrigin"
	buildEventFn := "build" + entityName + "DirtyEvent"

	g.addImport("encoding/json")
	g.addImport("time")

	g.addLine(fmt.Sprintf("// %sDirtyEvent is the change envelope emitted for %s. `Before` and `After`", entityName, entityName))
	g.addLine("// are map[string]any keyed by entity column names - Insert sets only After,")
	g.addLine("// Delete sets only Before, Update sets both. Values originate from JSON decode,")
	g.addLine("// so numbers arrive as json.Number; use type-asserting helpers when reading them.")
	g.addLine(fmt.Sprintf("type %sDirtyEvent = fluxaorm.DirtyEvent[map[string]any]", entityName))
	g.addLine("")

	// snapshotAfter helper - overlays pending databaseBind values onto origin
	// values, giving the post-change state for both inserts (origin holds new
	// values since SetX writes directly to origin on new=true) and updates
	// (databaseBind holds changed columns). Type-agnostic so reference columns
	// and complex types work without special-casing.
	g.addLine(fmt.Sprintf("func %s(e *%s) map[string]any {", snapshotAfterFn, entityName))
	g.addLine("\tsnap := map[string]any{\"ID\": e.GetID()}")
	g.addLine("\tbind := e.PrivateGetDatabaseBind()")
	for _, col := range g.snapshotColumnNames(schema) {
		g.addLine(fmt.Sprintf("\tif bind != nil { if v, ok := bind[%q]; ok { snap[%q] = v } else { snap[%q] = e.privateGetOriginalColumnValue(%q) } } else { snap[%q] = e.privateGetOriginalColumnValue(%q) }", col, col, col, col, col, col))
	}
	g.addLine("\treturn snap")
	g.addLine("}")
	g.addLine("")

	// snapshotFromOrigin helper - reads origin values for all columns.
	// Used for Before (Update) and Before (Delete) - the original pre-change state.
	g.addLine(fmt.Sprintf("func %s(e *%s) map[string]any {", snapshotBeforeFn, entityName))
	g.addLine("\tsnap := map[string]any{\"ID\": e.GetID()}")
	for _, col := range g.snapshotColumnNames(schema) {
		g.addLine(fmt.Sprintf("\tsnap[%q] = e.privateGetOriginalColumnValue(%q)", col, col))
	}
	g.addLine("\treturn snap")
	g.addLine("}")
	g.addLine("")

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

	// The publisher registers under the table name; the subject format belongs
	// to fluxaorm, so generated code never spells one out.
	g.addLine("func init() {")
	g.addLine(fmt.Sprintf("\tfluxaorm.RegisterEntityPublisher[%s](%q, %s)", entityName, schema.tableName, buildEventFn))
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

// consumerIdent turns a consumer name like "order-indexer" into the Go
// identifier suffix "OrderIndexer", used for the generated ref and builder type.
func consumerIdent(name ConsumerName) string {
	parts := strings.FieldsFunc(string(name), func(r rune) bool { return r == '-' || r == '_' })
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

// generateConsumersFile emits `consumers.go` with the typed consumer refs, the
// per-task dispatch functions, and one builder file per consumer.
func (g *codeGenerator) generateConsumersFile(reg *engineRegistryImplementation) error {
	consumers := sortedConsumers(reg)
	if len(consumers) == 0 {
		return nil
	}

	aliases := taskPackageAliases(reg)

	g.body = ""
	g.imports = make(map[string]bool)
	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.addLine("// Consumer typed handles. Each pairs a declared consumer name with the")
	g.addLine("// generated builder factory so fluxaorm.NewConsumer resolves the right typed")
	g.addLine("// builder via Go generics.")
	g.addLine("var (")
	for _, consumer := range consumers {
		ident := consumerIdent(consumer.name)
		g.addLine(fmt.Sprintf("\tConsumer%s = fluxaorm.NewConsumerRef[%sBuilder](%q, []fluxaorm.Subject{",
			ident, ident, consumer.name))
		for _, subject := range consumer.subjects {
			g.addLine(fmt.Sprintf("\t\t%q,", subject))
		}
		g.addLine(fmt.Sprintf("\t}, new%sBuilder)", ident))
	}
	g.addLine(")")
	g.addLine("")

	g.addLine("// AllConsumers lists every generated consumer so apps can assert that exactly")
	g.addLine("// one job drains each one.")
	g.addLine("var AllConsumers = []fluxaorm.ConsumerHandle{")
	for _, consumer := range consumers {
		g.addLine(fmt.Sprintf("\tConsumer%s,", consumerIdent(consumer.name)))
	}
	g.addLine("}")
	g.addLine("")

	g.addLine("// ConsumerEntities lists the entity providers each entity consumer declared,")
	g.addLine("// so apps can drive their own per-consumer metadata from the declaration")
	g.addLine("// rather than from a hand-maintained list that silently drifts. Task")
	g.addLine("// consumers are absent: they declare queues, not entities.")
	g.addLine("var ConsumerEntities = map[fluxaorm.ConsumerName][]fluxaorm.EntityProvider{")
	for _, consumer := range consumers {
		if len(consumer.entities) == 0 {
			continue
		}
		providers := make([]string, 0, len(consumer.entities))
		for _, schema := range consumer.entities {
			providers = append(providers, "&"+g.capitalizeFirst(schema.tableName)+"Provider")
		}
		g.addLine(fmt.Sprintf("\tConsumer%s.Name(): {%s},", consumerIdent(consumer.name), strings.Join(providers, ", ")))
	}
	g.addLine("}")

	g.generateDispatchFunctions(reg, aliases)

	if err := g.writeGeneratedFile("consumers.go"); err != nil {
		return err
	}

	for _, consumer := range consumers {
		if err := g.generateConsumerBuilderFile(consumer, aliases); err != nil {
			return err
		}
	}

	return nil
}

// generateDispatchFunctions emits one Dispatch<Task> per registered task, so
// application code names the task struct and never a subject or a string.
func (g *codeGenerator) generateDispatchFunctions(
	reg *engineRegistryImplementation, aliases map[string]string,
) {
	tasks := sortedTasks(reg)
	if len(tasks) == 0 {
		return
	}

	for _, schema := range tasks {
		g.addImportAs(schema.pkgPath, aliases[schema.pkgPath])
		qualified := taskTypeRef(schema, aliases)

		g.addLine("")
		g.addLine(fmt.Sprintf(
			"// Dispatch%s publishes a %s task onto the %q queue and returns the ID of its",
			schema.name, schema.name, schema.queue))
		g.addLine("// job_runs row.")
		g.addLine("//")
		g.addLine("// It fails rather than publishing when called inside a transaction: the publish")
		g.addLine("// cannot roll back with it, so the task would run against data that never")
		g.addLine("// existed. Dispatch after the commit.")
		g.addLine(fmt.Sprintf("func Dispatch%s(", schema.name))
		g.addLine("	orm fluxaorm.Context,")
		g.addLine(fmt.Sprintf("	task *%s,", qualified))
		g.addLine("	opts ...fluxaorm.DispatchOption,")
		g.addLine(") (uint64, error) {")
		g.addLine("	return fluxaorm.DispatchTask(orm, task, opts...)")
		g.addLine("}")
	}
}

// sortedTasks lists every registered task by name, so generated output is
// stable across runs.
func sortedTasks(reg *engineRegistryImplementation) []*taskSchema {
	out := make([]*taskSchema, 0, len(reg.tasks))
	for _, schema := range reg.tasks {
		out = append(out, schema)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })

	return out
}

// taskPackageAliases assigns an import alias to every task package whose base
// path segment collides with another's, or with the generated package itself.
// The package identifier comes from reflect, not from the path, because any
// /vN module path makes the last segment wrong.
func taskPackageAliases(reg *engineRegistryImplementation) map[string]string {
	aliases := make(map[string]string)
	taken := map[string]bool{"fluxaorm": true}

	for _, schema := range sortedTasks(reg) {
		if _, has := aliases[schema.pkgPath]; has {
			continue
		}
		name := schema.pkgName
		if name == "" {
			name = path.Base(schema.pkgPath)
		}
		alias := name
		for i := 2; taken[alias]; i++ {
			alias = fmt.Sprintf("%s%d", name, i)
		}
		taken[alias] = true
		aliases[schema.pkgPath] = alias
	}

	return aliases
}

// taskTypeRef renders the qualified type name generated code uses.
func taskTypeRef(schema *taskSchema, aliases map[string]string) string {
	return aliases[schema.pkgPath] + "." + schema.t.Name()
}

// generateConsumerBuilderFile emits `<consumer>_consumer.go`: the typed builder
// and one OnX / OnXBatch pair per entity the consumer declared.
//
// Only declared entities get methods, so subscribing to something the consumer
// never declared is a compile error rather than a handler that never fires.
func (g *codeGenerator) generateConsumerBuilderFile(
	consumer *resolvedConsumer, aliases map[string]string,
) error {
	ident := consumerIdent(consumer.name)
	builderTypeName := ident + "Builder"
	factoryFn := "new" + ident + "Builder"

	g.body = ""
	g.imports = make(map[string]bool)
	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.addLine(fmt.Sprintf("// %s is the typed builder for the %q consumer.", builderTypeName, consumer.name))
	g.addLine(fmt.Sprintf("// Returned by fluxaorm.NewConsumer(engine, Consumer%s).", ident))
	g.addLine(fmt.Sprintf("type %s struct {", builderTypeName))
	g.addLine("\t*fluxaorm.ConsumerBuilder")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("// %s is the factory closure stored in the ConsumerRef; users never call it directly.", factoryFn))
	g.addLine(fmt.Sprintf("func %s(core *fluxaorm.ConsumerBuilder) *%s {", factoryFn, builderTypeName))
	g.addLine(fmt.Sprintf("\treturn &%s{ConsumerBuilder: core}", builderTypeName))
	g.addLine("}")
	g.addLine("")

	for _, schema := range consumer.tasks {
		g.addImportAs(schema.pkgPath, aliases[schema.pkgPath])
		qualified := taskTypeRef(schema, aliases)
		methodName := "On" + string(schema.name)

		g.addLine(fmt.Sprintf("// %s registers the handler for the %s task on the %q consumer.", methodName, schema.name, consumer.name))
		g.addLine("// Returning nil acks the task; returning err retries it with backoff up to its")
		g.addLine("// attempt cap, after which it is terminated and its run row marked failed.")
		g.addLine(fmt.Sprintf("func (b *%s) %s(", builderTypeName, methodName))
		g.addLine(fmt.Sprintf("	handler func(ctx fluxaorm.Context, task *%s) error,", qualified))
		g.addLine(fmt.Sprintf(") *%s {", builderTypeName))
		g.addLine(fmt.Sprintf("	b.AddDispatch(%q, fluxaorm.BuildTaskDispatch(handler))", schema.name))
		g.addLine("	return b")
		g.addLine("}")
		g.addLine("")
	}

	for _, schema := range consumer.entities {
		entityName := g.capitalizeFirst(schema.tableName)
		methodName := "On" + entityName

		g.addLine(fmt.Sprintf("// %s registers a typed handler for %s changes on the %q consumer.", methodName, entityName, consumer.name))
		g.addLine("// Returning nil from the handler acks the message; returning err redelivers it.")
		g.addLine(fmt.Sprintf("func (b *%s) %s(", builderTypeName, methodName))
		g.addLine(fmt.Sprintf("\thandler func(ctx fluxaorm.Context, ev *%sDirtyEvent) error,", entityName))
		g.addLine("\topts ...fluxaorm.HandlerOption,")
		g.addLine(fmt.Sprintf(") *%s {", builderTypeName))
		g.addLine(fmt.Sprintf("\tb.AddDispatch(%q, fluxaorm.BuildDispatch[map[string]any](handler, opts...))", entityName))
		g.addLine("\treturn b")
		g.addLine("}")
		g.addLine("")

		batchMethodName := methodName + "Batch"
		g.addLine(fmt.Sprintf("// %s registers a batch handler for %s changes on the %q consumer.", batchMethodName, entityName, consumer.name))
		g.addLine(fmt.Sprintf("// Every %s message in one fetched batch is passed together. Returning nil acks", entityName))
		g.addLine("// them all; returning err redelivers them all, so the handler must be idempotent.")
		g.addLine(fmt.Sprintf("// Registering both %s and %s for the same entity panics at Build().", methodName, batchMethodName))
		g.addLine(fmt.Sprintf("func (b *%s) %s(", builderTypeName, batchMethodName))
		g.addLine(fmt.Sprintf("\thandler func(ctx fluxaorm.Context, evs []*%sDirtyEvent) error,", entityName))
		g.addLine("\topts ...fluxaorm.HandlerOption,")
		g.addLine(fmt.Sprintf(") *%s {", builderTypeName))
		g.addLine(fmt.Sprintf("\tb.AddBatchDispatch(%q, fluxaorm.BuildBatchDispatch[map[string]any](handler, opts...))", entityName))
		g.addLine("\treturn b")
		g.addLine("}")
		g.addLine("")
	}

	return g.writeGeneratedFile(string(consumer.name) + "_consumer.go")
}

// writeGeneratedFile flushes the current buffer into one generated file.
func (g *codeGenerator) writeGeneratedFile(name string) error {
	filePath := filepath.Join(g.dir, name)
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
