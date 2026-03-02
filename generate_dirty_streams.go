package fluxaorm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func (g *codeGenerator) generateDirtyStreamsFile(streams map[string]bool) error {
	names := make([]string, 0, len(streams))
	for name := range streams {
		names = append(names, name)
	}
	sort.Strings(names)

	g.addImport("github.com/latolukasz/fluxaorm/v2")

	g.addLine("type dirtyStreamConsumers struct {")
	for _, name := range names {
		g.addLine(fmt.Sprintf("\t%s dirtyStreamHelper", name))
	}
	g.addLine("}")
	g.addLine("")

	g.addLine("type dirtyStreamHelper struct {")
	g.addLine("\tstreamName string")
	g.addLine("}")
	g.addLine("")

	g.addLine("func (d dirtyStreamHelper) ConsumeSingle(ctx fluxaorm.Context) (fluxaorm.EventsConsumer, error) {")
	g.addLine("\treturn ctx.GetEventBroker().ConsumerSingle(ctx, d.streamName)")
	g.addLine("}")
	g.addLine("")

	g.addLine("func (d dirtyStreamHelper) ConsumeMany(ctx fluxaorm.Context) (fluxaorm.EventsConsumer, error) {")
	g.addLine("\treturn ctx.GetEventBroker().ConsumerMany(ctx, d.streamName)")
	g.addLine("}")
	g.addLine("")

	g.addLine("func (d dirtyStreamHelper) Name() string {")
	g.addLine("\treturn d.streamName")
	g.addLine("}")
	g.addLine("")

	g.addLine("var DirtyStreams = dirtyStreamConsumers{")
	for _, name := range names {
		g.addLine(fmt.Sprintf("\t%s: dirtyStreamHelper{streamName: %q},", name, name))
	}
	g.addLine("}")

	// Write file
	filePath := filepath.Join(g.dir, "dirty_streams.go")
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	g.writeToFile(f, "package "+filepath.Base(g.dir)+"\n\n")
	if len(g.imports) > 0 {
		g.writeToFile(f, "import (\n")
		for imp := range g.imports {
			g.writeToFile(f, fmt.Sprintf("\t%q\n", imp))
		}
		g.writeToFile(f, ")\n\n")
	}
	g.writeToFile(f, g.body)
	return nil
}
