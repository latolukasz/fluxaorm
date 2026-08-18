// Command genboot regenerates test_generate/entities without compiling the
// test binary. TestGenerate imports the package it generates, so a generator
// change that breaks the output also breaks the only way to fix it. This
// program depends on the fixtures alone, so it always runs.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/latolukasz/fluxaorm/v2"
	test_generate "github.com/latolukasz/fluxaorm/v2/test_generate"
)

func main() {
	registry := fluxaorm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", fluxaorm.DefaultPoolCode, &fluxaorm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, fluxaorm.DefaultPoolCode, nil)
	registry.RegisterRedis("localhost:6395", 1, "second", nil)
	registry.RegisterNats([]string{"nats://localhost:9944"}, "nats", nil)
	for _, ref := range test_generate.FixtureCDCStreams() {
		registry.RegisterCDCStream(ref, fluxaorm.CDCStreamOptions{NatsPool: "nats"})
	}
	registry.RegisterEntity(test_generate.FixtureEntities()...)

	engine, err := registry.Validate()
	if err != nil {
		fail(err)
	}
	defer engine.Nats("nats").Close()

	orm := engine.NewContext(context.Background())
	for _, alter := range mustAlters(orm) {
		if err = alter.Exec(orm); err != nil {
			fail(err)
		}
	}
	if err = os.MkdirAll("test_generate/entities", 0755); err != nil {
		fail(err)
	}
	if err = fluxaorm.Generate(engine, "test_generate/entities"); err != nil {
		fail(err)
	}
	fmt.Println("regenerated test_generate/entities")
}

func mustAlters(orm fluxaorm.Context) []fluxaorm.Alter {
	alters, err := fluxaorm.GetAlters(orm)
	if err != nil {
		fail(err)
	}
	return alters
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
