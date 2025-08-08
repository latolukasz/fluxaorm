package modified

import (
	"github.com/latolukasz/fluxaorm"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testPluginModifiedEntity struct {
	ID                     uint64
	Name                   string
	AddedAtDate            time.Time
	AddedAtDateOptional    *time.Time
	ModifiedAtDate         time.Time
	ModifiedAtDateOptional *time.Time
	AddedAtTime            time.Time  `orm:"time"`
	AddedAtTimeOptional    *time.Time `orm:"time"`
	ModifiedAtTime         time.Time  `orm:"time"`
	ModifiedAtTimeOptional *time.Time `orm:"time"`
	AddedAtIgnored         time.Time  `orm:"ignore"`
}

func TestPlugin(t *testing.T) {
	registry := fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtDate", "ModifiedAtDate"))
	engine := fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})

	now := time.Now().UTC()
	dateManual, _ := time.ParseInLocation(time.DateOnly, "2022-02-03", time.UTC)
	timeManual, _ := time.ParseInLocation(time.DateTime, "2022-02-03 04:05:06", time.UTC)

	entity := fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "a"
	assert.NoError(t, engine.Flush())
	assert.NotNil(t, entity.AddedAtDate)
	assert.Equal(t, entity.AddedAtDate.Format(time.DateOnly), now.Format(time.DateOnly))
	assert.Equal(t, "0001-01-01", entity.ModifiedAtDate.Format(time.DateOnly))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, entity.AddedAtDate.Format(time.DateOnly), now.Format(time.DateOnly))
	assert.Equal(t, "0001-01-01", entity.ModifiedAtDate.Format(time.DateOnly))

	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "a1"
	entity.AddedAtDate = dateManual
	assert.NoError(t, engine.Flush())
	assert.Equal(t, "2022-02-03", entity.AddedAtDate.Format(time.DateOnly))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "2022-02-03", entity.AddedAtDate.Format(time.DateOnly))

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtTime", "ModifiedAtTime"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "b"
	assert.NoError(t, engine.Flush())
	assert.NotNil(t, entity.AddedAtTime)
	assert.Equal(t, entity.AddedAtTime.Format(time.DateTime), now.Format(time.DateTime))
	assert.Equal(t, "0001-01-01 00:00:00", entity.ModifiedAtTime.Format(time.DateTime))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, entity.AddedAtTime.Format(time.DateTime), now.Format(time.DateTime))
	assert.Equal(t, "0001-01-01 00:00:00", entity.ModifiedAtTime.Format(time.DateTime))

	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "b1"
	entity.AddedAtTime = timeManual
	assert.NoError(t, engine.Flush())
	assert.Equal(t, "2022-02-03 04:05:06", entity.AddedAtTime.Format(time.DateTime))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "2022-02-03 04:05:06", entity.AddedAtTime.Format(time.DateTime))

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtTimeOptional", "ModifiedAtTimeOptional"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "d"
	assert.NoError(t, engine.Flush())
	assert.NotNil(t, entity.AddedAtTimeOptional)
	assert.Equal(t, entity.AddedAtTimeOptional.Format(time.DateTime), now.Format(time.DateTime))
	assert.Nil(t, entity.ModifiedAtTimeOptional)
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, entity.AddedAtTimeOptional.Format(time.DateTime), now.Format(time.DateTime))
	assert.Nil(t, entity.ModifiedAtTimeOptional)

	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "d1"
	entity.AddedAtTimeOptional = &timeManual
	assert.NoError(t, engine.Flush())
	assert.Equal(t, "2022-02-03 04:05:06", entity.AddedAtTimeOptional.Format(time.DateTime))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "2022-02-03 04:05:06", entity.AddedAtTimeOptional.Format(time.DateTime))

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtDateOptional", "ModifiedAtDateOptional"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "d"
	assert.NoError(t, engine.Flush())
	assert.NotNil(t, entity.AddedAtDateOptional)
	assert.Equal(t, entity.AddedAtDateOptional.Format(time.DateOnly), now.Format(time.DateOnly))
	assert.Nil(t, entity.ModifiedAtDateOptional)
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, entity.AddedAtDateOptional.Format(time.DateOnly), now.Format(time.DateOnly))
	assert.Nil(t, entity.ModifiedAtDateOptional)

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtTimeOptional", "ModifiedAtTimeOptional"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "D"
	assert.NoError(t, engine.Flush())
	entity = fluxaorm.EditEntity(engine, entity)
	entity.Name = "D1"
	time.Sleep(time.Second)
	assert.NoError(t, engine.Flush())
	later := now.Add(time.Second)
	assert.Equal(t, entity.AddedAtTimeOptional.Format(time.DateTime), now.Format(time.DateTime))
	assert.NotNil(t, entity.ModifiedAtTimeOptional)
	assert.Equal(t, entity.ModifiedAtTimeOptional.Format(time.DateTime), later.Format(time.DateTime))
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, entity.AddedAtTimeOptional.Format(time.DateTime), now.Format(time.DateTime))
	assert.NotNil(t, entity.ModifiedAtTimeOptional)
	assert.Equal(t, entity.ModifiedAtTimeOptional.Format(time.DateTime), later.Format(time.DateTime))

	now = time.Now().UTC()
	time.Sleep(time.Second)
	assert.NoError(t, fluxaorm.EditEntityField(engine, entity, "Name", "g2"))
	assert.NoError(t, engine.Flush())
	later = now.Add(time.Second)
	assert.Equal(t, entity.ModifiedAtTimeOptional.Format(time.DateTime), later.Format(time.DateTime))

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("Invalid", "Invalid"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "e"
	assert.NoError(t, engine.Flush())
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "e", entity.Name)

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("AddedAtIgnored", "AddedAtIgnored"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "f"
	assert.NoError(t, engine.Flush())
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "f", entity.Name)

	registry = fluxaorm.NewRegistry()
	registry.RegisterPlugin(New("Name", "Name"))
	engine = fluxaorm.PrepareTables(t, registry, testPluginModifiedEntity{})
	now = time.Now().UTC()
	entity = fluxaorm.NewEntity[testPluginModifiedEntity](engine)
	entity.Name = "g"
	assert.NoError(t, engine.Flush())
	entity, _ = fluxaorm.GetByID[testPluginModifiedEntity](engine, entity.ID)
	assert.Equal(t, "g", entity.Name)

	fluxaorm.DeleteEntity(engine, entity)
	assert.NoError(t, engine.Flush())

	assert.PanicsWithError(t, "at least one column name must be defined", func() {
		New("", "")
	})
	assert.PanicsWithError(t, "addedAt field 'a' must be public", func() {
		New("a", "b")
	})
	assert.PanicsWithError(t, "modifiedAtField field 'b' must be public", func() {
		New("A", "b")
	})
}
