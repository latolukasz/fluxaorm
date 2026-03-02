package fluxaorm

import "github.com/puzpuzpuz/xsync/v2"

// DirtyStreamInsert is the flush type for INSERT operations.
const DirtyStreamInsert uint8 = 1

// DirtyStreamUpdate is the flush type for UPDATE operations.
const DirtyStreamUpdate uint8 = 2

// DirtyStreamDelete is the flush type for DELETE operations.
const DirtyStreamDelete uint8 = 3

// DirtyFieldChange holds old and new values for a changed field.
type DirtyFieldChange struct {
	Old AsyncSQLParam `msgpack:"o"`
	New AsyncSQLParam `msgpack:"n"`
}

// OldValue returns the old value as a Go type.
func (c DirtyFieldChange) OldValue() any { return c.Old.Value() }

// NewValue returns the new value as a Go type.
func (c DirtyFieldChange) NewValue() any { return c.New.Value() }

// DirtyStreamEvent is the payload published to dirty streams.
type DirtyStreamEvent struct {
	EntityType string                      `msgpack:"et"`
	EntityID   uint64                      `msgpack:"id"`
	FlushType  uint8                       `msgpack:"ft"`
	Changes    map[string]DirtyFieldChange `msgpack:"ch,omitempty"`
}

func (orm *ormImplementation) publishDirtyStreamEvents() error {
	if orm.trackedEntities == nil || orm.trackedEntities.Size() == 0 {
		return nil
	}
	registry := orm.engine.registry
	var eventFlusher EventFlusher
	var flusherErr error

	orm.trackedEntities.Range(func(cacheIndex uint64, value *xsync.MapOf[uint64, Entity]) bool {
		schema, ok := registry.entitySchemasByIndex[cacheIndex]
		if !ok || !schema.hasDirtyStreams {
			return true
		}
		value.Range(func(_ uint64, e Entity) bool {
			flushType, changes := e.PrivateFlushEvent()
			if flushType == 0 {
				return true
			}
			databaseBind := e.PrivateGetDatabaseBind()
			for _, ds := range schema.dirtyStreams {
				publish := false
				switch flushType {
				case 1:
					publish = ds.onInsert
				case 2:
					if ds.onUpdate {
						publish = true
					} else if len(ds.fieldTriggers) > 0 && changes != nil {
						for _, ft := range ds.fieldTriggers {
							if _, ok := changes[ft]; ok {
								publish = true
								break
							}
						}
					}
				case 3:
					publish = ds.onDelete
				}
				if !publish {
					continue
				}
				ev := DirtyStreamEvent{
					EntityType: schema.tableName,
					EntityID:   e.GetID(),
					FlushType:  flushType,
				}
				if flushType == 2 && changes != nil {
					ev.Changes = make(map[string]DirtyFieldChange, len(changes))
					for field, oldVal := range changes {
						newVal := databaseBind[field]
						ev.Changes[field] = DirtyFieldChange{
							Old: convertChangeValue(oldVal),
							New: convertChangeValue(newVal),
						}
					}
				}
				if eventFlusher == nil {
					eventFlusher = orm.GetEventBroker().NewFlusher()
				}
				flusherErr = eventFlusher.Publish(ds.streamName, ev)
				if flusherErr != nil {
					return false
				}
			}
			return true
		})
		return flusherErr == nil
	})
	if flusherErr != nil {
		return flusherErr
	}
	if eventFlusher != nil {
		return eventFlusher.Flush()
	}
	return nil
}
