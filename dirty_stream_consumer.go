package fluxaorm

import (
	"strconv"
	"time"
)

type DirtyStreamEvent struct {
	EntityName string
	ID         uint64
	Operation  FlushType
	Bind       Bind
	event      Event
	ack        bool
}

func (ds *DirtyStreamEvent) ACK() {
	ds.event.Ack()
	ds.ack = true
}

type DirtyStreamEventHandler func(events []*DirtyStreamEvent)

type DirtyStreamConsumer struct {
	consumer *eventsConsumer
	handler  DirtyStreamEventHandler
}

func NewDirtyStreamConsumerSingle(ctx Context, stream string, handler DirtyStreamEventHandler) *DirtyStreamConsumer {
	c := &DirtyStreamConsumer{}
	c.consumer = ctx.GetEventBroker().ConsumerSingle(ctx, "dirty_"+stream).(*eventsConsumer)
	c.handler = handler
	return c
}

func NewDirtyStreamConsumerMany(ctx Context, stream string, handler DirtyStreamEventHandler) *DirtyStreamConsumer {
	c := &DirtyStreamConsumer{}
	c.consumer = ctx.GetEventBroker().ConsumerMany(ctx, "dirty_"+stream).(*eventsConsumer)
	c.handler = handler
	return c
}

func (r *DirtyStreamConsumer) Consume(count int, blockTime time.Duration) {
	r.consumer.Consume(count, blockTime, r.eventsHandler(count))
}

func (r *DirtyStreamConsumer) AutoClaim(count int, minIdle time.Duration) {
	r.consumer.AutoClaim(count, minIdle, r.eventsHandler(count))
}

func (r *DirtyStreamConsumer) Cleanup() {
	r.consumer.Cleanup()
}

func (r *DirtyStreamConsumer) eventsHandler(count int) func(events []Event) {
	return func(events []Event) {
		returnedEvents := make([]*DirtyStreamEvent, 0, count)
		returnedEvents = returnedEvents[:0]
		for _, e := range events {
			dirtyEvent := r.handleEvent(e)
			if dirtyEvent == nil {
				continue
			}
			returnedEvents = append(returnedEvents, dirtyEvent)
		}
		if len(returnedEvents) == 0 {
			return
		}
		r.handler(returnedEvents)
		for _, v := range returnedEvents {
			if !v.ack {
				v.ACK()
			}
		}
	}
}

func (r *DirtyStreamConsumer) handleEvent(event Event) *DirtyStreamEvent {
	var flashType FlushType
	switch event.Tag("action") {
	case "add":
		flashType = Insert
		break
	case "edit":
		flashType = Update
		break
	case "delete":
		flashType = Delete
		break
	default:
		event.Ack()
		return nil
	}
	entity := event.Tag("entity")
	if entity == "" {
		event.Ack()
		return nil
	}
	schema := r.consumer.ctx.Engine().Registry().EntitySchema(entity)
	if schema == nil {
		event.Ack()
	}
	idAsString := event.Tag("id")
	if idAsString == "" {
		event.Ack()
		return nil
	}
	id, err := strconv.ParseUint(idAsString, 10, 64)
	if err != nil || id == 0 {
		event.Ack()
		return nil
	}
	var bind Bind
	event.Unserialize(&bind)
	if bind == nil {
		event.Ack()
		return nil
	}
	return &DirtyStreamEvent{
		EntityName: entity,
		ID:         id,
		Operation:  flashType,
		Bind:       bind,
		event:      event,
	}
}
