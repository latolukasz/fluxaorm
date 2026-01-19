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

func (ds *DirtyStreamEvent) ACK() error {
	err := ds.event.Ack()
	if err != nil {
		return err
	}
	ds.ack = true
	return nil
}

type DirtyStreamEventHandler func(events []*DirtyStreamEvent)

type DirtyStreamConsumer struct {
	consumer *eventsConsumer
	handler  DirtyStreamEventHandler
}

func NewDirtyStreamConsumerSingle(ctx Context, stream string, handler DirtyStreamEventHandler) (*DirtyStreamConsumer, error) {
	c := &DirtyStreamConsumer{}

	consumer, err := ctx.GetEventBroker().ConsumerSingle(ctx, "dirty_"+stream)
	if err != nil {
		return nil, err
	}
	c.consumer = consumer.(*eventsConsumer)
	c.handler = handler
	return c, nil
}

func NewDirtyStreamConsumerMany(ctx Context, stream string, handler DirtyStreamEventHandler) (*DirtyStreamConsumer, error) {
	c := &DirtyStreamConsumer{}
	consumer, err := ctx.GetEventBroker().ConsumerMany(ctx, "dirty_"+stream)
	if err != nil {
		return nil, err
	}
	c.consumer = consumer.(*eventsConsumer)
	c.handler = handler
	return c, nil
}

func (r *DirtyStreamConsumer) Consume(count int, blockTime time.Duration) error {
	return r.consumer.Consume(count, blockTime, r.eventsHandler(count))
}

func (r *DirtyStreamConsumer) AutoClaim(count int, minIdle time.Duration) error {
	return r.consumer.AutoClaim(count, minIdle, r.eventsHandler(count))
}

func (r *DirtyStreamConsumer) Cleanup() error {
	return r.consumer.Cleanup()
}

func (r *DirtyStreamConsumer) eventsHandler(count int) func(events []Event) error {
	return func(events []Event) error {
		returnedEvents := make([]*DirtyStreamEvent, 0, count)
		returnedEvents = returnedEvents[:0]
		for _, e := range events {
			dirtyEvent, err := r.handleEvent(e)
			if err != nil {
				return err
			}
			if dirtyEvent == nil {
				continue
			}
			returnedEvents = append(returnedEvents, dirtyEvent)
		}
		if len(returnedEvents) == 0 {
			return nil
		}
		r.handler(returnedEvents)
		for _, v := range returnedEvents {
			if !v.ack {
				err := v.ACK()
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (r *DirtyStreamConsumer) handleEvent(event Event) (*DirtyStreamEvent, error) {
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
		return nil, event.Ack()
	}
	entity := event.Tag("entity")
	if entity == "" {
		return nil, event.Ack()
	}
	schema := r.consumer.ctx.Engine().Registry().EntitySchema(entity)
	if schema == nil {
		return nil, event.Ack()
	}
	idAsString := event.Tag("id")
	if idAsString == "" {
		return nil, event.Ack()
	}
	id, err := strconv.ParseUint(idAsString, 10, 64)
	if err != nil || id == 0 {
		return nil, event.Ack()
	}
	var bind Bind
	err = event.Unserialize(&bind)
	if err != nil {
		return nil, err
	}
	if bind == nil {
		return nil, event.Ack()
	}
	return &DirtyStreamEvent{
		EntityName: entity,
		ID:         id,
		Operation:  flashType,
		Bind:       bind,
		event:      event,
	}, nil
}
