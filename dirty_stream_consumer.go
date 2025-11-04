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

type DirtyStreamEventHandler func(event *DirtyStreamEvent)

type DirtyStreamConsumer struct {
	eventConsumerBase
	consumer *eventsConsumer
	stream   string
	handler  DirtyStreamEventHandler
}

func NewDirtyStreamConsumer(ctx Context, stream string, handler DirtyStreamEventHandler) *DirtyStreamConsumer {
	c := &DirtyStreamConsumer{}
	c.ctx = ctx.(*ormImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	c.stream = stream
	c.handler = handler
	return c
}

func (r *LogTablesConsumer) DirtyStreamConsumer(ttl time.Duration) {
	r.eventConsumerBase.SetBlockTime(ttl)
}

func (r *DirtyStreamConsumer) Digest(nr, count int) bool {
	r.consumer = r.ctx.GetEventBroker().Consumer(r.ctx, r.stream).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	returnedEvents := make([]*DirtyStreamEvent, 0, count)
	return r.consumer.ConsumeMany(nr, count, func(events []Event) {
		returnedEvents = returnedEvents[:0]
		toACK := 0
		for _, e := range events {
			dirtyEvent := r.handleEvent(e)
			if dirtyEvent == nil {
				continue
			}
			r.handler(dirtyEvent)
			if !dirtyEvent.ack {
				toACK++
			}
			returnedEvents = append(returnedEvents, dirtyEvent)
		}
		if toACK == 0 {
			return
		} else if toACK == 1 {
			returnedEvents[0].ACK()
		} else {
			ids := make([]string, toACK)
			i := 0
			for _, v := range returnedEvents {
				if !v.ack {
					ids[i] = v.event.ID()
					i++
				}
			}
			r.consumer.redis.XAck(r.ctx, r.stream, consumerGroupName, ids...)
			r.consumer.redis.XDel(r.ctx, r.stream, ids...)
		}
	})
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
	schema := r.ctx.Engine().Registry().EntitySchema(entity)
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
