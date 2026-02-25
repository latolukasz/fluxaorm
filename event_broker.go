package fluxaorm

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shamaton/msgpack"
)

const consumerGroupName = "consumer_group"

type Event interface {
	Ack() error
	ID() string
	Tag(key string) (value string)
	Unserialize(val interface{}) error
}

type event struct {
	consumer *eventsConsumer
	message  redis.XMessage
	stream   string
	ack      bool
}

func (ev *event) Ack() error {
	if ev.ack {
		return nil
	}
	_, err := ev.consumer.redis.XAck(ev.consumer.ctx, ev.stream, ev.consumer.group, ev.message.ID)
	if err != nil {
		return err
	}
	_, err = ev.consumer.redis.XDel(ev.consumer.ctx, ev.stream, ev.message.ID)
	if err != nil {
		return err
	}
	ev.ack = true
	return nil
}

func (ev *event) ID() string {
	return ev.message.ID
}

func (ev *event) Tag(key string) (value string) {
	val, has := ev.message.Values[key]
	if has {
		return val.(string)
	}
	return ""
}

func (ev *event) Unserialize(value interface{}) error {
	val := ev.message.Values["s"]
	return msgpack.Unmarshal([]byte(val.(string)), &value)
}

type EventBroker interface {
	Publish(stream string, body interface{}, meta ...string) (id string, err error)
	ConsumerSingle(ctx Context, stream string) (EventsConsumer, error)
	ConsumerMany(ctx Context, stream string) (EventsConsumer, error)
	NewFlusher() EventFlusher
	GetStreamsStatistics(stream ...string) ([]*RedisStreamStatistics, error)
	GetStreamStatistics(stream string) (*RedisStreamStatistics, error)
}

type EventFlusher interface {
	Publish(stream string, body interface{}, meta ...string) error
	Flush() error
}

type eventFlusher struct {
	eb     *eventBroker
	events map[string][][]string
}

type eventBroker struct {
	ctx *ormImplementation
}

func createEventSlice(body any, meta []string) ([]string, error) {
	if body == nil {
		return meta, nil
	}
	asString, err := msgpack.Marshal(body)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(meta)+2)
	values[0] = "s"
	values[1] = string(asString)
	for k, v := range meta {
		values[k+2] = v
	}
	return values, nil
}

func (ef *eventFlusher) Publish(stream string, body interface{}, meta ...string) error {
	e, err := createEventSlice(body, meta)
	if err != nil {
		return err
	}
	ef.events[stream] = append(ef.events[stream], e)
	return nil
}

func (ef *eventFlusher) Flush() error {
	grouped := make(map[RedisCache]map[string][][]string)
	for stream, events := range ef.events {
		r, err := getRedisForStream(ef.eb.ctx, stream)
		if err != nil {
			return err
		}
		if grouped[r] == nil {
			grouped[r] = make(map[string][][]string)
		}
		grouped[r][stream] = events
	}
	for r, events := range grouped {
		p := ef.eb.ctx.RedisPipeLine(r.GetCode())
		for stream, list := range events {
			for _, e := range list {
				p.XAdd(stream, e)
			}
		}
		_, err := p.Exec(ef.eb.ctx)
		if err != nil {
			return err
		}
	}
	ef.events = make(map[string][][]string)
	return nil
}

func (orm *ormImplementation) GetEventBroker() EventBroker {
	orm.mutexData.Lock()
	defer orm.mutexData.Unlock()
	if orm.eventBroker == nil {
		orm.eventBroker = &eventBroker{ctx: orm}
	}
	return orm.eventBroker
}

func (eb *eventBroker) NewFlusher() EventFlusher {
	return &eventFlusher{eb: eb, events: make(map[string][][]string)}
}

func (eb *eventBroker) Publish(stream string, body interface{}, meta ...string) (id string, err error) {
	r, err := getRedisForStream(eb.ctx, stream)
	if err != nil {
		return "", err
	}
	values, err := createEventSlice(body, meta)
	if err != nil {
		return "", err
	}
	return r.xAdd(eb.ctx, stream, values)
}

func getRedisForStream(orm *ormImplementation, stream string) (RedisCache, error) {
	pool, has := orm.engine.registry.redisStreamPools[stream]
	if !has {
		return nil, fmt.Errorf("unregistered stream %s", stream)
	}
	return orm.Engine().Redis(pool), nil
}

type EventConsumerHandler func([]Event) error

type EventsConsumer interface {
	Consume(count int, blockTime time.Duration, handler EventConsumerHandler) error
	AutoClaim(count int, minIdle time.Duration, handler EventConsumerHandler) error
	Cleanup() error
	Name() string
}

func (eb *eventBroker) ConsumerSingle(ctx Context, stream string) (EventsConsumer, error) {
	r, err := getRedisForStream(eb.ctx, stream)
	if err != nil {
		return nil, err
	}
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{name: "consumer-single", lastID: "0", firstRun: true, many: false, ctx: ctx.(*ormImplementation)},
		redis:             r,
		stream:            stream,
		group:             consumerGroupName}, nil
}

func (eb *eventBroker) ConsumerMany(ctx Context, stream string) (EventsConsumer, error) {
	r, err := getRedisForStream(eb.ctx, stream)
	if err != nil {
		return nil, err
	}
	var nr uint64
	err = binary.Read(rand.Reader, binary.LittleEndian, &nr)
	if err != nil {
		return nil, err
	}
	name := "consumer-" + time.Now().UTC().Format("2006_01_02_15_04_05") + "-" + strconv.FormatUint(nr, 10)
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{name: name, firstRun: true, lastID: "0", many: true, ctx: ctx.(*ormImplementation)},
		redis:             r,
		stream:            stream,
		group:             consumerGroupName}, nil
}

type eventConsumerBase struct {
	ctx         *ormImplementation
	name        string
	many        bool
	firstRun    bool
	initialized bool
	lastID      string
}

type eventsConsumer struct {
	eventConsumerBase
	redis  RedisCache
	stream string
	group  string
}

func (b *eventConsumerBase) Name() string {
	return b.name
}

func (r *eventsConsumer) Cleanup() error {
	_, err := r.redis.XGroupDelConsumer(r.ctx, r.stream, r.group, r.name)
	return err
}

func (r *eventsConsumer) Consume(count int, blockTime time.Duration, handler EventConsumerHandler) error {
	if r.firstRun {
		err := r.consume(count, blockTime, handler)
		if err != nil {
			return err
		}
	}
	return r.consume(count, blockTime, handler)
}

func (r *eventsConsumer) initIfNeeded() error {
	if !r.initialized {
		_, _, err := r.redis.XGroupCreateMkStream(r.ctx, r.stream, r.group, "0")
		if err != nil {
			return err
		}
		r.initialized = true
	}
	return nil
}

func (r *eventsConsumer) consume(count int, blockTime time.Duration, handler EventConsumerHandler) error {
	err := r.initIfNeeded()
	if err != nil {
		return err
	}
	attributes := &consumeAttributes{
		BlockTime: blockTime,
		Count:     count,
		Handler:   handler,
	}
	return r.digest(attributes)
}

type consumeAttributes struct {
	BlockTime time.Duration
	Stop      chan bool
	Count     int
	Handler   EventConsumerHandler
}

func (r *eventsConsumer) AutoClaim(count int, minIdle time.Duration, handler EventConsumerHandler) error {
	err := r.initIfNeeded()
	if err != nil {
		return err
	}
	a := &redis.XAutoClaimArgs{
		Stream:   r.stream,
		Group:    r.group,
		Consumer: r.name,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}
	for {
		messages, _, err := r.redis.XAutoClaim(r.ctx, a)
		if err != nil {
			return err
		}
		if len(messages) > 0 {
			events := make([]Event, len(messages))
			i := 0
			for _, message := range messages {
				events[i] = &event{stream: r.stream, message: message, consumer: r}
				i++
			}
			err = r.digestEvents(r.ctx, handler, events)
			if err != nil {
				return err
			}
		}
		if len(messages) < 1000 {
			return nil
		}
	}
}

func (r *eventsConsumer) digest(attributes *consumeAttributes) error {
	lastID := ">"
	block := attributes.BlockTime
	if r.firstRun {
		lastID = r.lastID
		block = -1
	}
	a := &redis.XReadGroupArgs{Consumer: r.name, Group: r.group, Streams: []string{r.stream, lastID},
		Count: int64(attributes.Count), Block: block}
	results, err := r.redis.XReadGroup(r.ctx, a)
	if err != nil {
		return err
	}
	totalMessages := 0
	for _, row := range results {
		l := len(row.Messages)
		if l > 0 {
			totalMessages += l
			if r.firstRun {
				r.lastID = row.Messages[l-1].ID
			}
		}
	}
	if totalMessages == 0 {
		r.firstRun = false
	}
	events := make([]Event, totalMessages)
	i := 0
	for _, row := range results {
		for _, message := range row.Messages {
			events[i] = &event{stream: row.Stream, message: message, consumer: r}
			i++
		}
	}
	return r.digestEvents(r.ctx, attributes.Handler, events)
}

func (r *eventsConsumer) digestEvents(ctx Context, handler EventConsumerHandler, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	err := handler(events)
	if err != nil {
		return err
	}
	var toAck map[string][]string
	for _, ev := range events {
		ev := ev.(*event)
		if !ev.ack {
			if toAck == nil {
				toAck = make(map[string][]string)
			}
			toAck[ev.stream] = append(toAck[ev.stream], ev.message.ID)
		}
	}
	for stream, ids := range toAck {
		_, err := r.redis.XAck(ctx, stream, r.group, ids...)
		if err != nil {
			return err
		}
		_, err = r.redis.XDel(ctx, stream, ids...)
		if err != nil {
			return err
		}
	}
	return nil
}

