package fluxaorm

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shamaton/msgpack"
)

const consumerGroupName = "consumer_group"

type Event interface {
	Ack()
	ID() string
	Tag(key string) (value string)
	Unserialize(val interface{})
}

type event struct {
	consumer *eventsConsumer
	message  redis.XMessage
	stream   string
	ack      bool
}

func (ev *event) Ack() {
	if ev.ack {
		return
	}
	ev.consumer.redis.XAck(ev.consumer.ctx, ev.stream, ev.consumer.group, ev.message.ID)
	ev.consumer.redis.XDel(ev.consumer.ctx, ev.stream, ev.message.ID)
	ev.ack = true
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

func (ev *event) Unserialize(value interface{}) {
	val := ev.message.Values["s"]
	err := msgpack.Unmarshal([]byte(val.(string)), &value)
	checkError(err)
}

type EventBroker interface {
	Publish(stream string, body interface{}, meta ...string) (id string)
	Consumer(ctx Context, stream string) EventsConsumer
	NewFlusher() EventFlusher
	GetStreamsStatistics(stream ...string) []*RedisStreamStatistics
	GetStreamStatistics(stream string) *RedisStreamStatistics
}

type EventFlusher interface {
	Publish(stream string, body interface{}, meta ...string)
	Flush()
}

type eventFlusher struct {
	eb     *eventBroker
	events map[string][][]string
}

type eventBroker struct {
	ctx *ormImplementation
}

func createEventSlice(body any, meta []string) []string {
	if body == nil {
		return meta
	}
	asString, err := msgpack.Marshal(body)
	checkError(err)
	values := make([]string, len(meta)+2)
	values[0] = "s"
	values[1] = string(asString)
	for k, v := range meta {
		values[k+2] = v
	}
	return values
}

func (ef *eventFlusher) Publish(stream string, body interface{}, meta ...string) {
	ef.events[stream] = append(ef.events[stream], createEventSlice(body, meta))
}

func (ef *eventFlusher) Flush() {
	grouped := make(map[RedisCache]map[string][][]string)
	for stream, events := range ef.events {
		r := getRedisForStream(ef.eb.ctx, stream)
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
		p.Exec(ef.eb.ctx)
	}
	ef.events = make(map[string][][]string)
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

func (eb *eventBroker) Publish(stream string, body interface{}, meta ...string) (id string) {
	return getRedisForStream(eb.ctx, stream).xAdd(eb.ctx, stream, createEventSlice(body, meta))
}

func getRedisForStream(orm *ormImplementation, stream string) RedisCache {
	pool, has := orm.engine.registry.redisStreamPools[stream]
	if !has {
		panic(fmt.Errorf("unregistered stream %s", stream))
	}
	return orm.Engine().Redis(pool)
}

type EventConsumerHandler func([]Event)

type EventsConsumer interface {
	ConsumeSingle(count int, handler EventConsumerHandler) bool
	ConsumeMany(count int, handler EventConsumerHandler) bool
	DisableBlockMode()
	SetBlockTime(ttl time.Duration)
	SetAutoClaimTime(timeout time.Duration)
	Name() string
}

func (eb *eventBroker) Consumer(ctx Context, stream string) EventsConsumer {
	r := getRedisForStream(eb.ctx, stream)
	var nr uint64
	err := binary.Read(rand.Reader, binary.LittleEndian, &nr)
	if err != nil {
		panic(err)
	}
	name := "consumer-" + time.Now().UTC().Format("2006_01_02_15_04_05") + "-" + strconv.FormatUint(nr, 10)
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{name: name, ctx: ctx.(*ormImplementation), block: true, blockTime: time.Second * 5, autoClaimTime: time.Minute * 10},
		redis:             r,
		stream:            stream,
		group:             consumerGroupName}
}

type eventConsumerBase struct {
	ctx           *ormImplementation
	block         bool
	blockTime     time.Duration
	autoClaimTime time.Duration
	name          string
}

type eventsConsumer struct {
	eventConsumerBase
	redis  RedisCache
	stream string
	group  string
}

func (b *eventConsumerBase) DisableBlockMode() {
	b.block = false
}

func (b *eventConsumerBase) SetBlockTime(ttl time.Duration) {
	b.blockTime = ttl
}

func (b *eventConsumerBase) SetAutoClaimTime(timeout time.Duration) {
	b.autoClaimTime = timeout
}

func (b *eventConsumerBase) Name() string {
	return b.name
}

func (r *eventsConsumer) ConsumeSingle(count int, handler EventConsumerHandler) bool {
	return r.consume(false, r.getName(1), count, handler)
}

func (r *eventsConsumer) ConsumeMany(count int, handler EventConsumerHandler) bool {
	var nr uint64
	err := binary.Read(rand.Reader, binary.LittleEndian, &nr)
	if err != nil {
		panic(err)
	}
	return r.consume(true, r.getName(nr), count, handler)
}

func (r *eventsConsumer) consume(autoClaim bool, name string, count int, handler EventConsumerHandler) (finished bool) {
	ctx := r.ctx.CloneWithContext(context.Background())
	r.redis.XGroupCreateMkStream(r.ctx, r.stream, r.group, "0")
	attributes := &consumeAttributes{
		Pending:   true,
		BlockTime: -1,
		Name:      name,
		Count:     count,
		Handler:   handler,
		LastID:    "0",
	}
	var ticker *time.Ticker
	if autoClaim {
		ticker = time.NewTicker(time.Minute * 5)
	} else {
		ticker = time.NewTicker(time.Hour * 24)
	}
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Context().Done():
			if autoClaim {
				r.redis.XGroupDelConsumer(ctx, r.stream, r.group, name)
			}
			return true
		case <-ticker.C:
			if !autoClaim {
				continue
			}
			r.digestKeys(ctx, attributes, name, true)
		default:
			if r.digest(ctx, attributes, name) {
				if autoClaim {
					r.redis.XGroupDelConsumer(ctx, r.stream, r.group, name)
				}
				return true
			}
		}
	}
}

type consumeAttributes struct {
	Pending   bool
	BlockTime time.Duration
	Stop      chan bool
	Name      string
	Count     int
	Handler   EventConsumerHandler
	LastID    string
}

func (r *eventsConsumer) digest(ctx Context, attributes *consumeAttributes, name string) (stop bool) {
	finished := r.digestKeys(ctx, attributes, name, false)
	if !r.block && finished {
		return true
	}
	return false
}

func (r *eventsConsumer) digestKeys(ctx Context, attributes *consumeAttributes, consumerName string, autoClaim bool) (finished bool) {

	if autoClaim {
		a := &redis.XAutoClaimArgs{
			Stream:   r.stream,
			Group:    r.group,
			Consumer: consumerName,
			MinIdle:  r.autoClaimTime,
			Count:    int64(attributes.Count),
		}
		for {
			messages, _ := r.redis.XAutoClaim(ctx, a)
			if len(messages) > 0 {
				events := make([]Event, len(messages))
				i := 0
				for _, message := range messages {
					events[i] = &event{stream: r.stream, message: message, consumer: r}
					i++
				}
				r.digestEvents(ctx, attributes, events)
			}
			if len(messages) < 100 {
				return true
			}
		}
	} else {
		lastID := ">"
		if attributes.Pending {
			lastID = attributes.LastID
		}
		a := &redis.XReadGroupArgs{Consumer: attributes.Name, Group: r.group, Streams: []string{r.stream, lastID},
			Count: int64(attributes.Count), Block: attributes.BlockTime}
		results := r.redis.XReadGroup(ctx, a)
		totalMessages := 0
		for _, row := range results {
			l := len(row.Messages)
			if l > 0 {
				totalMessages += l
				if attributes.Pending {
					attributes.LastID = row.Messages[l-1].ID
				}
			}
		}
		if totalMessages == 0 {
			if attributes.Pending {
				attributes.Pending = false
				if r.block {
					attributes.BlockTime = r.blockTime
				}
				return false
			}
			return true
		}
		events := make([]Event, totalMessages)
		i := 0
		for _, row := range results {
			for _, message := range row.Messages {
				events[i] = &event{stream: row.Stream, message: message, consumer: r}
				i++
			}
		}
		r.digestEvents(ctx, attributes, events)
	}
	return false
}

func (r *eventsConsumer) digestEvents(ctx Context, attributes *consumeAttributes, events []Event) {
	attributes.Handler(events)
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
		r.redis.XAck(ctx, stream, r.group, ids...)
		r.redis.XDel(ctx, stream, ids...)
	}
}

func (r *eventsConsumer) getName(nr uint64) string {
	if nr == 1 {
		return "consumer-single"
	}
	return r.name
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return s[0] + "-" + strconv.Itoa(counter+1)
}
