package fluxaorm

import (
	"context"
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
	Consume(count int, handler EventConsumerHandler) bool
	ConsumeMany(nr, count int, handler EventConsumerHandler) bool
	Claim(from, to int)
	DisableBlockMode()
	SetBlockTime(ttl time.Duration)
}

func (eb *eventBroker) Consumer(ctx Context, stream string) EventsConsumer {
	r := getRedisForStream(eb.ctx, stream)
	return &eventsConsumer{
		eventConsumerBase: eventConsumerBase{ctx: ctx.(*ormImplementation), block: true, blockTime: time.Second * 5},
		redis:             r,
		stream:            stream,
		group:             consumerGroupName,
		lockTTL:           time.Second * 90,
		lockTick:          time.Minute}
}

type eventConsumerBase struct {
	ctx       *ormImplementation
	block     bool
	blockTime time.Duration
}

type eventsConsumer struct {
	eventConsumerBase
	redis           RedisCache
	stream          string
	group           string
	lockTTL         time.Duration
	lockTick        time.Duration
	garbageLastTick int64
}

func (b *eventConsumerBase) DisableBlockMode() {
	b.block = false
}

func (b *eventConsumerBase) SetBlockTime(ttl time.Duration) {
	b.blockTime = ttl
}

func (r *eventsConsumer) Consume(count int, handler EventConsumerHandler) bool {
	return r.ConsumeMany(1, count, handler)
}

func (r *eventsConsumer) ConsumeMany(nr, count int, handler EventConsumerHandler) bool {
	return r.consume(r.getName(nr), count, handler)
}

func (r *eventsConsumer) consume(name string, count int, handler EventConsumerHandler) (finished bool) {
	lockKey := r.group + "_" + name
	locker := r.redis.GetLocker()
	lock, has := locker.Obtain(r.ctx, lockKey, r.lockTTL, 0)
	if !has {
		return false
	}
	timer := time.NewTimer(r.lockTick)
	defer func() {
		lock.Release(r.ctx)
		timer.Stop()
	}()
	r.redis.XGroupCreateMkStream(r.ctx, r.stream, r.group, "0")
	attributes := &consumeAttributes{
		Pending:   true,
		BlockTime: -1,
		Name:      name,
		Count:     count,
		Handler:   handler,
		LastID:    "0",
	}
	ctx := r.ctx.CloneWithContext(context.Background())
	for {
		select {
		case <-r.ctx.Context().Done():
			return true
		case <-timer.C:
			if !lock.Refresh(ctx, time.Second) {
				return false
			}
			timer.Reset(r.lockTick)
		default:
			if r.digest(ctx, attributes) {
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

func (r *eventsConsumer) digest(ctx Context, attributes *consumeAttributes) (stop bool) {
	finished := r.digestKeys(ctx, attributes)
	if !r.block && finished {
		return true
	}
	return false
}

func (r *eventsConsumer) digestKeys(ctx Context, attributes *consumeAttributes) (finished bool) {
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
	return false
}

func (r *eventsConsumer) Claim(from, to int) {
	start := "-"
	for {
		xPendingArg := &redis.XPendingExtArgs{Stream: r.stream, Group: r.group, Start: start, End: "+", Consumer: r.getName(from), Count: 100}
		pending := r.redis.XPendingExt(r.ctx, xPendingArg)
		l := len(pending)
		if l == 0 {
			break
		}
		ids := make([]string, l)
		for i, row := range pending {
			ids[i] = row.ID
		}
		start = r.incrementID(ids[l-1])
		arg := &redis.XClaimArgs{Consumer: r.getName(to), Stream: r.stream, Group: r.group, Messages: ids}
		r.redis.XClaimJustID(r.ctx, arg)
		if l < 100 {
			break
		}
	}
}

func (r *eventsConsumer) getName(nr int) string {
	return "consumer-" + strconv.Itoa(nr)
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return s[0] + "-" + strconv.Itoa(counter+1)
}
