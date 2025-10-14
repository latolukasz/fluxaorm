package fluxaorm

import (
	"github.com/go-sql-driver/mysql"
	"slices"
	"strconv"
	"strings"
	"time"
)

const LazyChannelName = "orm-lazy-channel"
const LazyErrorsChannelName = "orm-lazy-errors-channel"
const LogChannelName = "orm-log-channel"
const RedisStreamGarbageCollectorChannelName = "orm-stream-garbage-collector"
const BackgroundConsumerGroupName = "orm-async-consumer"

var mySQLErrorCodesToSkip = []uint16{
	1022, // Can't write; duplicate key in table '%s'
	1048, // Column '%s' cannot be null
	1049, // Unknown database '%s'
	1051, // Unknown table '%s'
	1054, // Unknown column '%s' in '%s'
	1062, // Duplicate entry '%s' for key %d
	1063, // Incorrect column specifier for column '%s'
	1064, // Syntax error
	1067, // Invalid default value for '%s'
	1109, // Message: Unknown table '%s' in %s
	1146, // Table '%s.%s' doesn't exist
	1149, // You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
	2032, // Data truncated
}

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	LogID     uint64
	Meta      map[string]interface{}
	Before    map[string]interface{}
	Changes   map[string]interface{}
	Updated   time.Time
}

type BackgroundConsumer struct {
	eventConsumerBase
	garbageCollectorSha1 string
	consumer             *eventsConsumer
}

func NewBackgroundConsumer(ctx Context) *BackgroundConsumer {
	c := &BackgroundConsumer{}
	c.ctx = ctx.(*ormImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	return c
}

func (r *BackgroundConsumer) SetBlockTime(ttl time.Duration) {
	r.eventConsumerBase.SetBlockTime(ttl)
}

func (r *BackgroundConsumer) Digest() bool {
	r.consumer = r.ctx.GetEventBroker().Consumer(r.ctx, BackgroundConsumerGroupName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(500, func(events []Event) {
		for _, e := range events {
			switch e.Stream() {
			case LazyChannelName:
				r.handleLazyFlush(e)
			case LogChannelName:
				r.handleLogTable(e)
			case RedisStreamGarbageCollectorChannelName:
				r.handleRedisChannelGarbageCollector(e)
			}
		}
	})
}

func (r *BackgroundConsumer) handleLazyFlush(event Event) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
				r.ctx.GetEventBroker().Publish(LazyErrorsChannelName, event)
				return
			}
			panic(rec)
		}
	}()
	var lazyEvent []any
	event.Unserialize(&lazyEvent)
	if lazyEvent == nil || len(lazyEvent) < 2 {
		event.delete()
		return
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		event.delete()
		return
	}
	dbCode, valid := lazyEvent[1].(string)
	if !valid {
		event.delete()
		return
	}
	db := r.ctx.Engine().DB(dbCode)
	if db == nil {
		event.delete()
		return
	}
	if len(lazyEvent) > 2 {
		db.Exec(r.ctx, sql, lazyEvent[2:]...)
	} else {
		db.Exec(r.ctx, sql)
	}
	event.Ack()
}

func (r *BackgroundConsumer) handleLogTable(event Event) {
	defer func() {
		if rec := recover(); rec != nil {
			asMySQLError, isMySQLError := rec.(*mysql.MySQLError)
			if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
				return
			}
			panic(rec)
		}
	}()
	var lazyEvent []any
	event.Unserialize(&lazyEvent)
	if lazyEvent == nil || len(lazyEvent) < 3 {
		event.delete()
		return
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		event.delete()
		return
	}
	dbCode, valid := lazyEvent[len(lazyEvent)-1].(string)
	if !valid {
		event.delete()
		return
	}
	db := r.ctx.Engine().DB(dbCode)
	if db == nil {
		event.delete()
		return
	}
	args := lazyEvent[1 : len(lazyEvent)-1]
	db.Exec(r.ctx, sql, args...)
	event.Ack()
	event.Ack()
}

func (r *BackgroundConsumer) handleRedisChannelGarbageCollector(event Event) {
	garbageEvent := &garbageCollectorEvent{}
	event.Unserialize(garbageEvent)
	redisGarbage := r.ctx.Engine().Redis(garbageEvent.Pool)
	streams := r.ctx.engine.registry.getRedisStreamsForGroup(garbageEvent.Group)
	if !redisGarbage.SetNX(r.ctx, garbageEvent.Group+"_gc", "1", time.Second*30) {
		event.delete()
		return
	}
	def := r.ctx.engine.registry.redisStreamGroups[redisGarbage.GetCode()]
	for _, stream := range streams {
		info := redisGarbage.XInfoGroups(r.ctx, stream)
		ids := make(map[string][]int64)
		for name := range def[stream] {
			ids[name] = []int64{0, 0}
		}
		inPending := false
		for _, group := range info {
			_, has := ids[group.Name]
			if has && group.LastDeliveredID != "" {
				lastDelivered := group.LastDeliveredID
				pending := redisGarbage.XPending(r.ctx, stream, group.Name)
				if pending.Lower != "" {
					lastDelivered = pending.Lower
					inPending = true
				}
				s := strings.Split(lastDelivered, "-")
				id, _ := strconv.ParseInt(s[0], 10, 64)
				ids[group.Name][0] = id
				counter, _ := strconv.ParseInt(s[1], 10, 64)
				ids[group.Name][1] = counter
			}
		}
		minID := []int64{-1, 0}
		for _, id := range ids {
			if id[0] == 0 {
				minID[0] = 0
				minID[1] = 0
			} else if minID[0] == -1 || id[0] < minID[0] || (id[0] == minID[0] && id[1] < minID[1]) {
				minID[0] = id[0]
				minID[1] = id[1]
			}
		}
		if minID[0] == 0 {
			continue
		}
		// TODO check of redis 6.2 and use trim with minid
		var end string
		if inPending {
			if minID[1] > 0 {
				end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1]-1, 10)
			} else {
				end = strconv.FormatInt(minID[0]-1, 10)
			}
		} else {
			end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1], 10)
		}

		if r.garbageCollectorSha1 == "" {
			r.setGCScript(redisGarbage)
		}

		for {
			res, exists := redisGarbage.EvalSha(r.ctx, r.garbageCollectorSha1, []string{stream}, end)
			if !exists {
				r.setGCScript(redisGarbage)
				res, _ = redisGarbage.EvalSha(r.ctx, r.garbageCollectorSha1, []string{stream}, end)
			}
			if res == int64(1) {
				break
			}
		}
	}
	event.delete()
}

func (r *BackgroundConsumer) setGCScript(redisGarbage RedisCache) {
	script := `
						local count = 0
						local all = 0
						while(true)
						do
							local T = redis.call('XRANGE', KEYS[1], "-", ARGV[1], "COUNT", 1000)
							local ids = {}
							for _, v in pairs(T) do
								table.insert(ids, v[1])
								count = count + 1
							end
							if table.getn(ids) > 0 then
								redis.call('XDEL', KEYS[1], unpack(ids))
							end
							if table.getn(ids) < 1000 then
								all = 1
								break
							end
							if count >= 100000 then
								break
							end
						end
						return all
						`
	r.garbageCollectorSha1 = redisGarbage.ScriptLoad(r.ctx, script)
}
