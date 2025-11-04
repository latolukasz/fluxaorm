package fluxaorm

import (
	"slices"
	"time"

	"github.com/go-sql-driver/mysql"
)

const LogChannelName = "orm-log-channel"

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

type LogTablesConsumer struct {
	eventConsumerBase
	consumer *eventsConsumer
}

func NewLogTablesConsumer(ctx Context) *LogTablesConsumer {
	c := &LogTablesConsumer{}
	c.ctx = ctx.(*ormImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	return c
}

func (r *LogTablesConsumer) SetBlockTime(ttl time.Duration) {
	r.eventConsumerBase.SetBlockTime(ttl)
}

func (r *LogTablesConsumer) Digest() bool {
	r.consumer = r.ctx.GetEventBroker().Consumer(r.ctx, LogChannelName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(500, func(events []Event) {
		for _, e := range events {
			r.handleLogTable(e)
		}
	})
}

func (r *LogTablesConsumer) handleLogTable(event Event) {
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
		event.Ack()
		return
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		event.Ack()
		return
	}
	dbCode, valid := lazyEvent[len(lazyEvent)-1].(string)
	if !valid {
		event.Ack()
		return
	}
	db := r.ctx.Engine().DB(dbCode)
	if db == nil {
		event.Ack()
		return
	}
	args := lazyEvent[1 : len(lazyEvent)-1]
	db.Exec(r.ctx, sql, args...)
	event.Ack()
	event.Ack()
}
