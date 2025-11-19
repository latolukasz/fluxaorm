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
	consumer *eventsConsumer
}

func NewLogTablesConsumerSingle(ctx Context) *LogTablesConsumer {
	c := &LogTablesConsumer{}
	c.consumer = ctx.GetEventBroker().ConsumerSingle(ctx, LogChannelName).(*eventsConsumer)
	return c
}

func NewLogTablesConsumerMany(ctx Context) *LogTablesConsumer {
	c := &LogTablesConsumer{}
	c.consumer = ctx.GetEventBroker().ConsumerMany(ctx, LogChannelName).(*eventsConsumer)
	return c
}

func (r *LogTablesConsumer) Consume(count int, blockTime time.Duration) {
	r.consumer.Consume(count, blockTime, func(events []Event) {
		for _, e := range events {
			r.handleLogTable(e)
		}
	})
}

func (r *LogTablesConsumer) AutoClaim(count int, minIdle time.Duration) {
	r.consumer.AutoClaim(count, minIdle, func(events []Event) {
		for _, e := range events {
			r.handleLogTable(e)
		}
	})
}

func (r *LogTablesConsumer) Cleanup() {
	r.consumer.Cleanup()
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
	db := r.consumer.ctx.Engine().DB(dbCode)
	if db == nil {
		event.Ack()
		return
	}
	args := lazyEvent[1 : len(lazyEvent)-1]
	db.Exec(r.consumer.ctx, sql, args...)
	event.Ack()
	event.Ack()
}
