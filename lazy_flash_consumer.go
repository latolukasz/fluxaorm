package fluxaorm

import (
	"slices"
	"time"

	"github.com/go-sql-driver/mysql"
)

const LazyChannelName = "orm-lazy-channel"
const LazyErrorsChannelName = "orm-lazy-errors-channel"

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

type LazyFlashConsumer struct {
	eventConsumerBase
	consumer *eventsConsumer
}

func NewLazyFlashConsumer(ctx Context) *LazyFlashConsumer {
	c := &LazyFlashConsumer{}
	c.ctx = ctx.(*ormImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	return c
}

func (r *LazyFlashConsumer) SetBlockTime(ttl time.Duration) {
	r.eventConsumerBase.SetBlockTime(ttl)
}

func (r *LazyFlashConsumer) Digest() bool {
	r.consumer = r.ctx.GetEventBroker().Consumer(r.ctx, LazyChannelName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(500, func(events []Event) {
		for _, e := range events {
			r.handleLazyFlush(e)
		}
	})
}

func (r *LazyFlashConsumer) handleLazyFlush(event Event) {
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
		event.Ack()
		return
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		event.Ack()
		return
	}
	dbCode, valid := lazyEvent[1].(string)
	if !valid {
		event.Ack()
		return
	}
	db := r.ctx.Engine().DB(dbCode)
	if db == nil {
		event.Ack()
		return
	}
	if len(lazyEvent) > 2 {
		db.Exec(r.ctx, sql, lazyEvent[2:]...)
	} else {
		db.Exec(r.ctx, sql)
	}
	event.Ack()
}
