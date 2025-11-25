package fluxaorm

import (
	"errors"
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
	consumer *eventsConsumer
}

func NewLazyFlashConsumer(ctx Context) *LazyFlashConsumer {
	c := &LazyFlashConsumer{}
	consumer, _ := ctx.GetEventBroker().ConsumerSingle(ctx, LazyChannelName)
	c.consumer, _ = consumer.(*eventsConsumer)
	return c
}

func (r *LazyFlashConsumer) Consume(blockTime time.Duration) error {
	return r.consumer.Consume(500, blockTime, func(events []Event) error {
		for _, e := range events {
			err := r.handleLazyFlush(e)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *LazyFlashConsumer) handleLazyFlush(event Event) error {
	var lazyEvent []any
	var err error
	err = event.Unserialize(&lazyEvent)
	if err != nil {
		return err
	}
	if lazyEvent == nil || len(lazyEvent) < 2 {
		return event.Ack()
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		return event.Ack()
	}
	dbCode, valid := lazyEvent[1].(string)
	if !valid {
		return event.Ack()
	}
	db := r.consumer.ctx.Engine().DB(dbCode)
	if db == nil {
		return event.Ack()
	}
	if len(lazyEvent) > 2 {
		_, err = db.Exec(r.consumer.ctx, sql, lazyEvent[2:]...)
	} else {
		_, err = db.Exec(r.consumer.ctx, sql)
	}
	if err != nil {
		var asMySQLError *mysql.MySQLError
		isMySQLError := errors.As(err, &asMySQLError)
		if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
			_, err = r.consumer.ctx.GetEventBroker().Publish(LazyErrorsChannelName, event)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return event.Ack()
}
