package fluxaorm

import (
	"errors"
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
	consumer, _ := ctx.GetEventBroker().ConsumerSingle(ctx, LogChannelName)
	c.consumer = consumer.(*eventsConsumer)
	return c
}

func NewLogTablesConsumerMany(ctx Context) *LogTablesConsumer {
	c := &LogTablesConsumer{}
	consumer, _ := ctx.GetEventBroker().ConsumerMany(ctx, LogChannelName)
	c.consumer = consumer.(*eventsConsumer)
	return c
}

func (r *LogTablesConsumer) Consume(count int, blockTime time.Duration) error {
	return r.consumer.Consume(count, blockTime, func(events []Event) error {
		for _, e := range events {
			err := r.handleLogTable(e)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *LogTablesConsumer) AutoClaim(count int, minIdle time.Duration) error {
	return r.consumer.AutoClaim(count, minIdle, func(events []Event) error {
		for _, e := range events {
			err := r.handleLogTable(e)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *LogTablesConsumer) Cleanup() error {
	return r.consumer.Cleanup()
}

func (r *LogTablesConsumer) handleLogTable(event Event) error {
	var lazyEvent []any
	err := event.Unserialize(&lazyEvent)
	if err != nil {
		return err
	}
	if lazyEvent == nil || len(lazyEvent) < 3 {
		return event.Ack()
	}
	sql, valid := lazyEvent[0].(string)
	if !valid {
		return event.Ack()
	}
	dbCode, valid := lazyEvent[len(lazyEvent)-1].(string)
	if !valid {
		return event.Ack()
	}
	db := r.consumer.ctx.Engine().DB(dbCode)
	if db == nil {
		return event.Ack()
	}
	args := lazyEvent[1 : len(lazyEvent)-1]
	_, err = db.Exec(r.consumer.ctx, sql, args...)
	if err != nil {
		var asMySQLError *mysql.MySQLError
		isMySQLError := errors.As(err, &asMySQLError)
		if isMySQLError && slices.Contains(mySQLErrorCodesToSkip, asMySQLError.Number) {
			return nil
		}
		return err
	}
	err = event.Ack()
	if err != nil {
		return err
	}
	return event.Ack()
}
