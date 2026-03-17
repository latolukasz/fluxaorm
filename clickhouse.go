package fluxaorm

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ClickhouseConfig interface {
	GetCode() string
	GetDataSourceURI() string
	GetOptions() *ClickhouseOptions
	getClient() *sql.DB
}

type clickhouseConfig struct {
	dataSourceName string
	code           string
	client         *sql.DB
	options        *ClickhouseOptions
}

func (p *clickhouseConfig) GetCode() string {
	return p.code
}

func (p *clickhouseConfig) GetDataSourceURI() string {
	return p.dataSourceName
}

func (p *clickhouseConfig) getClient() *sql.DB {
	return p.client
}

func (p *clickhouseConfig) GetOptions() *ClickhouseOptions {
	return p.options
}

type ClickhouseOptions struct {
	ConnMaxLifetime    time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
}

type Clickhouse interface {
	GetConfig() ClickhouseConfig
	GetDBClient() DBClient
	SetMockDBClient(mock DBClient)
	Exec(ctx Context, query string, args ...any) (ExecResult, error)
	QueryRow(ctx Context, query string, toFill ...any) (found bool, err error)
	Query(ctx Context, query string, args ...any) (rows Rows, close func(), err error)
}

type clickhouseImplementation struct {
	client sqlClientBase
	config ClickhouseConfig
}

func (ch *clickhouseImplementation) GetConfig() ClickhouseConfig {
	return ch.config
}

func (ch *clickhouseImplementation) GetDBClient() DBClient {
	return ch.client.(*standardSQLClient).db
}

func (ch *clickhouseImplementation) SetMockDBClient(mock DBClient) {
	ch.client.(*standardSQLClient).db = mock
}

func (ch *clickhouseImplementation) Exec(ctx Context, query string, args ...any) (ExecResult, error) {
	hasLogger, _ := ctx.getClickhouseLoggers()
	start := time.Now()
	rows, err := ch.client.Exec(query, args...)
	end := time.Since(start)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		ch.fillLogFields(ctx, "EXEC", message, end, err)
	}
	ch.fillMetrics(ctx, end, metricsOperationExec, err)
	return &execResult{r: rows}, err
}

func (ch *clickhouseImplementation) QueryRow(ctx Context, query string, toFill ...any) (found bool, err error) {
	hasLogger, _ := ctx.getClickhouseLoggers()
	start := time.Now()
	row := ch.client.QueryRow(query)
	end := time.Since(start)
	if row.Err() != nil {
		if hasLogger {
			ch.fillLogFields(ctx, "SELECT", query, end, row.Err())
		}
		ch.fillMetrics(ctx, end, metricsOperationSelect, row.Err())
		return false, row.Err()
	}
	err = row.Scan(toFill...)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if hasLogger {
				ch.fillLogFields(ctx, "SELECT", query, end, nil)
			}
			ch.fillMetrics(ctx, end, metricsOperationSelect, nil)
			return false, nil
		}
		if hasLogger {
			ch.fillLogFields(ctx, "SELECT", query, end, err)
		}
		ch.fillMetrics(ctx, end, metricsOperationSelect, err)
		return false, err
	}
	if hasLogger {
		ch.fillLogFields(ctx, "SELECT", query, end, nil)
	}
	ch.fillMetrics(ctx, end, metricsOperationSelect, nil)
	return true, nil
}

func (ch *clickhouseImplementation) Query(ctx Context, query string, args ...any) (rows Rows, close func(), err error) {
	hasLogger, _ := ctx.getClickhouseLoggers()
	start := time.Now()
	result, err := ch.client.Query(query, args...)
	end := time.Since(start)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		ch.fillLogFields(ctx, "SELECT", message, end, err)
	}
	if err != nil {
		ch.fillMetrics(ctx, end, metricsOperationSelect, err)
		return nil, nil, err
	}
	ch.fillMetrics(ctx, end, metricsOperationSelect, err)
	return &rowsStruct{result}, func() {
		if result != nil {
			_ = result.Close()
		}
	}, nil
}

func (ch *clickhouseImplementation) fillMetrics(ctx Context, end time.Duration, name string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesClickhouse.WithLabelValues(name, ch.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Observe(end.Seconds())
		if err != nil {
			metrics.queriesClickhouseErrors.WithLabelValues(ch.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
	}
}

func (ch *clickhouseImplementation) fillLogFields(ctx Context, operation, query string, duration time.Duration, err error) {
	query = strings.ReplaceAll(query, "\n", " ")
	_, loggers := ctx.getClickhouseLoggers()
	fillLogFields(ctx, loggers, ch.GetConfig().GetCode(), sourceClickhouse, operation, query, &duration, false, err)
}
