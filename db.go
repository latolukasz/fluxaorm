package fluxaorm

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const metricsOperationTransaction = "transaction"
const metricsOperationExec = "exec"
const metricsOperationSelect = "select"

type MySQLConfig interface {
	GetCode() string
	GetDatabaseName() string
	GetDataSourceURI() string
	GetOptions() *MySQLOptions
	getClient() *sql.DB
}

type mySQLConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	client         *sql.DB
	options        *MySQLOptions
}

func (p *mySQLConfig) GetCode() string {
	return p.code
}

func (p *mySQLConfig) GetDatabaseName() string {
	return p.databaseName
}

func (p *mySQLConfig) GetDataSourceURI() string {
	return p.dataSourceName
}

func (p *mySQLConfig) getClient() *sql.DB {
	return p.client
}

func (p *mySQLConfig) GetOptions() *MySQLOptions {
	return p.options
}

type ExecResult interface {
	LastInsertId() (uint64, error)
	RowsAffected() (uint64, error)
}

type execResult struct {
	r sql.Result
}

func (e *execResult) LastInsertId() (uint64, error) {
	id, err := e.r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}

func (e *execResult) RowsAffected() (uint64, error) {
	id, err := e.r.RowsAffected()
	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}

type sqlClientBase interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) SQLRow
	QueryRowContext(context context.Context, query string, args ...any) SQLRow
	Query(query string, args ...any) (SQLRows, error)
	QueryContext(context context.Context, query string, args ...any) (SQLRows, error)
}

type sqlClient interface {
	sqlClientBase
	Begin() (*sql.Tx, error)
}

type txClient interface {
	sqlClientBase
	Commit() error
	Rollback() error
}

type DBClientQuery interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(context context.Context, query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(context context.Context, query string, args ...any) (*sql.Rows, error)
}

type DBClient interface {
	DBClientQuery
}

type DBClientNoTX interface {
	DBClientQuery
	Begin() (*sql.Tx, error)
}

type TXClient interface {
	DBClientQuery
}

type standardSQLClient struct {
	db DBClient
}

type txSQLClient struct {
	standardSQLClient
	tx *sql.Tx
}

func (tx *txSQLClient) Commit() error {
	return tx.tx.Commit()
}

func (tx *txSQLClient) Rollback() error {
	return tx.tx.Rollback()
}

func (db *standardSQLClient) Exec(query string, args ...any) (sql.Result, error) {
	res, err := db.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) Begin() (*sql.Tx, error) {
	return db.db.(DBClientNoTX).Begin()
}

func (db *standardSQLClient) ExecContext(context context.Context, query string, args ...any) (sql.Result, error) {
	res, err := db.db.ExecContext(context, query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) QueryRow(query string, args ...any) SQLRow {
	return db.db.QueryRow(query, args...)
}

func (db *standardSQLClient) QueryRowContext(context context.Context, query string, args ...any) SQLRow {
	return db.db.QueryRowContext(context, query, args...)
}

func (db *standardSQLClient) Query(query string, args ...any) (SQLRows, error) {
	rows, err := db.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *standardSQLClient) QueryContext(context context.Context, query string, args ...any) (SQLRows, error) {
	rows, err := db.db.QueryContext(context, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type SQLRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type rowsStruct struct {
	sqlRows SQLRows
}

func (r *rowsStruct) Next() bool {
	has := r.sqlRows.Next()
	if !has {
		_ = r.sqlRows.Close()
	}
	return has
}

func (r *rowsStruct) Columns() ([]string, error) {
	return r.sqlRows.Columns()
}

func (r *rowsStruct) Scan(dest ...any) error {
	return r.sqlRows.Scan(dest...)
}

type SQLRow interface {
	Scan(dest ...any) error
	Err() error
}

type DBBase interface {
	GetConfig() MySQLConfig
	GetDBClient() DBClient
	SetMockDBClient(mock DBClient)
	Exec(ctx Context, query string, args ...any) (ExecResult, error)
	QueryRow(ctx Context, query Where, toFill ...any) (found bool, err error)
	Query(ctx Context, query string, args ...any) (rows Rows, close func(), err error)
}

type DB interface {
	DBBase
	Begin(ctx Context) (DBTransaction, error)
}

type DBTransaction interface {
	DBBase
	Commit(ctx Context) error
	Rollback(ctx Context) error
}

type dbImplementation struct {
	client      sqlClient
	config      MySQLConfig
	transaction bool
}

func (db *dbImplementation) GetConfig() MySQLConfig {
	return db.config
}

func (db *dbImplementation) Commit(ctx Context) error {
	if !db.transaction {
		return nil
	}
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	err := db.client.(txClient).Commit()
	end := time.Since(start)
	db.client.(*txSQLClient).db = nil
	db.client.(*txSQLClient).tx = nil
	db.transaction = false
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "COMMIT", end, err)
	}
	db.fillMetrics(ctx, end, metricsOperationTransaction, err)
	return err
}

func (db *dbImplementation) Rollback(ctx Context) error {
	if !db.transaction {
		return nil
	}
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	err := db.client.(txClient).Rollback()
	end := time.Since(start)
	db.transaction = false
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "ROLLBACK", end, err)
	}
	db.fillMetrics(ctx, end, metricsOperationTransaction, err)
	return err
}

func (db *dbImplementation) Begin(ctx Context) (DBTransaction, error) {
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	tx, err := db.client.Begin()
	end := time.Since(start)
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "START TRANSACTION", end, err)
	}
	db.fillMetrics(ctx, end, metricsOperationTransaction, err)
	if err != nil {
		return nil, err
	}
	dbTX := &dbImplementation{config: db.config, client: &txSQLClient{standardSQLClient{db: tx}, tx}, transaction: true}
	return dbTX, nil
}

func (db *dbImplementation) GetDBClient() DBClient {
	return db.client.(*standardSQLClient).db
}

func (db *dbImplementation) SetMockDBClient(mock DBClient) {
	db.client.(*standardSQLClient).db = mock
}

func (db *dbImplementation) Exec(ctx Context, query string, args ...any) (ExecResult, error) {
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	rows, err := db.client.Exec(query, args...)
	end := time.Since(start)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields(ctx, "EXEC", message, end, err)
	}
	db.fillMetrics(ctx, end, metricsOperationExec, err)
	return &execResult{r: rows}, err
}

func (db *dbImplementation) fillMetrics(ctx Context, end time.Duration, name string, err error) {
	metrics, hasMetrics := ctx.Engine().Registry().getMetricsRegistry()
	if hasMetrics {
		metrics.queriesDB.WithLabelValues(name, db.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Observe(end.Seconds())
		if err != nil {
			metrics.queriesDBErrors.WithLabelValues(db.GetConfig().GetCode(), ctx.getMetricsSourceTag()).Inc()
		}
	}
}

func (db *dbImplementation) QueryRow(ctx Context, query Where, toFill ...any) (found bool, err error) {
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	row := db.client.QueryRow(query.String(), query.GetParameters()...)
	end := time.Since(start)
	if row.Err() != nil {
		if hasLogger {
			message := query.String()
			if len(query.GetParameters()) > 0 {
				message += " " + fmt.Sprintf("%v", query.GetParameters())
			}
			db.fillMetrics(ctx, end, metricsOperationSelect, err)
			db.fillLogFields(ctx, "SELECT", message, end, err)
		}
		return false, row.Err()
	}
	err = row.Scan(toFill...)
	message := ""
	if hasLogger {
		message = query.String()
		if len(query.GetParameters()) > 0 {
			message += " " + fmt.Sprintf("%v", query.GetParameters())
		}
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if hasLogger {
				db.fillLogFields(ctx, "SELECT", message, end, nil)
			}
			db.fillMetrics(ctx, end, metricsOperationSelect, nil)
			return false, nil
		}
		if hasLogger {
			db.fillLogFields(ctx, "SELECT", message, end, err)
		}
		db.fillMetrics(ctx, end, metricsOperationSelect, err)
		return false, err
	}
	if hasLogger {
		db.fillLogFields(ctx, "SELECT", message, end, nil)
	}
	db.fillMetrics(ctx, end, metricsOperationSelect, nil)
	return true, nil
}

func (db *dbImplementation) Query(ctx Context, query string, args ...any) (rows Rows, close func(), err error) {
	hasLogger, _ := ctx.getDBLoggers()
	start := time.Now()
	result, err := db.client.Query(query, args...)
	end := time.Since(start)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields(ctx, "SELECT", message, end, err)
	}
	if err != nil {
		db.fillMetrics(ctx, end, metricsOperationSelect, err)
		return nil, nil, err
	}
	db.fillMetrics(ctx, end, metricsOperationSelect, err)
	return &rowsStruct{result}, func() {
		if result != nil {
			_ = result.Close()
		}
	}, nil
}

func (db *dbImplementation) fillLogFields(ctx Context, operation, query string, duration time.Duration, err error) {
	query = strings.ReplaceAll(query, "\n", " ")
	_, loggers := ctx.getDBLoggers()
	fillLogFields(ctx, loggers, db.GetConfig().GetCode(), sourceMySQL, operation, query, &duration, false, err)
}
