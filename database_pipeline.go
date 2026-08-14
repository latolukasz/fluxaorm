package fluxaorm

type DatabasePipeline struct {
	ctx        Context
	db         DB
	pool       string
	queries    []string
	parameters [][]any
	tables     []string
}

func (dp *DatabasePipeline) AddQuery(query string, parameters ...any) {
	dp.queries = append(dp.queries, query)
	dp.parameters = append(dp.parameters, parameters)
	dp.tables = append(dp.tables, "")
}

func (dp *DatabasePipeline) AddQueryForTable(table, query string, parameters ...any) {
	dp.queries = append(dp.queries, query)
	dp.parameters = append(dp.parameters, parameters)
	dp.tables = append(dp.tables, table)
}

func (dp *DatabasePipeline) discard() {
	dp.queries = dp.queries[:0]
	dp.parameters = dp.parameters[:0]
	dp.tables = dp.tables[:0]
}

func (dp *DatabasePipeline) Exec(ctx Context) error {
	if len(dp.queries) == 0 {
		return nil
	}
	defer dp.discard()
	// Inside a transaction the statements join it. Opening a nested Begin would
	// panic: the transaction client has no Begin.
	if orm, ok := ctx.(*ormImplementation); ok && orm.tx != nil {
		tx, err := orm.txFor(dp.pool)
		if err != nil {
			return err
		}
		for i, query := range dp.queries {
			if _, err = tx.Exec(ctx, query, dp.parameters[i]...); err != nil {
				return err
			}
		}
		return nil
	}
	if len(dp.queries) == 1 {
		_, err := dp.db.Exec(ctx, dp.queries[0], dp.parameters[0]...)
		return err
	}
	tr, err := dp.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tr.Rollback(ctx)
	}()
	for i, query := range dp.queries {
		_, err = tr.Exec(ctx, query, dp.parameters[i]...)
		if err != nil {
			return err
		}
	}
	return tr.Commit(ctx)
}
