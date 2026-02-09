package fluxaorm

type DatabasePipeline struct {
	ctx        Context
	db         DB
	pool       string
	queries    []string
	parameters [][]any
}

func (dp *DatabasePipeline) AddQuery(query string, parameters ...any) {
	dp.queries = append(dp.queries, query)
	dp.parameters = append(dp.parameters, parameters)
}

func (dp *DatabasePipeline) Exec(ctx Context) error {
	if len(dp.queries) == 0 {
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
	for i, query := range dp.queries {
		_, err = tr.Exec(ctx, query, dp.parameters[i])
		if err != nil {
			return err
		}
	}
	defer func() {
		_ = tr.Rollback(ctx)
	}()
	return tr.Commit(ctx)
}
