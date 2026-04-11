package fluxaorm

type indexDefinition struct {
	Columns []string
}

type EntityIndexes interface {
	Indexes() [][]string
}

type EntityUniqueIndexes interface {
	UniqueIndexes() [][]string
}

type EntityCachedUniqueIndexes interface {
	CachedUniqueIndexes() [][]string
}
