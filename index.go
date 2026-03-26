package fluxaorm

type indexDefinition struct {
	Columns []string
}

type EntityIndexes interface {
	Indexes() map[string][]string
}

type EntityUniqueIndexes interface {
	UniqueIndexes() map[string][]string
}

type EntityCachedUniqueIndexes interface {
	CachedUniqueIndexes() map[string][]string
}
