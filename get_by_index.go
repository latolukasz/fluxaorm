package fluxaorm

const redisValidSetValue = "Y"

type IndexDefinition struct {
	Columns string
	Cached  bool
}

type UniqueIndexDefinition struct {
	Columns string
	Cached  bool
}

type IndexInterface interface {
	Indexes() any
}

type indexDefinition struct {
	Cached     bool
	Columns    []string
	Where      string
	Duplicated bool
}
