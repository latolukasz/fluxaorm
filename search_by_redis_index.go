package orm

type redisSearchIndexDefinition struct {
	FieldType  string
	Sortable   bool
	NoSteam    bool
	IndexEmpty bool
}
