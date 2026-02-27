package fluxaorm

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type dirtyDefinition struct {
	Stream  string
	Columns map[string]bool
}

type searchableFieldDef struct {
	columnName  string
	sqlRowIndex int
	redisType   string
	sortable    bool
	goKind      string
	nullable    bool
	precision   int
}

type pendingSearchableField struct {
	redisType string
	goKind    string
	sortable  bool
	nullable  bool
	precision int
}

type enumDefinition struct {
	fields       []string
	fieldNames   []string
	mapping      map[string]int
	required     bool
	defaultValue string
	name         string
}

func (d *enumDefinition) GetFields() []string {
	return d.fields
}

func (d *enumDefinition) Has(value string) bool {
	_, has := d.mapping[value]
	return has
}

func (d *enumDefinition) Index(value string) int {
	return d.mapping[value]
}

func initEnumDefinition(name string, values []string, required bool) *enumDefinition {
	enum := &enumDefinition{
		required:   required,
		name:       name,
		mapping:    make(map[string]int),
		fields:     make([]string, 0, len(values)),
		fieldNames: make([]string, 0, len(values)),
	}
	for i, v := range values {
		v = strings.TrimSpace(v)
		enum.fields = append(enum.fields, v)
		enum.fieldNames = append(enum.fieldNames, enumValueToFieldName(v))
		enum.mapping[v] = i + 1
	}
	if len(enum.fields) > 0 {
		enum.defaultValue = enum.fields[0]
	}
	return enum
}

func enumValueToFieldName(value string) string {
	parts := strings.Split(value, "_")
	result := ""
	for _, p := range parts {
		if len(p) > 0 {
			result += strings.ToUpper(p[:1]) + p[1:]
		}
	}
	if result == "" {
		return "V"
	}
	return result
}

type entitySchema struct {
	index                   uint64
	cacheTTL                int
	tableName               string
	mysqlPoolCode           string
	t                       reflect.Type
	hasFakeDelete           bool
	fields                  *tableFields
	engine                  Engine
	tags                    map[string]map[string]string
	columnNames             []string
	fieldDefinitions        map[string]schemaFieldAttributes
	uniqueIndexes           map[string]indexDefinition
	uniqueIndexesColumns    map[string][]string
	references              map[string]referenceDefinition
	dirtyAdded              []*dirtyDefinition
	dirtyUpdated            []*dirtyDefinition
	dirtyDeleted            []*dirtyDefinition
	options                 map[string]any
	hasLocalCache           bool
	localCache              *localCache
	localCacheLimit         int
	redisCacheName          string
	hasRedisCache           bool
	redisCache              *redisCache
	cacheKey                string
	uuidCacheKey            string
	uuidMutex               sync.Mutex
	structureHash           string
	hasCreatedAt            bool
	createdAtFIndex         int
	hasUpdatedAt            bool
	updatedAtFIndex         int
	hasRedisSearch          bool
	redisSearchPoolCode     string
	redisSearchIndex        string
	redisSearchPrefix       string
	searchableFields        []searchableFieldDef
	pendingSearchableFields map[string]pendingSearchableField
	cachedUniqueIndexes     map[string]bool
	hasCachedUniqueIndexes  bool
	uniqueIndexFIndexes     map[string][]int
}

type tableFields struct {
	t                         reflect.Type
	fields                    map[int]reflect.StructField
	forcedOldBid              map[int]bool
	arrays                    map[int]int
	prefix                    string
	uIntegers                 []int
	integers                  []int
	references                []int
	referencesRequired        []bool
	uIntegersNullable         []int
	uIntegersNullableSize     []int
	integersNullable          []int
	integersNullableSize      []int
	strings                   []int
	stringMaxLengths          []int
	stringsRequired           []bool
	stringsEnums              []int
	enums                     []*enumDefinition
	sliceStringsSets          []int
	sets                      []*enumDefinition
	bytes                     []int
	booleans                  []int
	booleansNullable          []int
	floats                    []int
	floatsPrecision           []int
	floatsDecimalSize         []int
	floatsSize                []int
	floatsUnsigned            []bool
	floatsNullable            []int
	floatsNullablePrecision   []int
	floatsNullableDecimalSize []int
	floatsNullableUnsigned    []bool
	floatsNullableSize        []int
	timesNullable             []int
	datesNullable             []int
	times                     []int
	dates                     []int
	structs                   []int
	structsFields             []*tableFields
}

func (e *entitySchema) GetTableName() string {
	return e.tableName
}

func (e *entitySchema) GetType() reflect.Type {
	return e.t
}

func (e *entitySchema) DropTable(ctx Context) error {
	pool := e.GetDB()
	_, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetConfig().GetDatabaseName(), e.tableName))
	return err
}

func (e *entitySchema) TruncateTable(ctx Context) error {
	pool := e.GetDB()
	_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", pool.GetConfig().GetDatabaseName(), e.tableName))
	if err != nil {
		return err
	}
	return nil
}

func (e *entitySchema) UpdateSchema(ctx Context) error {
	pool := e.GetDB()
	alters, has, err := e.GetSchemaChanges(ctx)
	if err != nil {
		return err
	}
	if has {
		for _, alter := range alters {
			_, err = pool.Exec(ctx, alter.SQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *entitySchema) UpdateSchemaAndTruncateTable(ctx Context) error {
	err := e.UpdateSchema(ctx)
	if err != nil {
		return err
	}
	pool := e.GetDB()
	_, err = pool.Exec(ctx, fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetConfig().GetDatabaseName(), e.tableName))
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetConfig().GetDatabaseName(), e.tableName))
	return err
}

func (e *entitySchema) GetDB() DB {
	return e.engine.DB(e.mysqlPoolCode)
}

func (e *entitySchema) GetLocalCache() (cache LocalCache, has bool) {
	if !e.hasLocalCache {
		return nil, false
	}
	return e.localCache, true
}

func (e *entitySchema) GetRedisCache() (cache RedisCache, has bool) {
	if !e.hasRedisCache {
		return nil, false
	}
	return e.redisCache, true
}

func (e *entitySchema) GetColumns() []string {
	return e.columnNames
}

func (e *entitySchema) GetUniqueIndexes() map[string][]string {
	return e.uniqueIndexesColumns
}

func (e *entitySchema) GetSchemaChanges(ctx Context) (alters []Alter, has bool, err error) {
	pre, alters, post, err := getSchemaChanges(ctx, e)
	if err != nil {
		return nil, false, err
	}
	final := pre
	final = append(final, alters...)
	final = append(final, post...)
	return final, len(final) > 0, nil
}

func (e *entitySchema) markSearchableField(colName, redisType, goKind string, sortable, nullable bool, precision int) {
	if e.pendingSearchableFields == nil {
		e.pendingSearchableFields = make(map[string]pendingSearchableField)
	}
	e.pendingSearchableFields[colName] = pendingSearchableField{
		redisType: redisType,
		goKind:    goKind,
		sortable:  sortable,
		nullable:  nullable,
		precision: precision,
	}
}

func (e *entitySchema) init(registry *registry, entityType reflect.Type) error {
	e.t = entityType
	e.tags = extractTags(registry, entityType, "")
	userTTL := e.getTag("ttl", "", "")
	if userTTL != "" {
		ttl, err := strconv.Atoi(userTTL)
		if err != nil {
			return fmt.Errorf("invalid ttl '%s' for entity '%s'", userTTL, entityType.Name())
		}
		e.cacheTTL = ttl
	}

	e.options = make(map[string]any)
	e.references = make(map[string]referenceDefinition)
	e.uniqueIndexes = make(map[string]indexDefinition)
	e.cachedUniqueIndexes = make(map[string]bool)
	fakeDeleteField, foundFakeDeleteField := e.t.FieldByName("FakeDelete")
	e.hasFakeDelete = foundFakeDeleteField && fakeDeleteField.Type.Kind() == reflect.Bool
	createdAtField, foundCreatedAt := e.t.FieldByName("CreatedAt")
	if foundCreatedAt && createdAtField.Type.String() == "time.Time" {
		e.hasCreatedAt = true
	}
	updatedAtField, foundUpdatedAt := e.t.FieldByName("UpdatedAt")
	if foundUpdatedAt && updatedAtField.Type.String() == "time.Time" {
		e.hasUpdatedAt = true
	}
	e.mysqlPoolCode = e.getTag("mysql", "default", DefaultPoolCode)
	_, has := registry.mysqlPools[e.mysqlPoolCode]
	if !has {
		return fmt.Errorf("mysql pool '%s' not found", e.mysqlPoolCode)
	}
	redisSearchPoolCode := e.getTag("redisSearch", DefaultPoolCode, "")
	if redisSearchPoolCode != "" {
		_, has := registry.redisPools[redisSearchPoolCode]
		if !has {
			return fmt.Errorf("redis pool '%s' not found for redisSearch in entity '%s'", redisSearchPoolCode, entityType.Name())
		}
		e.redisSearchPoolCode = redisSearchPoolCode
	}
	e.tableName = e.getTag("table", entityType.Name(), entityType.Name())
	redisCacheName := e.getTag("redisCache", DefaultPoolCode, "")
	if redisCacheName != "" {
		_, has := registry.redisPools[redisCacheName]
		if !has {
			return fmt.Errorf("redis pool '%s' not found", redisCacheName)
		}
	}
	cacheKey := ""
	if e.mysqlPoolCode != DefaultPoolCode {
		cacheKey = e.mysqlPoolCode
	}
	cacheKey += e.tableName
	addList := make([]*dirtyDefinition, 0)
	editList := make([]*dirtyDefinition, 0)
	deleteList := make([]*dirtyDefinition, 0)
	for k, v := range e.tags {
		keys, has := v["dirty"]
		if has {
			for _, dirtyDef := range strings.Split(keys, ",") {
				types := strings.Split(dirtyDef, ":")
				sources := []string{"add", "edit", "delete"}
				if k != "ID" {
					sources = []string{"edit"}
				}
				if len(types) > 2 {
					return fmt.Errorf("invalid dirty definition for field %s in entity %s", k, entityType.Name())
				} else if len(types) == 2 {
					if k != "ID" {
						return fmt.Errorf("invalid dirty definition for field %s in entity %s", k, entityType.Name())
					}
					userSources := strings.Split(types[1], "|")
					if len(userSources) > 3 {
						return fmt.Errorf("invalid dirty definition for field %s in entity %s", k, entityType.Name())
					}
					for _, source := range userSources {
						if !slices.Contains(sources, source) {
							return fmt.Errorf("invalid dirty definition for field %s in entity %s", k, entityType.Name())
						}
					}
					sources = userSources
				}
				stream := types[0]
				for _, source := range sources {
					var actual []*dirtyDefinition
					if source == "add" {
						actual = addList
					} else if source == "edit" {
						actual = editList
					} else if source == "delete" {
						actual = deleteList
					}
					var def *dirtyDefinition
					for _, beforeDef := range actual {
						if beforeDef.Stream == stream {
							def = beforeDef
							def.Columns[k] = true
							break
						}
					}
					if def == nil {
						def = &dirtyDefinition{
							Stream:  stream,
							Columns: map[string]bool{k: true},
						}
						actual = append(actual, def)
						if source == "add" {
							addList = actual
						} else if source == "edit" {
							editList = actual
						} else if source == "delete" {
							deleteList = actual
						}
					}
				}
			}
		}
		e.dirtyAdded = addList
		e.dirtyUpdated = editList
		e.dirtyDeleted = deleteList
	}
	e.fieldDefinitions = make(map[string]schemaFieldAttributes)
	e.uniqueIndexesColumns = make(map[string][]string)
	err := e.initIndexes()
	if err != nil {
		return err
	}
	fields, err := e.buildTableFields(entityType, registry, 0, "", nil, e.tags, "")
	if err != nil {
		return err
	}
	e.fields = fields
	e.columnNames = e.fields.buildColumnNames("")
	columnMapping := make(map[string]int)
	for i, name := range e.columnNames {
		columnMapping[name] = i
	}
	if e.hasCreatedAt {
		e.createdAtFIndex = columnMapping["CreatedAt"]
	}
	if e.hasUpdatedAt {
		e.updatedAtFIndex = columnMapping["UpdatedAt"]
	}
	e.uniqueIndexFIndexes = make(map[string][]int)
	for indexName, def := range e.uniqueIndexes {
		fIndexes := make([]int, len(def.Columns))
		for i, colName := range def.Columns {
			fIndexes[i] = columnMapping[colName]
		}
		e.uniqueIndexFIndexes[indexName] = fIndexes
	}
	if len(e.pendingSearchableFields) > 0 && e.redisSearchPoolCode != "" {
		for i, colName := range e.columnNames {
			if pending, ok := e.pendingSearchableFields[colName]; ok {
				e.searchableFields = append(e.searchableFields, searchableFieldDef{
					columnName:  colName,
					sqlRowIndex: i,
					redisType:   pending.redisType,
					sortable:    pending.sortable,
					goKind:      pending.goKind,
					nullable:    pending.nullable,
					precision:   pending.precision,
				})
			}
		}
		if len(e.searchableFields) > 0 {
			e.hasRedisSearch = true
			sortedDefs := make([]string, len(e.searchableFields))
			for i, f := range e.searchableFields {
				sortable := "0"
				if f.sortable {
					sortable = "1"
				}
				sortedDefs[i] = f.columnName + ":" + f.redisType + ":" + sortable
			}
			slices.Sort(sortedDefs)
			h := fnv.New32a()
			for _, s := range sortedDefs {
				_, _ = h.Write([]byte(s))
			}
			e.redisSearchIndex = e.tableName + "_" + fmt.Sprintf("%08x", h.Sum32())
			h2 := fnv.New32a()
			_, _ = h2.Write([]byte(e.tableName + ":search"))
			prefix := fmt.Sprintf("%08x", h2.Sum32())
			e.redisSearchPrefix = prefix[:5] + ":h:"
		}
	}
	e.pendingSearchableFields = nil
	cacheKey = fmt.Sprintf("%x", sha256.Sum256([]byte(cacheKey+strings.Join(e.columnNames, ":"))))
	e.uuidCacheKey = cacheKey[0:12]
	cacheKey = cacheKey[0:5]
	h := fnv.New32a()
	_, _ = h.Write([]byte(cacheKey))
	e.structureHash = strconv.FormatUint(uint64(h.Sum32()), 10)
	localCacheLimit := e.getTag("localCache", "0", "")
	if localCacheLimit != "" {
		localCacheLimitAsInt, err := strconv.Atoi(localCacheLimit)
		if err != nil {
			return fmt.Errorf("invalid local cache pool limit '%s'", localCacheLimit)
		}
		e.hasLocalCache = true
		e.localCacheLimit = localCacheLimitAsInt
	}
	e.redisCacheName = redisCacheName
	e.hasRedisCache = redisCacheName != ""
	e.cacheKey = cacheKey
	err = e.validateIndexes()
	if err != nil {
		return err
	}
	return nil
}

func (e *entitySchema) initIndexes() error {
	type pendingCol struct {
		fieldName string
		position  int
	}
	pending := make(map[string][]pendingCol)
	for fieldName, tags := range e.tags {
		uniqueVal, hasUnique := tags["unique"]
		if !hasUnique {
			continue
		}
		indexName := fieldName
		position := 1
		if uniqueVal != "true" {
			parts := strings.SplitN(uniqueVal, ":", 2)
			indexName = parts[0]
			if len(parts) == 2 {
				pos, err := strconv.Atoi(parts[1])
				if err != nil || pos < 1 {
					return fmt.Errorf("invalid unique index position '%s' for field '%s' in entity '%s'", parts[1], fieldName, e.t.String())
				}
				position = pos
			}
		}
		pending[indexName] = append(pending[indexName], pendingCol{fieldName, position})
	}
	for indexName, cols := range pending {
		posMap := make(map[int]string, len(cols))
		for _, col := range cols {
			if _, exists := posMap[col.position]; exists {
				return fmt.Errorf("unique index '%s' in entity '%s' has duplicate position %d", indexName, e.t.String(), col.position)
			}
			posMap[col.position] = col.fieldName
		}
		columns := make([]string, len(posMap))
		for pos, fieldName := range posMap {
			if pos < 1 || pos > len(posMap) {
				return fmt.Errorf("unique index '%s' in entity '%s' has invalid position %d (must be between 1 and %d)", indexName, e.t.String(), pos, len(posMap))
			}
			columns[pos-1] = fieldName
		}
		e.uniqueIndexes[indexName] = indexDefinition{Columns: columns}
		e.uniqueIndexesColumns[indexName] = columns
		firstCol := columns[0]
		if tags, ok := e.tags[firstCol]; ok {
			if _, hasCached := tags["cached"]; hasCached {
				e.cachedUniqueIndexes[indexName] = true
				e.hasCachedUniqueIndexes = true
			}
		}
	}
	return nil
}

func (e *entitySchema) validateIndexes() error {
	all := make(map[string]map[int]string)
	for indexName, def := range e.uniqueIndexes {
		all[indexName] = make(map[int]string)
		for i, columnName := range def.Columns {
			if !slices.Contains(e.columnNames, columnName) {
				return fmt.Errorf("unique index column '%s' not found in entity '%s'", columnName, e.t.String())
			}
			all[indexName][i+1] = columnName
		}
	}
	for k, v := range all {
		for k2, v2 := range all {
			if k == k2 {
				continue
			}
			same := 0
			for i := 1; i <= len(v); i++ {
				right, has := v2[i]
				if has && right == v[i] {
					same++
					continue
				}
				break
			}
			if same == len(v) {
				return fmt.Errorf("duplicated index %s with %s in %s", k, k2, e.t.String())
			}
		}
	}
	return nil
}

func (e *entitySchema) getTag(key, trueValue, defaultValue string) string {
	userValue, has := e.tags["ID"][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return e.GetTag("ID", key, trueValue, defaultValue)
}

func (e *entitySchema) GetTag(field, key, trueValue, defaultValue string) string {
	userValue, has := e.tags[field][key]
	if has {
		if userValue == "true" {
			return trueValue
		}
		return userValue
	}
	return defaultValue
}

func (e *entitySchema) SetOption(key string, value any) {
	e.options[key] = value
}

func (e *entitySchema) Option(key string) any {
	return e.options[key]
}

func (e *entitySchema) uuid(ctx Context) uint64 {
	r := ctx.Engine().Redis(e.getForcedRedisCode())
	id, err := r.Incr(ctx, e.uuidCacheKey)
	if err != nil {
		panic(err)
	}
	if id == 1 {
		e.initUUID(ctx)
		return e.uuid(ctx)
	}
	return uint64(id)
}

func (e *entitySchema) initUUID(ctx Context) {
	r := ctx.Engine().Redis(e.getForcedRedisCode())
	e.uuidMutex.Lock()
	defer e.uuidMutex.Unlock()
	now, has, err := r.Get(ctx, e.uuidCacheKey)
	if err != nil {
		panic(err)
	}
	if has && now != "1" {
		return
	}
	lockName := e.uuidCacheKey + ":lock"
	lock, obtained, err := r.GetLocker().Obtain(ctx, lockName, time.Minute, time.Second*5)
	if err != nil {
		panic(err)
	}
	if !obtained {
		panic(errors.New("uuid lock timeout"))
	}
	defer lock.Release(ctx)
	now, has, err = r.Get(ctx, e.uuidCacheKey)
	if err != nil {
		panic(err)
	}
	if has && now != "1" {
		return
	}
	maxID := int64(0)
	_, err = e.GetDB().QueryRow(ctx, NewWhere("SELECT IFNULL(MAX(ID), 0) FROM `"+e.GetTableName()+"`"), &maxID)
	if err != nil {
		panic(err)
	}
	if maxID == 0 {
		maxID = 1
	}
	_, err = r.IncrBy(ctx, e.uuidCacheKey, maxID)
	if err != nil {
		panic(err)
	}
}

func (e *entitySchema) getForcedRedisCode() string {
	if e.hasRedisCache {
		return e.redisCacheName
	}
	return DefaultPoolCode
}

func (e *entitySchema) DisableCache(local, redis bool) {
	if local {
		e.hasLocalCache = false
	}
	if redis {
		e.redisCacheName = ""
		e.hasRedisCache = false
	}
}

func (e *entitySchema) ClearCache(ctx Context) (int, error) {
	if e.hasLocalCache {
		e.localCache.Clear(ctx)
	}
	if !e.hasRedisCache {
		return 0, nil
	}
	r := ctx.Engine().Redis(e.redisCacheName)
	script := `
local cursor = '0'
local deleted = 0

repeat
  local result = redis.call('SCAN', cursor, 'MATCH', KEYS[1], 'COUNT', 1000)
  cursor = result[1]
  local keys = result[2]

  if #keys > 0 then
    deleted = deleted + redis.call('UNLINK', unpack(keys))
  end
until cursor == '0'

return deleted
`
	res, err := r.Eval(ctx, script, []string{e.cacheKey + ":*"})
	if err != nil {
		return 0, err
	}
	return int(res.(int64)), nil
}

func (e *entitySchema) buildTableFields(t reflect.Type, registry *registry,
	start int, prefix string, parents []int, schemaTags map[string]map[string]string, extraPrefix string) (*tableFields, error) {
	fields := &tableFields{t: t, prefix: prefix, fields: make(map[int]reflect.StructField)}
	fields.forcedOldBid = make(map[int]bool)
	fields.arrays = make(map[int]int)
	for i := start; i < t.NumField(); i++ {
		f := t.Field(i)
		tags := schemaTags[prefix+f.Name]
		_, has := tags["ignore"]
		if has {
			continue
		}
		hasUnique := false
		for _, def := range e.uniqueIndexes {
			if slices.Contains(def.Columns, prefix+f.Name) {
				hasUnique = true
				break
			}
		}
		if hasUnique {
			fields.forcedOldBid[i] = true
		}
		attributes := schemaFieldAttributes{
			Fields:      fields,
			Tags:        tags,
			Index:       i,
			Parents:     parents,
			Prefix:      prefix,
			ExtraPrefix: extraPrefix,
			Field:       f,
			TypeName:    f.Type.String(),
		}
		fields.fields[i] = f
		switch attributes.TypeName {
		case "uint":
			e.buildUintField(attributes, 0, math.MaxUint)
		case "uint8":
			e.buildUintField(attributes, 0, math.MaxUint8)
		case "uint16":
			e.buildUintField(attributes, 0, math.MaxUint16)
		case "uint32":
			e.buildUintField(attributes, 0, math.MaxUint32)
		case "uint64":
			e.buildUintField(attributes, 0, math.MaxUint64)
		case "*uint":
			e.buildUintPointerField(attributes, 0, math.MaxUint)
		case "*uint8":
			e.buildUintPointerField(attributes, 0, math.MaxUint8)
		case "*uint16":
			e.buildUintPointerField(attributes, 0, math.MaxUint16)
		case "*uint32":
			e.buildUintPointerField(attributes, 0, math.MaxUint32)
		case "*uint64":
			e.buildUintPointerField(attributes, 0, math.MaxUint64)
		case "int":
			e.buildIntField(attributes, math.MinInt, math.MaxInt)
		case "int8":
			e.buildIntField(attributes, math.MinInt8, math.MaxInt8)
		case "int16":
			e.buildIntField(attributes, math.MinInt16, math.MaxInt16)
		case "int32":
			e.buildIntField(attributes, math.MinInt32, math.MaxInt32)
		case "int64":
			e.buildIntField(attributes, math.MinInt64, math.MaxInt64)

		case "*int":
			e.buildIntPointerField(attributes, math.MinInt, math.MaxInt)
		case "*int8":
			e.buildIntPointerField(attributes, math.MinInt8, math.MaxInt8)
		case "*int16":
			e.buildIntPointerField(attributes, math.MinInt16, math.MaxInt16)
		case "*int32":
			e.buildIntPointerField(attributes, math.MinInt32, math.MaxInt32)
		case "*int64":
			e.buildIntPointerField(attributes, math.MinInt64, math.MaxInt64)
		case "string":
			if enumVals, hasEnum := attributes.Tags["enum"]; hasEnum {
				e.buildEnumField(attributes, strings.Split(enumVals, ","))
			} else if setVals, hasSet := attributes.Tags["set"]; hasSet {
				e.buildStringSliceField(attributes, strings.Split(setVals, ","))
			} else {
				e.buildStringField(attributes)
			}
		case "[]uint8":
			e.buildBytesField(attributes)
		case "bool":
			e.buildBoolField(attributes)
		case "*bool":
			e.buildBoolPointerField(attributes)
		case "float32",
			"float64":
			e.buildFloatField(attributes)
		case "*float32",
			"*float64":
			e.buildFloatPointerField(attributes)
		case "*time.Time":
			e.buildTimePointerField(attributes)
		case "time.Time":
			e.buildTimeField(attributes)
		default:
			fType := f.Type
			k := fType.Kind().String()
			if k == "struct" {
				err := e.buildStructField(attributes, registry, schemaTags)
				if err != nil {
					return nil, err
				}
			} else if fType.Implements(reflect.TypeOf((*referenceInterface)(nil)).Elem()) {
				e.buildReferenceField(attributes)
				if attributes.Tags["cached"] == "true" {
					fields.forcedOldBid[i] = true
				}
			} else {
				return nil, fmt.Errorf("%s field %s type %s is not supported", e.t.String(), f.Name, f.Type.String())
			}
		}
	}
	return fields, nil
}

type schemaFieldAttributes struct {
	Field       reflect.StructField
	TypeName    string
	Tags        map[string]string
	Fields      *tableFields
	Index       int
	Parents     []int
	Prefix      string
	ExtraPrefix string
}

func (attributes schemaFieldAttributes) GetColumnNames() []string {
	l, isArray := attributes.Fields.arrays[attributes.Index]
	if !isArray {
		return []string{attributes.Prefix + attributes.ExtraPrefix + attributes.Field.Name}
	}
	names := make([]string, l)
	for i := 0; i <= l; i++ {
		if i == l {
			break
		}
		names[i] = attributes.Prefix + attributes.ExtraPrefix + attributes.Field.Name + "_" + strconv.Itoa(i+1)
	}
	return names
}

func (e *entitySchema) buildUintField(attributes schemaFieldAttributes, min int64, max uint64) {
	attributes.Fields.uIntegers = append(attributes.Fields.uIntegers, attributes.Index)
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "uint", attributes.Tags["sortable"] == "true", false, 0)
		}
	}
}

func (e *entitySchema) buildReferenceField(attributes schemaFieldAttributes) {
	attributes.Fields.references = append(attributes.Fields.references, attributes.Index)
	fType := attributes.Field.Type
	for i, columnName := range attributes.GetColumnNames() {
		isRequired := attributes.Tags["required"] == "true"
		attributes.Fields.referencesRequired = append(attributes.Fields.referencesRequired, isRequired)
		var refType reflect.Type
		if i == 0 {
			refType = reflect.New(fType).Interface().(referenceInterface).getType()
			def := referenceDefinition{
				Type: refType,
			}
			e.references[columnName] = def
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "ref", attributes.Tags["sortable"] == "true", !isRequired, 0)
		}
	}
}

func (e *entitySchema) buildUintPointerField(attributes schemaFieldAttributes, min int64, max uint64) {
	attributes.Fields.uIntegersNullable = append(attributes.Fields.uIntegersNullable, attributes.Index)
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			switch attributes.TypeName {
			case "*uint":
				attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 0)
			case "*uint8":
				attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 8)
			case "*uint16":
				attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 16)
			case "*uint32":
				attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 32)
			case "*uint64":
				attributes.Fields.uIntegersNullableSize = append(attributes.Fields.uIntegersNullableSize, 64)
			}
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "uint", attributes.Tags["sortable"] == "true", true, 0)
		}
	}
}

func (e *entitySchema) buildIntField(attributes schemaFieldAttributes, min int64, max uint64) {
	attributes.Fields.integers = append(attributes.Fields.integers, attributes.Index)
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "int", attributes.Tags["sortable"] == "true", false, 0)
		}
	}
}

func (e *entitySchema) buildIntPointerField(attributes schemaFieldAttributes, min int64, max uint64) {
	attributes.Fields.integersNullable = append(attributes.Fields.integersNullable, attributes.Index)
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			switch attributes.TypeName {
			case "*int":
				attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 0)
			case "*int8":
				attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 8)
			case "*int16":
				attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 16)
			case "*int32":
				attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 32)
			case "*int64":
				attributes.Fields.integersNullableSize = append(attributes.Fields.integersNullableSize, 64)
			}
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "int", attributes.Tags["sortable"] == "true", true, 0)
		}
	}
}

func (e *entitySchema) buildEnumField(attributes schemaFieldAttributes, values []string) {
	attributes.Fields.stringsEnums = append(attributes.Fields.stringsEnums, attributes.Index)
	enumName := attributes.Field.Name
	if customName, has := attributes.Tags["enumName"]; has {
		enumName = customName
	}
	required := attributes.Tags["required"] == "true"
	for i, columnName := range attributes.GetColumnNames() {
		def := initEnumDefinition(enumName, values, required)
		if i == 0 {
			attributes.Fields.enums = append(attributes.Fields.enums, def)
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "TAG", "enum", attributes.Tags["sortable"] == "true", !required, 0)
		}
	}
}

func (e *entitySchema) buildStringField(attributes schemaFieldAttributes) {
	attributes.Fields.strings = append(attributes.Fields.strings, attributes.Index)
	isRequired := attributes.Tags["required"] == "true"
	for i, columnName := range attributes.GetColumnNames() {
		stringLength := 255
		if i == 0 {
			length := attributes.Tags["length"]
			if length == "max" {
				stringLength = 16777215
			} else if length != "" {
				stringLength, _ = strconv.Atoi(length)
			}
			attributes.Fields.stringMaxLengths = append(attributes.Fields.stringMaxLengths, stringLength)
			attributes.Fields.stringsRequired = append(attributes.Fields.stringsRequired, isRequired)
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "TEXT", "string", attributes.Tags["sortable"] == "true", !isRequired, 0)
		}
	}
}

func (e *entitySchema) buildBytesField(attributes schemaFieldAttributes) {
	attributes.Fields.bytes = append(attributes.Fields.bytes, attributes.Index)
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
	}
}

func (e *entitySchema) buildStringSliceField(attributes schemaFieldAttributes, values []string) {
	attributes.Fields.sliceStringsSets = append(attributes.Fields.sliceStringsSets, attributes.Index)
	enumName := attributes.Field.Name
	if customName, has := attributes.Tags["enumName"]; has {
		enumName = customName
	}
	required := attributes.Tags["required"] == "true"
	for i, columnName := range attributes.GetColumnNames() {
		def := initEnumDefinition(enumName, values, required)
		if i == 0 {
			attributes.Fields.sets = append(attributes.Fields.sets, def)
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "TAG", "set", attributes.Tags["sortable"] == "true", !required, 0)
		}
	}
}

func (e *entitySchema) buildBoolField(attributes schemaFieldAttributes) {
	attributes.Fields.booleans = append(attributes.Fields.booleans, attributes.Index)
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "bool", attributes.Tags["sortable"] == "true", false, 0)
		}
	}
}

func (e *entitySchema) buildBoolPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.booleansNullable = append(attributes.Fields.booleansNullable, attributes.Index)
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "bool", attributes.Tags["sortable"] == "true", true, 0)
		}
	}
}

func (e *entitySchema) buildFloatField(attributes schemaFieldAttributes) {
	attributes.Fields.floats = append(attributes.Fields.floats, attributes.Index)
	precision := 8
	decimalSize := -1
	unsigned := false
	floatBitSize := 32
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			if attributes.TypeName == "float64" {
				floatBitSize = 64
			}
			if floatBitSize == 32 {
				precision = 4
				attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
			} else {
				attributes.Fields.floatsSize = append(attributes.Fields.floatsSize, 64)
			}
			precisionAttribute, has := attributes.Tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				decimal, isDecimal := attributes.Tags["decimal"]
				if isDecimal {
					decimalArgs := strings.Split(decimal, ",")
					precision, _ = strconv.Atoi(decimalArgs[1])
					decimalSize, _ = strconv.Atoi(decimalArgs[0])
					decimalSize -= precision
				}
			}
			unsigned = attributes.Tags["unsigned"] == "true"
			attributes.Fields.floatsPrecision = append(attributes.Fields.floatsPrecision, precision)
			attributes.Fields.floatsDecimalSize = append(attributes.Fields.floatsDecimalSize, decimalSize)
			attributes.Fields.floatsUnsigned = append(attributes.Fields.floatsUnsigned, unsigned)
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "float", attributes.Tags["sortable"] == "true", false, precision)
		}
	}
}

func (e *entitySchema) buildFloatPointerField(attributes schemaFieldAttributes) {
	attributes.Fields.floatsNullable = append(attributes.Fields.floatsNullable, attributes.Index)
	unsigned := false
	precision := 8
	decimalSize := -1
	floatBitSize := 32
	for i, columnName := range attributes.GetColumnNames() {
		if i == 0 {
			unsigned = attributes.Tags["unsigned"] == "true"
			if attributes.TypeName == "*float64" {
				floatBitSize = 64
			}
			if floatBitSize == 32 {
				precision = 4
				attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 32)
			} else {
				attributes.Fields.floatsNullableSize = append(attributes.Fields.floatsNullableSize, 64)
			}
			precisionAttribute, has := attributes.Tags["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			} else {
				decimal, isDecimal := attributes.Tags["decimal"]
				if isDecimal {
					decimalArgs := strings.Split(decimal, ",")
					precision, _ = strconv.Atoi(decimalArgs[1])
					decimalSize, _ = strconv.Atoi(decimalArgs[0])
					decimalSize -= precision
				}
			}
			attributes.Fields.floatsNullablePrecision = append(attributes.Fields.floatsNullablePrecision, precision)
			attributes.Fields.floatsNullableDecimalSize = append(attributes.Fields.floatsNullableDecimalSize, decimalSize)
			attributes.Fields.floatsNullableUnsigned = append(attributes.Fields.floatsNullableUnsigned, unsigned)
		}
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", "float", attributes.Tags["sortable"] == "true", true, precision)
		}
	}
}

func (e *entitySchema) buildTimePointerField(attributes schemaFieldAttributes) {
	_, hasTime := attributes.Tags["time"]
	fieldName := attributes.Prefix + attributes.Field.Name
	forceTime := fieldName == "CreatedAt" || fieldName == "UpdatedAt"
	if hasTime || forceTime {
		attributes.Fields.timesNullable = append(attributes.Fields.timesNullable, attributes.Index)
	} else {
		attributes.Fields.datesNullable = append(attributes.Fields.datesNullable, attributes.Index)
	}
	goKind := "date"
	if hasTime || forceTime {
		goKind = "time"
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", goKind, attributes.Tags["sortable"] == "true", true, 0)
		}
	}
}

func (e *entitySchema) buildTimeField(attributes schemaFieldAttributes) {
	_, hasTime := attributes.Tags["time"]
	fieldName := attributes.Prefix + attributes.Field.Name
	forceTime := fieldName == "CreatedAt" || fieldName == "UpdatedAt"
	if hasTime || forceTime {
		attributes.Fields.times = append(attributes.Fields.times, attributes.Index)
	} else {
		attributes.Fields.dates = append(attributes.Fields.dates, attributes.Index)
	}
	goKind := "date"
	if hasTime || forceTime {
		goKind = "time"
	}
	for _, columnName := range attributes.GetColumnNames() {
		e.fieldDefinitions[columnName] = attributes
		if attributes.Tags["searchable"] == "true" {
			e.markSearchableField(columnName, "NUMERIC", goKind, attributes.Tags["sortable"] == "true", false, 0)
		}
	}
}

func (e *entitySchema) buildStructField(attributes schemaFieldAttributes, registry *registry,
	schemaTags map[string]map[string]string) error {
	var parents []int
	if attributes.Parents != nil {
		parents = append(parents, attributes.Parents...)
	}
	parents = append(parents, attributes.Index)
	attributes.Fields.structs = append(attributes.Fields.structs, attributes.Index)
	subPrefix := ""
	if !attributes.Field.Anonymous {
		subPrefix = attributes.Field.Name
	}
	subFields, err := e.buildTableFields(attributes.Field.Type, registry, 0, subPrefix, parents, schemaTags, "")
	if err != nil {
		return err
	}
	attributes.Fields.structsFields = append(attributes.Fields.structsFields, subFields)
	return nil
}

func extractTags(registry *registry, entityType reflect.Type, prefix string) (fields map[string]map[string]string) {
	fields = make(map[string]map[string]string)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		for k, v := range extractTag(registry, field) {
			fields[prefix+k] = v
		}
		_, hasIgnore := fields[field.Name]["ignore"]
		if hasIgnore {
			continue
		}
		name := prefix + field.Name
		refOne := ""
		hasRef := false
		if field.Type.Kind().String() == "ptr" {
			refName := field.Type.Elem().String()
			_, hasRef = registry.entities[refName]
			if hasRef {
				refOne = refName
			}
		}

		query, hasQuery := field.Tag.Lookup("query")
		queryOne, hasQueryOne := field.Tag.Lookup("queryOne")
		if hasQuery {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[name]["query"] = query
		}
		if hasQueryOne {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[field.Name]["queryOne"] = queryOne
		}
		if hasRef {
			if fields[name] == nil {
				fields[name] = make(map[string]string)
			}
			fields[name]["ref"] = refOne
			fields[name]["refPath"] = field.Name
			if prefix != "" {
				fields[name]["refPath"] = prefix + "." + field.Name
			}
		}
	}
	return
}

func extractTag(registry *registry, field reflect.StructField) map[string]map[string]string {
	tag, ok := field.Tag.Lookup("orm")
	if ok {
		args := strings.Split(tag, ";")
		length := len(args)
		var attributes = make(map[string]string, length)
		for j := 0; j < length; j++ {
			arg := strings.Split(args[j], "=")
			if len(arg) == 1 {
				attributes[arg[0]] = "true"
			} else {
				attributes[arg[0]] = arg[1]
			}
		}
		return map[string]map[string]string{field.Name: attributes}
	} else if field.Type.Kind().String() == "struct" {
		t := field.Type.String()
		if t != "time.Time" {
			prefix := ""
			if !field.Anonymous {
				prefix = field.Name
			}
			return extractTags(registry, field.Type, prefix)
		}
	}
	return make(map[string]map[string]string)
}

func (fields *tableFields) buildColumnNames(subFieldPrefix string) []string {
	fieldsQuery := ""
	columns := make([]string, 0)
	ids := fields.uIntegers
	ids = append(ids, fields.references...)
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.floats...)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.dates...)
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.uIntegersNullable...)
	ids = append(ids, fields.integersNullable...)
	ids = append(ids, fields.stringsEnums...)
	ids = append(ids, fields.bytes...)
	ids = append(ids, fields.sliceStringsSets...)
	ids = append(ids, fields.booleansNullable...)
	ids = append(ids, fields.floatsNullable...)
	ids = append(ids, fields.timesNullable...)
	ids = append(ids, fields.datesNullable...)
	for _, index := range ids {
		l := fields.arrays[index]
		if l > 0 {
			for i := 1; i <= l; i++ {
				name := subFieldPrefix + fields.fields[index].Name + "_" + strconv.Itoa(i)
				columns = append(columns, name)
				fieldsQuery += ",`" + name + "`"
			}
		} else {
			name := subFieldPrefix + fields.fields[index].Name
			columns = append(columns, name)
			fieldsQuery += ",`" + name + "`"
		}
	}
	for i, subFields := range fields.structsFields {
		field := fields.fields[fields.structs[i]]
		prefixName := subFieldPrefix
		if !field.Anonymous {
			prefixName += field.Name
		}
		subColumns := subFields.buildColumnNames(prefixName)
		columns = append(columns, subColumns...)
	}
	return columns
}

var scanIntNullablePointer = func() any {
	return &sql.NullInt64{}
}

var pointerUintNullableScan = func(val any) any {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return uint64(v.Int64)
	}
	return nil
}

var pointerIntNullableScan = func(val any) any {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return v.Int64
	}
	return nil
}

var scanStringNullablePointer = func() any {
	return &sql.NullString{}
}

var pointerStringNullableScan = func(val any) any {
	v := val.(*sql.NullString)
	if v.Valid {
		return v.String
	}
	return nil
}

var scanBoolPointer = func() any {
	v := false
	return &v
}

var pointerBoolScan = func(val any) any {
	return *val.(*bool)
}

var scanBoolNullablePointer = func() any {
	return &sql.NullBool{}
}

var pointerBoolNullableScan = func(val any) any {
	v := val.(*sql.NullBool)
	if v.Valid {
		return v.Bool
	}
	return nil
}

var scanFloatNullablePointer = func() any {
	return &sql.NullFloat64{}
}

var pointerFloatNullableScan = func(val any) any {
	v := val.(*sql.NullFloat64)
	if v.Valid {
		return v.Float64
	}
	return nil
}

var scanStringPointer = func() any {
	v := ""
	return &v
}

var pointerStringScan = func(val any) any {
	return *val.(*string)
}
