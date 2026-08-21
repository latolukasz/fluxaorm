package fluxaorm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// ErrOutboxDispatchMark wraps a failure to flip staged outbox rows to
// dispatched. The rows are safe: they stay pending, the relay republishes them,
// and JetStream's dedup window absorbs the duplicate. Distinguishable from a
// publish failure so a caller can tell "not delivered" from "delivered twice".
var ErrOutboxDispatchMark = errors.New("cdc outbox dispatch mark failed")

// CDCOutboxEntity is the transactional outbox table. It is declared here rather
// than by the application so the column set is a compile-time fact on both
// sides: fluxaorm writes it with raw SQL, the application reads it through the
// generated provider. Register it with `registry.RegisterEntity` and it
// generates, alters and queries like any other entity.
//
// An entity tagged `orm:"outbox"` gets one row per write, inside that
// write's own transaction. With `dirty=` the row is a delivery guarantee; alone
// it is a change log. See the package docs for the full truth table.
type CDCOutboxEntity struct {
	ID           uint64     `orm:"table=cdc_outbox"`
	Status       string     `orm:"enum=pending,dispatched,stored;enumName=CDCOutboxStatus;required"`
	EntityName   string     `orm:"required;length=100"`
	EntityID     uint64     `orm:"required"`
	Op           uint8      `orm:"required"`
	Streams      string     `orm:"length=1000"`
	NatsPool     string     `orm:"length=50"`
	Payload      string     `orm:"length=max;required"`
	DispatchedAt *time.Time `orm:"time"`
	CreatedAt    time.Time  `orm:"time"`
}

// Indexes covers both readers with one index: the relay scans pending rows past
// a grace period, the purge scans terminal rows past a retention window.
func (e CDCOutboxEntity) Indexes() [][]string {
	return [][]string{{"Status", "CreatedAt"}}
}

const (
	// CDCOutboxPending is a row whose event is not yet known to be on the wire.
	CDCOutboxPending = "pending"
	// CDCOutboxDispatched is a row whose event was published successfully.
	CDCOutboxDispatched = "dispatched"
	// CDCOutboxStored is a row from an entity tagged `outbox` without
	// `dirty=`. There is no stream to publish to, so it is born terminal.
	CDCOutboxStored = "stored"
)

// cdcOutboxStreamSeparator joins the stream names in the Streams column.
// fluxaorm owns this encoding on write; readers split on it.
const cdcOutboxStreamSeparator = ","

const cdcOutboxInsertSQL = "INSERT INTO `%s` (`ID`,`Status`,`EntityName`,`EntityID`,`Op`,`Streams`,`NatsPool`,`Payload`,`CreatedAt`) VALUES (?,?,?,?,?,?,?,?,?)"

// resolvedCDCOutbox is the registry's handle on the outbox table, resolved once
// at Validate() time from the registered CDCOutboxEntity's schema.
type resolvedCDCOutbox struct {
	tableName string
	poolCode  string
}

// resolveCDCOutbox locates the registered CDCOutboxEntity and checks that every
// entity routing rows to it writes to the same MySQL pool. Cross-pool would
// mean the outbox row is not in the source write's transaction at all, which is
// the entire guarantee.
func resolveCDCOutbox(e *engineImplementation) error {
	reg := e.registry
	outboxType := reflect.TypeOf(CDCOutboxEntity{})

	var tagged []string
	for _, schema := range reg.entitySchemas {
		if schema.outbox {
			tagged = append(tagged, schema.t.Name())
		}
	}
	if len(tagged) == 0 {
		return nil
	}
	// Map iteration order is random; sort so the error below names the same
	// entity every run rather than a different one each restart.
	sort.Strings(tagged)

	schema, has := reg.entitySchemas[outboxType]
	if !has {
		return fmt.Errorf(
			"entity '%s' is tagged `orm:\"outbox\"` but fluxaorm.CDCOutboxEntity is not registered; add registry.RegisterEntity(fluxaorm.CDCOutboxEntity{})",
			tagged[0])
	}
	reg.cdcOutbox = &resolvedCDCOutbox{tableName: schema.tableName, poolCode: schema.mysqlPoolCode}

	for _, s := range reg.entitySchemas {
		if !s.outbox {
			continue
		}
		if s.mysqlPoolCode != schema.mysqlPoolCode {
			return fmt.Errorf(
				"entity '%s' is tagged `orm:\"outbox\"` on mysql pool '%s' but the outbox table is on pool '%s'; the outbox row would not be in the same transaction",
				s.t.Name(), s.mysqlPoolCode, schema.mysqlPoolCode)
		}
		if err := checkOutboxStreamsSharePool(reg, s); err != nil {
			return err
		}
	}
	return nil
}

// checkOutboxStreamsSharePool rejects an outbox entity whose CDC streams live on
// different NATS pools. A row carries a single NatsPool, so the relay would
// republish every one of that entity's streams to whichever pool came first -
// right subjects, wrong server, no error anywhere.
func checkOutboxStreamsSharePool(reg *engineRegistryImplementation, schema *entitySchema) error {
	if len(schema.dirtyStreams) < 2 {
		return nil
	}
	first := reg.dirtyStreams[schema.dirtyStreams[0]].options.NatsPool
	for _, name := range schema.dirtyStreams[1:] {
		if pool := reg.dirtyStreams[name].options.NatsPool; pool != first {
			return fmt.Errorf(
				"entity '%s' is tagged `orm:\"outbox\"` but its CDC streams span nats pools ('%s' on '%s', '%s' on '%s'); an outbox row records only one pool",
				schema.t.Name(), schema.dirtyStreams[0], first, name, pool)
		}
	}
	return nil
}

// stageDirtyOutbox queues one outbox INSERT per written entity that routes to
// the outbox, onto the same database pipeline as the write itself - so the row
// commits with the entity or not at all.
//
// Returns the IDs of rows still awaiting delivery. Store-only rows are born
// terminal and are deliberately absent from that slice. The IDs are returned
// rather than kept on the context because runPostCommit is queued once per
// Save: a context-level slice would let the first closure mark rows belonging
// to a later Save that has not published yet.
func (orm *ormImplementation) stageDirtyOutbox(list []*pendingWrite) ([]uint64, error) {
	outbox := orm.engine.registry.cdcOutbox
	if outbox == nil || len(orm.engine.registry.dirtyPublishers) == 0 {
		return nil, nil
	}

	var pending []uint64
	now := time.Now()
	for _, w := range list {
		eventType, _ := w.entity.PrivateFlushEvent()
		if eventType == 0 {
			continue
		}
		et := reflect.TypeOf(w.entity)
		if et.Kind() == reflect.Ptr {
			et = et.Elem()
		}
		publisher, hasPub := orm.engine.registry.dirtyPublishers[et]
		if !hasPub || !publisher.outbox {
			continue
		}
		op := dirtyOpFromFlushType(eventType)
		payload, err := publisher.buildEvent(w.entity, op, nil)
		if err != nil {
			return nil, fmt.Errorf("build outbox payload for %s: %w", publisher.entityName, err)
		}
		if payload == nil {
			continue
		}
		// Hand the exact bytes to the publisher so the stored row and the
		// published message stay byte-identical, and a replay keeps its dedup id.
		w.dirtyPayload = payload

		status := CDCOutboxStored
		streams := ""
		natsPool := ""
		if len(publisher.streams) > 0 {
			status = CDCOutboxPending
			names := make([]string, len(publisher.streams))
			for i, s := range publisher.streams {
				names[i] = string(s)
			}
			streams = strings.Join(names, cdcOutboxStreamSeparator)
			natsPool = orm.engine.registry.dirtyStreams[publisher.streams[0]].options.NatsPool
		}

		id := orm.engine.NextID()
		orm.DatabasePipeLine(outbox.poolCode).AddQueryForTable(
			outbox.tableName,
			fmt.Sprintf(cdcOutboxInsertSQL, outbox.tableName),
			id, status, publisher.entityName, w.entity.GetID(), uint8(op), streams, natsPool, string(payload), now,
		)
		if status == CDCOutboxPending {
			pending = append(pending, id)
		}
	}
	return pending, nil
}

// markOutboxDispatched flips the rows whose event this Save just published.
// Runs post-commit, where orm.tx is always nil, so it is a single autocommit
// UPDATE rather than an orm.Save that would re-enter writeEntities.
func (orm *ormImplementation) markOutboxDispatched(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	outbox := orm.engine.registry.cdcOutbox
	args := make([]any, 0, len(ids)+1)
	args = append(args, time.Now())
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	sql := fmt.Sprintf(
		"UPDATE `%s` SET `Status` = '%s', `DispatchedAt` = ? WHERE `ID` IN (%s)",
		outbox.tableName, CDCOutboxDispatched, strings.Join(placeholders, ","))
	if _, err := orm.DB(outbox.poolCode).Exec(orm, sql, args...); err != nil {
		return fmt.Errorf("%w: %w", ErrOutboxDispatchMark, err)
	}
	return nil
}

// CDCOutboxRelayResult reports one page of relay work.
//
// Dispatched is the count of rows actually marked delivered, and it is what a
// paging caller must loop on. Fetched only says how many rows were read: a page
// that fails to publish stays pending, so paging on Fetched would re-read the
// same rows forever. Published is the message count per NATS pool - one row can
// contribute several - and exists for a labelled metric, not for control flow.
type CDCOutboxRelayResult struct {
	Fetched    int
	Dispatched int
	Published  map[string]int
}

type outboxPendingRow struct {
	id         uint64
	entityName string
	op         uint8
	streams    string
	natsPool   string
	payload    string
}

const cdcOutboxSelectPendingSQL = "SELECT `ID`,`EntityName`,`Op`,`Streams`,`NatsPool`,`Payload` FROM `%s` " +
	"WHERE `Status` = '" + CDCOutboxPending + "' AND `CreatedAt` <= ? ORDER BY `ID` LIMIT %d"

// RelayCDCOutbox republishes one page of outbox rows whose inline publish never
// landed, then marks the ones that made it onto the wire.
//
// It lives here rather than in the application because the write side lives
// here: the two halves have to agree on the status values, the column meanings
// and above all the deterministic Nats-Msg-Id that JetStream dedups on. An
// application-side relay that drifted on any of those would turn every recovery
// into a duplicate delivery instead of a no-op.
//
// minAge skips rows still racing the ORM's own post-commit mark, and must stay
// well under the stream's DuplicateWindow (10m by default) so a republish is
// still deduped. Publish happens before the mark, so delivery is at-least-once:
// a row is never recorded as delivered before it is.
//
// A pool whose batch fails leaves its rows pending for the next call and
// contributes to the returned error; other pools still publish and mark. The
// result is populated either way.
func RelayCDCOutbox(ctx Context, minAge time.Duration, limit int) (CDCOutboxRelayResult, error) {
	result := CDCOutboxRelayResult{Published: map[string]int{}}

	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return result, errors.New("RelayCDCOutbox: unsupported context")
	}
	outbox := orm.engine.registry.cdcOutbox
	if outbox == nil {
		return result, nil
	}

	rows, err := readPendingOutboxRows(orm, outbox, minAge, limit)
	if err != nil {
		return result, err
	}
	result.Fetched = len(rows)
	if len(rows) == 0 {
		return result, nil
	}

	idsByPool := make(map[string][]uint64)
	msgsByPool := make(map[string][]*NatsMessage)
	for _, row := range rows {
		idsByPool[row.natsPool] = append(idsByPool[row.natsPool], row.id)
		for _, stream := range strings.Split(row.streams, cdcOutboxStreamSeparator) {
			msgsByPool[row.natsPool] = append(msgsByPool[row.natsPool],
				newDirtyMessage(NatsStreamName(stream), row.entityName, DirtyOp(row.op), []byte(row.payload)))
		}
	}

	var errs []error
	for poolCode, msgs := range msgsByPool {
		pool := orm.engine.Nats(poolCode)
		if pool == nil {
			errs = append(errs, fmt.Errorf("nats pool '%s' for outbox relay not configured", poolCode))
			continue
		}
		if publishErr := pool.PublishBatch(orm, msgs); publishErr != nil {
			errs = append(errs, fmt.Errorf("republish %d outbox events to pool %s: %w", len(msgs), poolCode, publishErr))
			continue
		}
		if markErr := orm.markOutboxDispatched(idsByPool[poolCode]); markErr != nil {
			errs = append(errs, markErr)
			continue
		}
		result.Dispatched += len(idsByPool[poolCode])
		result.Published[poolCode] = len(msgs)
	}

	return result, errors.Join(errs...)
}

func readPendingOutboxRows(
	orm *ormImplementation, outbox *resolvedCDCOutbox, minAge time.Duration, limit int,
) ([]outboxPendingRow, error) {
	query := fmt.Sprintf(cdcOutboxSelectPendingSQL, outbox.tableName, limit)
	sqlRows, closeRows, err := orm.DB(outbox.poolCode).Query(orm, query, time.Now().Add(-minAge))
	if err != nil {
		return nil, fmt.Errorf("read pending outbox rows: %w", err)
	}
	defer closeRows()

	var rows []outboxPendingRow
	for sqlRows.Next() {
		var r outboxPendingRow
		if scanErr := sqlRows.Scan(&r.id, &r.entityName, &r.op, &r.streams, &r.natsPool, &r.payload); scanErr != nil {
			return nil, fmt.Errorf("scan pending outbox row: %w", scanErr)
		}
		rows = append(rows, r)
	}

	return rows, nil
}

const cdcOutboxPurgeSQL = "DELETE FROM `%s` WHERE `Status` IN ('" + CDCOutboxDispatched + "','" + CDCOutboxStored + "') " +
	"AND `CreatedAt` <= ? ORDER BY `ID` LIMIT %d"

// PurgeCDCOutbox deletes terminal rows older than olderThan and reports how many
// went. Pending rows are never touched however old they get: one of those is an
// undelivered event, and dropping it is the silent loss the outbox exists to
// prevent. A stuck relay therefore shows up as a growing backlog, not as
// vanished events.
func PurgeCDCOutbox(ctx Context, olderThan time.Duration, limit int) (int, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, errors.New("PurgeCDCOutbox: unsupported context")
	}
	outbox := orm.engine.registry.cdcOutbox
	if outbox == nil {
		return 0, nil
	}

	res, err := orm.DB(outbox.poolCode).Exec(orm,
		fmt.Sprintf(cdcOutboxPurgeSQL, outbox.tableName, limit), time.Now().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("purge outbox rows: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge outbox rows: %w", err)
	}

	return int(deleted), nil
}

const cdcOutboxBacklogSQL = "SELECT COUNT(*), COALESCE(MIN(`CreatedAt`), NOW()) FROM `%s` WHERE `Status` = '" +
	CDCOutboxPending + "'"

// CDCOutboxBacklog reports how many events are undelivered and how old the
// oldest is. Both are alertable: a stalled relay is otherwise completely silent,
// because the rows are safe and nothing errors.
func CDCOutboxBacklog(ctx Context) (depth int, oldest time.Duration, err error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, 0, errors.New("CDCOutboxBacklog: unsupported context")
	}
	outbox := orm.engine.registry.cdcOutbox
	if outbox == nil {
		return 0, 0, nil
	}

	var oldestAt time.Time
	_, err = orm.DB(outbox.poolCode).QueryRow(orm,
		NewWhere(fmt.Sprintf(cdcOutboxBacklogSQL, outbox.tableName)), &depth, &oldestAt)
	if err != nil {
		return 0, 0, fmt.Errorf("read outbox backlog: %w", err)
	}
	if depth == 0 {
		return 0, 0, nil
	}

	return depth, time.Since(oldestAt), nil
}
