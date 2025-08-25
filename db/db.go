package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"sync"

	"github.com/jimsmart/schema"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var CURSORS_TABLE = "cursors"
var HISTORY_TABLE = "substreams_history"
var CLICKHOUSE_CLUSTER = ""

// Make the typing a bit easier
type OrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{OrderedMap: orderedmap.New[K, V]()}
}

type SystemTableError struct {
	error
}

type Loader struct {
	*sql.DB

	database     string
	schema       string
	entries      *OrderedMap[string, *OrderedMap[string, *Operation]]
	entriesCount uint64
	tables       map[string]*TableInfo
	cursorTable  *TableInfo

	handleReorgs             bool
	batchBlockFlushInterval  int
	batchRowFlushInterval    int
	liveBlockFlushInterval   int
	moduleMismatchMode       OnModuleHashMismatch
	maxFlushRetries          int
	sleepBetweenFlushRetries time.Duration

	logger *zap.Logger
	tracer logging.Tracer

	testTx *TestTx // used for testing: if non-nil, 'loader.BeginTx()' will return this object instead of a real *sql.Tx

	// async flush
	mu                 sync.Mutex
	cond               *sync.Cond
	activeFlushes      int
	maxParallelFlushes int
}

func NewLoader(
	psqlDsn string,
	batchBlockFlushInterval int,
	batchRowFlushInterval int,
	liveBlockFlushInterval int,
	moduleMismatchMode OnModuleHashMismatch,
	handleReorgs *bool,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Loader, error) {
	dsn, err := ParseDSN(psqlDsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	db, err := sql.Open(dsn.driver, dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	l := &Loader{
		DB:                       db,
		database:                 dsn.database,
		schema:                   dsn.schema,
		entries:                  NewOrderedMap[string, *OrderedMap[string, *Operation]](),
		tables:                   map[string]*TableInfo{},
		batchBlockFlushInterval:  batchBlockFlushInterval,
		batchRowFlushInterval:    batchRowFlushInterval,
		maxFlushRetries:          3,
		sleepBetweenFlushRetries: 5 * time.Second,
		liveBlockFlushInterval:   liveBlockFlushInterval,
		moduleMismatchMode:       moduleMismatchMode,
		logger:                   logger,
		tracer:                   tracer,
		maxParallelFlushes:       3,
	}
	l.mu = sync.Mutex{}
	l.cond = sync.NewCond(&l.mu)
	_, err = l.tryDialect()
	if err != nil {
		return nil, fmt.Errorf("dialect not found: %s", err)
	}

	if handleReorgs == nil {
		// automatic detection
		l.handleReorgs = !l.getDialect().OnlyInserts()
	} else {
		l.handleReorgs = *handleReorgs
	}

	if l.handleReorgs && l.getDialect().OnlyInserts() {
		return nil, fmt.Errorf("driver %s does not support reorg handling. You must use set a non-zero undo-buffer-size", dsn.driver)
	}

	logger.Info("created new DB loader",
		zap.Int("batch_block_flush_interval", batchBlockFlushInterval),
		zap.Int("batch_row_flush_interval", batchRowFlushInterval),
		zap.Int("live_block_flush_interval", liveBlockFlushInterval),
		zap.String("driver", dsn.driver),
		zap.String("database", dsn.database),
		zap.String("schema", dsn.schema),
		zap.String("user", dsn.username),
		zap.Stringer("password", obfuscatedString(dsn.password)),
		zap.String("host", dsn.host),
		zap.Int64("port", dsn.port),
		zap.Stringer("on_module_hash_mismatch", moduleMismatchMode),
		zap.Bool("handle_reorgs", l.handleReorgs),
		zap.String("dialect", fmt.Sprintf("%t", l.getDialect())),
	)

	return l, nil
}

type Tx interface {
	Rollback() error
	Commit() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (l *Loader) Begin() (Tx, error) {
	return l.BeginTx(context.Background(), nil)
}

func (l *Loader) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	if l.testTx != nil {
		return l.testTx, nil
	}
	return l.DB.BeginTx(ctx, opts)
}

func (l *Loader) BatchBlockFlushInterval() int {
	return l.batchBlockFlushInterval
}

func (l *Loader) LiveBlockFlushInterval() int {
	return l.liveBlockFlushInterval
}

func (l *Loader) GetPrimaryKey(tableName string, pk string) (map[string]string, error) {
	table, found := l.tables[tableName]
	if !found {
		return nil, fmt.Errorf("unknown table %q", tableName)
	}
	if len(table.primaryColumns) == 1 {
		return map[string]string{table.primaryColumns[0].name: pk}, nil
	}
	parts := strings.Split(pk, "/")
	if len(parts) != len(table.primaryColumns) {
		return nil, fmt.Errorf("composite primary key value count mismatch for table %q: got %d parts, expected %d", tableName, len(parts), len(table.primaryColumns))
	}
	res := make(map[string]string, len(parts))
	for i, col := range table.primaryColumns {
		res[col.name] = parts[i]
	}
	return res, nil
}

func (l *Loader) FlushNeeded() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	totalRows := 0
	// todo keep a running count when inserting/deleting rows directly
	for pair := l.entries.Oldest(); pair != nil; pair = pair.Next() {
		totalRows += pair.Value.Len()
	}
	return totalRows > l.batchRowFlushInterval
}

// WaitForAllFlushes blocks until there are no in-flight async flushes.
func (l *Loader) WaitForAllFlushes() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.activeFlushes > 0 {
		l.cond.Wait()
	}
}

// FlushAsync triggers a non-blocking flush. Blocks if the maximum number of parallel flushes is reached until a flush is completed.
func (l *Loader) FlushAsync(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastFinalBlock uint64) bool {
	l.mu.Lock()
	for l.activeFlushes >= l.maxParallelFlushes {
		l.cond.Wait()
	}
	// Snapshot entries and replace with a fresh buffer
	snapshot := l.entries
	l.entries = NewOrderedMap[string, *OrderedMap[string, *Operation]]()
	l.activeFlushes++
	l.mu.Unlock()

	l.logger.Info("async flush started", zap.Int("active_flushes", l.activeFlushes-1))

	go func() {
		// cleanup defer
		defer func() {
			l.mu.Lock()
			l.activeFlushes--
			l.cond.Broadcast()
			l.mu.Unlock()
		}()

		// Create a shallow copy loader that uses the snapshot for flushing
		tl := *l
		tl.entries = snapshot

		// Perform a single flush attempt with its own transaction (reusing existing logic but avoiding reset())
		tx, err := l.BeginTx(ctx, nil)
		if err != nil {
			l.logger.Warn("async flush: failed to begin tx", zap.Error(err))
			return
		}
		committed := false
		// rollback defer - runs before cleanup defer
		defer func() {
			if !committed {
				if err := tx.Rollback(); err != nil {
					l.logger.Warn("async flush: rollback failed", zap.Error(err))
				}
			}
		}()

		start := time.Now()
		rowFlushedCount, err := tl.getDialect().Flush(tx, ctx, &tl, outputModuleHash, lastFinalBlock)
		if err != nil {
			l.logger.Warn("async flush: dialect flush failed", zap.Error(err))
			return
		}

		// Update cursor for the snapshot
		if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
			l.logger.Warn("async flush: update cursor failed", zap.Error(err))
			return
		}

		if err := tx.Commit(); err != nil {
			l.logger.Warn("async flush: commit failed", zap.Error(err))
			return
		}
		committed = true

		l.logger.Info("async flush complete",
			zap.Int("row_count", rowFlushedCount),
			zap.Duration("took", time.Since(start)))
	}()

	return true
}

func (l *Loader) LoadTables() error {
	schemaTables, err := schema.Tables(l.DB)
	if err != nil {
		return fmt.Errorf("retrieving table and schema: %w", err)
	}

	seenCursorTable := false
	seenHistoryTable := false
	for schemaTableName, columns := range schemaTables {
		schemaName := schemaTableName[0]
		tableName := schemaTableName[1]
		l.logger.Debug("processing schema's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if schemaName != l.schema {
			continue
		}

		if tableName == CURSORS_TABLE {
			if err := l.validateCursorTables(columns); err != nil {
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			seenCursorTable = true
		}
		if tableName == HISTORY_TABLE {
			seenHistoryTable = true
		}

		columnByName := make(map[string]*ColumnInfo, len(columns))
		for _, f := range columns {
			columnByName[f.Name()] = &ColumnInfo{
				name:             f.Name(),
				escapedName:      EscapeIdentifier(f.Name()),
				databaseTypeName: f.DatabaseTypeName(),
				scanType:         f.ScanType(),
			}
		}

		key, err := schema.PrimaryKey(l.DB, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("get primary key: %w", err)
		}

		l.tables[tableName], err = NewTableInfo(schemaName, tableName, key, columnByName)
		if err != nil {
			return fmt.Errorf("invalid table: %w", err)
		}
	}

	if !seenCursorTable {
		return &SystemTableError{fmt.Errorf(`%s.%s table is not found`, EscapeIdentifier(l.schema), CURSORS_TABLE)}
	}
	if l.handleReorgs && !seenHistoryTable {
		return &SystemTableError{fmt.Errorf("%s.%s table is not found and reorgs handling is enabled", EscapeIdentifier(l.schema), HISTORY_TABLE)}
	}

	l.cursorTable = l.tables[CURSORS_TABLE]

	return nil
}

func (l *Loader) validateCursorTables(columns []*sql.ColumnType) (err error) {
	if len(columns) != 4 {
		return &SystemTableError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
	}
	columnsCheck := map[string]string{
		"block_num": "int64",
		"block_id":  "string",
		"cursor":    "string",
		"id":        "string",
	}
	for _, f := range columns {
		columnName := f.Name()
		if _, found := columnsCheck[columnName]; !found {
			return &SystemTableError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}
		expectedType := columnsCheck[columnName]
		actualType := f.ScanType().Kind().String()
		if expectedType != actualType {
			return &SystemTableError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}
	if len(columnsCheck) != 0 {
		for k := range columnsCheck {
			return &SystemTableError{fmt.Errorf("missing column %q from cursors", k)}
		}
	}
	key, err := schema.PrimaryKey(l.DB, l.schema, CURSORS_TABLE)
	if err != nil {
		return &SystemTableError{fmt.Errorf("failed getting primary key: %w", err)}
	}
	if len(key) == 0 {
		return &SystemTableError{fmt.Errorf("primary key not found: %w", err)}
	}
	if key[0] != "id" {
		return &SystemTableError{fmt.Errorf("column 'id' should be primary key not %q", key[0])}
	}
	return nil
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetIdentifier() string {
	return fmt.Sprintf("%s/%s", l.database, l.schema)
}

func (l *Loader) GetColumnsForTable(name string) []string {
	columns := make([]string, 0, len(l.tables[name].columnsByName))
	for column := range l.tables[name].columnsByName {
		// check if column is empty
		if len(column) > 0 {
			columns = append(columns, column)
		}
	}
	return columns
}

func (l *Loader) GetAvailableTablesInSchema() []string {
	tables := make([]string, len(l.tables))
	i := 0
	for table := range l.tables {
		tables[i] = table
		i++
	}
	return tables
}

func (l *Loader) GetDatabase() string {
	return l.database
}

func (l *Loader) GetSchema() string {
	return l.schema
}

func (l *Loader) HasTable(tableName string) bool {
	if _, found := l.tables[tableName]; found {
		return true
	}
	return false
}

func (l *Loader) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("entries_count", l.entriesCount)
	return nil
}

// Setup creates the schema, cursors and history table where the <schemaBytes> is a byte array
// taken from somewhere.
func (l *Loader) Setup(ctx context.Context, schemaSql string, withPostgraphile bool) error {
	if schemaSql != "" {
		if err := l.getDialect().ExecuteSetupScript(ctx, l, schemaSql); err != nil {
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	if err := l.setupCursorTable(ctx, withPostgraphile); err != nil {
		return fmt.Errorf("setup cursor table: %w", err)
	}

	if err := l.setupHistoryTable(ctx, withPostgraphile); err != nil {
		return fmt.Errorf("setup history table: %w", err)
	}

	return nil
}

func (l *Loader) setupCursorTable(ctx context.Context, withPostgraphile bool) error {
	query := l.getDialect().GetCreateCursorQuery(l.schema, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

func (l *Loader) setupHistoryTable(ctx context.Context, withPostgraphile bool) error {
	if l.getDialect().OnlyInserts() {
		return nil
	}
	query := l.getDialect().GetCreateHistoryQuery(l.schema, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

func (l *Loader) getDialect() dialect {
	d, _ := l.tryDialect()
	return d
}

func (l *Loader) tryDialect() (dialect, error) {
	dt := fmt.Sprintf("%T", l.DB.Driver())
	d, ok := driverDialect[dt]
	if !ok {
		return nil, UnknownDriverError{Driver: dt}
	}
	return d, nil
}

type obfuscatedString string

func (s obfuscatedString) String() string {
	if len(s) == 0 {
		return "<unset>"
	}

	return "********"
}
