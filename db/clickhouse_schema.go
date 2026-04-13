package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	chcolumn "github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/jimsmart/schema"
	"go.uber.org/zap"
)

const clickhouseInsertableColumnsQuery = `
	SELECT
		name,
		type,
		default_kind
	FROM
		system.columns
	WHERE
		database = $1
		AND table = $2
		AND is_subcolumn = 0
	ORDER BY
		position
`

func (l *Loader) loadClickhouseTables() error {
	tableNames, err := schema.TableNames(l.DB)
	if err != nil {
		return fmt.Errorf("retrieving table names: %w", err)
	}

	seenCursorTable := false
	seenHistoryTable := false
	for _, schemaTableName := range tableNames {
		schemaName := schemaTableName[0]
		tableName := schemaTableName[1]
		l.logger.Debug("processing schema's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if schemaName != l.schema {
			continue
		}

		columnByName, err := l.loadClickhouseInsertableColumns(schemaName, tableName)
		if err != nil {
			return fmt.Errorf("get clickhouse columns for %s.%s: %w", schemaName, tableName, err)
		}

		if tableName == CURSORS_TABLE {
			if err := l.validateCursorTableInfo(columnByName); err != nil {
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			seenCursorTable = true
		}
		if tableName == HISTORY_TABLE {
			seenHistoryTable = true
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
		l.logger.Warn("history table not found but reorg handling is enabled",
			zap.String("schema_name", l.schema),
			zap.String("table_name", HISTORY_TABLE),
		)
	}

	l.cursorTable = l.tables[CURSORS_TABLE]

	return nil
}

func (l *Loader) loadClickhouseInsertableColumns(schemaName, tableName string) (map[string]*ColumnInfo, error) {
	rows, err := l.Query(clickhouseInsertableColumnsQuery, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnByName := map[string]*ColumnInfo{}
	for rows.Next() {
		var name string
		var typeName string
		var defaultKind sql.NullString
		if err := rows.Scan(&name, &typeName, &defaultKind); err != nil {
			return nil, fmt.Errorf("scan clickhouse column metadata: %w", err)
		}

		if shouldSkipClickhouseColumn(defaultKind.String) {
			continue
		}

		column, err := chcolumn.Type(typeName).Column(name, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("parse clickhouse column %q type %q: %w", name, typeName, err)
		}

		columnByName[name] = &ColumnInfo{
			name:             name,
			escapedName:      EscapeIdentifier(name),
			databaseTypeName: typeName,
			scanType:         column.ScanType(),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read clickhouse column metadata: %w", err)
	}

	return columnByName, nil
}

func shouldSkipClickhouseColumn(defaultKind string) bool {
	switch strings.ToUpper(defaultKind) {
	case "ALIAS", "MATERIALIZED":
		return true
	default:
		return false
	}
}

func (l *Loader) validateCursorTableInfo(columns map[string]*ColumnInfo) error {
	if len(columns) != 4 {
		return &SystemTableError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
	}

	columnsCheck := map[string]string{
		"block_num": "int64",
		"block_id":  "string",
		"cursor":    "string",
		"id":        "string",
	}
	for columnName, column := range columns {
		expectedType, found := columnsCheck[columnName]
		if !found {
			return &SystemTableError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}

		actualType := column.scanType.Kind().String()
		if expectedType != actualType {
			return &SystemTableError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}

	if len(columnsCheck) != 0 {
		for columnName := range columnsCheck {
			return &SystemTableError{fmt.Errorf("missing expected column %q in cursors table", columnName)}
		}
	}

	return nil
}
