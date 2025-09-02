package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// Identifier pattern for quoted/unquoted identifiers to use across all regexes
const (
	// Captures any of: 'quoted name', "quoted name", or unquoted_name
	identifierPattern = `((?:'[^']*')|(?:"[^"]*")|(?:[a-zA-Z_][a-zA-Z0-9_]*))`
)

// Regex patterns for SQL statement matching
var (
	createDbPattern               = regexp.MustCompile(`(?i)^\s*CREATE\s+(DATABASE|SCHEMA)(\s+IF\s+NOT\s+EXISTS)?\s+` + identifierPattern)
	createTablePattern            = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE(\s+IF\s+NOT\s+EXISTS)?\s+` + identifierPattern)
	createMaterializedViewPattern = regexp.MustCompile(`(?i)^\s*CREATE\s+MATERIALIZED\s+VIEW(\s+IF\s+NOT\s+EXISTS)?\s+` + identifierPattern)
	createAnyViewPattern          = regexp.MustCompile(`(?i)^\s*CREATE(\s+OR\s+REPLACE)?\s+VIEW(\s+IF\s+NOT\s+EXISTS)?\s+` + identifierPattern)
	createAnyFunctionPattern      = regexp.MustCompile(`(?i)^\s*CREATE(\s+OR\s+REPLACE)?\s+FUNCTION(\s+IF\s+NOT\s+EXISTS)?\s+` + identifierPattern)
	alterTablePattern             = regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE(\s+IF\s+EXISTS)?\s+` + identifierPattern)
	mergeTreeEnginePattern        = regexp.MustCompile(`(?i)(ENGINE\s*=\s*)([A-Za-z]*MergeTree)(\(|\s+|;|$)`)
)

type clickhouseDialect struct{}

// Clickhouse should be used to insert a lot of data in batches. The current official clickhouse
// driver doesn't support Transactions for multiple tables. The only way to add in batches is
// creating a transaction for a table, adding all rows and commiting it.
func (d clickhouseDialect) Flush(tx Tx, ctx context.Context, l *Loader, outputModuleHash string, lastFinalBlock uint64) (int, error) {
	var entryCount int
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		tableName := entriesPair.Key
		entries := entriesPair.Value
		tx, err := l.DB.BeginTx(ctx, nil)
		if err != nil {
			return entryCount, fmt.Errorf("failed to begin db transaction")
		}

		if l.tracer.Enabled() {
			l.logger.Debug("flushing table entries", zap.String("table_name", tableName), zap.Int("entry_count", entries.Len()))
		}
		info := l.tables[tableName]
		columns := make([]string, 0, len(info.columnsByName))
		for column := range info.columnsByName {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		query := fmt.Sprintf(
			"INSERT INTO %s.%s (%s)",
			EscapeIdentifier(l.schema),
			EscapeIdentifier(tableName),
			strings.Join(columns, ","))
		batch, err := tx.Prepare(query)
		if err != nil {
			return entryCount, fmt.Errorf("failed to prepare insert into %q: %w", tableName, err)
		}
		for entryPair := entries.Oldest(); entryPair != nil; entryPair = entryPair.Next() {
			entry := entryPair.Value

			if err != nil {
				return entryCount, fmt.Errorf("failed to get query: %w", err)
			}

			if l.tracer.Enabled() {
				l.logger.Debug("adding query from operation to transaction", zap.Stringer("op", entry), zap.String("query", query))
			}

			values, err := convertOpToClickhouseValues(entry)
			if err != nil {
				return entryCount, fmt.Errorf("failed to get values: %w", err)
			}

			if _, err := batch.ExecContext(ctx, values...); err != nil {
				return entryCount, fmt.Errorf("executing for entry %q: %w", values, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return entryCount, fmt.Errorf("failed to commit db transaction: %w", err)
		}
		entryCount += entries.Len()
	}

	return entryCount, nil
}

func (d clickhouseDialect) Revert(tx Tx, ctx context.Context, l *Loader, lastValidFinalBlock uint64) error {
	return fmt.Errorf("clickhouse driver does not support reorg management.")
}

func (d clickhouseDialect) GetCreateCursorQuery(schema string, withPostgraphile bool) string {
	_ = withPostgraphile // TODO: see if this can work

	clusterClause := ""
	engine := "ReplacingMergeTree()"
	if CLICKHOUSE_CLUSTER != "" {
		clusterClause = fmt.Sprintf("ON CLUSTER %s", EscapeIdentifier(CLICKHOUSE_CLUSTER))
		engine = "ReplicatedReplacingMergeTree()"
	}

	return fmt.Sprintf(cli.Dedent(`
	CREATE TABLE IF NOT EXISTS %s.%s %s
	(
    id         String,
		cursor     String,
		block_num  Int64,
		block_id   String
	) Engine = %s ORDER BY id;
	`), EscapeIdentifier(schema), EscapeIdentifier(CURSORS_TABLE), clusterClause, engine)
}

func (d clickhouseDialect) GetCreateHistoryQuery(schema string, withPostgraphile bool) string {
	panic("clickhouse does not support reorg management")
}

func (d clickhouseDialect) ExecuteSetupScript(ctx context.Context, l *Loader, schemaSql string) error {
	if CLICKHOUSE_CLUSTER == "" {
		// If no cluster is specified, execute statements as-is
		for _, query := range strings.Split(schemaSql, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}
			if _, err := l.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("exec schema: %w", err)
			}
		}
		return nil
	}

	schemaSql = stripSQLComments(schemaSql)

	// Process each statement when cluster mode is enabled
	for _, query := range strings.Split(schemaSql, ";") {
		query := strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}
		modifiedQuery, _ := patchClickhouseQuery(query, CLICKHOUSE_CLUSTER)

		// Execute the modified statement
		if _, err := l.ExecContext(ctx, modifiedQuery); err != nil {
			l.logger.Error("failed to execute schema statement",
				zap.String("statement", modifiedQuery),
				zap.Error(err))
			return fmt.Errorf("exec schema: %w", err)
		}
	}

	return nil
}

// patchClickhouseQuery applies required transformations to ClickHouse SQL statements
// for cluster mode. Returns the modified query and the detected statement type.
func patchClickhouseQuery(sql, clusterName string) (string, string) {

	var stmtType string

	if matches := createDbPattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "CREATE DATABASE"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = createDbPattern.ReplaceAllString(sql,
				fmt.Sprintf("CREATE %s$2 $3 ON CLUSTER %s",
					matches[1], EscapeIdentifier(clusterName)))
		}
	}

	if matches := createTablePattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "CREATE TABLE"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = createTablePattern.ReplaceAllString(sql,
				fmt.Sprintf("CREATE TABLE$1 $2 ON CLUSTER %s",
					EscapeIdentifier(clusterName)))
		}

		sql = replaceEngineWithReplicated(sql)
	}

	if matches := createMaterializedViewPattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "CREATE MATERIALIZED VIEW"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = createMaterializedViewPattern.ReplaceAllString(sql,
				fmt.Sprintf("CREATE MATERIALIZED VIEW$1 $2 ON CLUSTER %s",
					EscapeIdentifier(clusterName)))
		}

		sql = replaceEngineWithReplicated(sql)
	}

	if matches := createAnyViewPattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "CREATE VIEW"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = createAnyViewPattern.ReplaceAllString(sql,
				fmt.Sprintf("CREATE$1 VIEW$2 $3 ON CLUSTER %s",
					EscapeIdentifier(clusterName)))
		}
	}

	// Functions are global so should always replace
	if matches := createAnyFunctionPattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "CREATE FUNCTION"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = createAnyFunctionPattern.ReplaceAllString(sql,
				fmt.Sprintf("CREATE$1 FUNCTION$2 $3 ON CLUSTER %s",
					EscapeIdentifier(clusterName)))
		}
	}

	// ALTER TABLE
	if matches := alterTablePattern.FindStringSubmatch(sql); matches != nil {
		stmtType = "ALTER TABLE"
		if !strings.Contains(strings.ToUpper(sql), "ON CLUSTER") {
			sql = alterTablePattern.ReplaceAllString(sql,
				fmt.Sprintf("ALTER TABLE$1 $2 ON CLUSTER %s",
					EscapeIdentifier(clusterName)))
		}
	}

	return sql, stmtType
}

// replaceEngineWithReplicated replaces non-replicated MergeTree engines with their Replicated variants
func replaceEngineWithReplicated(sql string) string {
	sql = mergeTreeEnginePattern.ReplaceAllStringFunc(sql, func(match string) string {
		submatches := mergeTreeEnginePattern.FindStringSubmatch(match)
		if len(submatches) >= 3 {
			prefix := submatches[1]     // ENGINE =
			engineName := submatches[2] // e.g., SummingMergeTree
			suffix := submatches[3]     // (, or space, or ; or end

			if !strings.HasPrefix(strings.ToUpper(engineName), "REPLICATED") &&
				strings.HasSuffix(strings.ToUpper(engineName), "MERGETREE") {
				return prefix + "Replicated" + engineName + suffix
			}
		}
		return match
	})

	return sql
}

// stripSQLComments removes all SQL comments from an SQL statement
func stripSQLComments(sql string) string {
	// Remove multi-line comments (/* ... */)
	blockCommentPattern := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	sql = blockCommentPattern.ReplaceAllString(sql, "")

	// Remove single-line comments (--)
	lineCommentPattern := regexp.MustCompile(`--[^\n]*`)
	sql = lineCommentPattern.ReplaceAllString(sql, "")

	// Normalize whitespace
	whitespacePattern := regexp.MustCompile(`\s+`)
	sql = whitespacePattern.ReplaceAllString(sql, " ")

	return strings.TrimSpace(sql)
}

func (d clickhouseDialect) GetUpdateCursorQuery(table, moduleHash string, cursor *sink.Cursor, block_num uint64, block_id string) string {
	return query(`
			INSERT INTO %s (id, cursor, block_num, block_id) values ('%s', '%s', %d, '%s')
	`, table, moduleHash, cursor, block_num, block_id)
}

func (d clickhouseDialect) GetAllCursorsQuery(table string) string {
	return fmt.Sprintf("SELECT id, cursor, block_num, block_id FROM %s FINAL", table)
}

func (d clickhouseDialect) ParseDatetimeNormalization(value string) string {
	return fmt.Sprintf("parseDateTimeBestEffort(%s)", escapeStringValue(value))
}

func (d clickhouseDialect) DriverSupportRowsAffected() bool {
	return false
}

func (d clickhouseDialect) OnlyInserts() bool {
	return true
}

func (d clickhouseDialect) AllowPkDuplicates() bool {
	return true
}

func (d clickhouseDialect) CreateUser(tx Tx, ctx context.Context, l *Loader, username string, password string, _database string, readOnly bool) error {
	user, pass := EscapeIdentifier(username), escapeStringValue(password)

	onClusterClause := ""
	if CLICKHOUSE_CLUSTER != "" {
		onClusterClause = fmt.Sprintf("ON CLUSTER %s", EscapeIdentifier(CLICKHOUSE_CLUSTER))
	}

	createUserQ := fmt.Sprintf("CREATE USER IF NOT EXISTS %s %s IDENTIFIED WITH plaintext_password BY %s;", user, onClusterClause, pass)
	_, err := tx.ExecContext(ctx, createUserQ)
	if err != nil {
		return fmt.Errorf("executing query %q: %w", createUserQ, err)
	}

	var grantQ string
	if readOnly {
		grantQ = fmt.Sprintf(`
            GRANT %s SELECT ON *.* TO %s;
        `, onClusterClause, user)
	} else {
		grantQ = fmt.Sprintf(`
            GRANT %s ALL ON *.* TO %s;
        `, onClusterClause, user)
	}

	_, err = tx.ExecContext(ctx, grantQ)
	if err != nil {
		return fmt.Errorf("executing query %q: %w", grantQ, err)
	}

	return nil
}

func convertOpToClickhouseValues(o *Operation) ([]any, error) {
	columns := make([]string, len(o.data))
	i := 0
	for column := range o.data {
		columns[i] = column
		i++
	}
	sort.Strings(columns)
	values := make([]any, len(o.data))
	for i, v := range columns {
		if col, exists := o.table.columnsByName[v]; exists {
			convertedType, err := convertToType(o.data[v], col.scanType)
			if err != nil {
				return nil, fmt.Errorf("converting value %q to type %q in column %q: %w", o.data[v], col.scanType, v, err)
			}
			values[i] = convertedType
		} else {
			return nil, fmt.Errorf("cannot find column %q for table %q (valid columns are %q)", v, o.table.identifier, strings.Join(maps.Keys(o.table.columnsByName), ", "))
		}
	}
	return values, nil
}

func convertToType(value string, valueType reflect.Type) (any, error) {
	switch valueType.Kind() {
	case reflect.String:
		return value, nil
	case reflect.Slice:
		if valueType.Elem().Kind() == reflect.Struct || valueType.Elem().Kind() == reflect.Ptr {
			return nil, fmt.Errorf("%q is not supported as Clickhouse Array type", valueType.Elem().Name())
		}

		res := reflect.New(reflect.SliceOf(valueType.Elem()))
		if err := json.Unmarshal([]byte(value), res.Interface()); err != nil {
			return "", fmt.Errorf("could not JSON unmarshal slice value %q: %w", value, err)
		}

		return res.Elem().Interface(), nil
	case reflect.Bool:
		return strconv.ParseBool(value)
	case reflect.Int:
		v, err := strconv.ParseInt(value, 10, 0)
		return int(v), err
	case reflect.Int8:
		v, err := strconv.ParseInt(value, 10, 8)
		return int8(v), err
	case reflect.Int16:
		v, err := strconv.ParseInt(value, 10, 16)
		return int16(v), err
	case reflect.Int32:
		v, err := strconv.ParseInt(value, 10, 32)
		return int32(v), err
	case reflect.Int64:
		return strconv.ParseInt(value, 10, 64)
	case reflect.Uint:
		v, err := strconv.ParseUint(value, 10, 0)
		return uint(v), err
	case reflect.Uint8:
		v, err := strconv.ParseUint(value, 10, 8)
		return uint8(v), err
	case reflect.Uint16:
		v, err := strconv.ParseUint(value, 10, 16)
		return uint16(v), err
	case reflect.Uint32:
		v, err := strconv.ParseUint(value, 10, 32)
		return uint32(v), err
	case reflect.Uint64:
		return strconv.ParseUint(value, 10, 0)
	case reflect.Float32, reflect.Float64:
		return strconv.ParseFloat(value, 10)
	case reflect.Struct:
		if valueType == reflectTypeTime {
			if integerRegex.MatchString(value) {
				i, err := strconv.Atoi(value)
				if err != nil {
					return "", fmt.Errorf("could not convert %s to int: %w", value, err)
				}

				return int64(i), nil
			}

			var v time.Time
			var err error
			if strings.Contains(value, "T") && strings.HasSuffix(value, "Z") {
				v, err = time.Parse("2006-01-02T15:04:05Z", value)
			} else if dateRegex.MatchString(value) {
				// This is a Clickhouse Date field. The Clickhouse Go client doesn't convert unix timestamp into Date,
				// so we just validate the format here and return a string.
				_, err = time.Parse("2006-01-02", value)
				if err != nil {
					return "", fmt.Errorf("could not convert %s to date: %w", value, err)
				}
				return value, nil
			} else {
				v, err = time.Parse("2006-01-02 15:04:05", value)
			}
			if err != nil {
				return "", fmt.Errorf("could not convert %s to time: %w", value, err)
			}
			return v.Unix(), nil
		}
		return "", fmt.Errorf("unsupported struct type %s", valueType)

	case reflect.Ptr:
		if valueType.String() == "*big.Int" {
			newInt := new(big.Int)
			newInt.SetString(value, 10)
			return newInt, nil
		}

		elemType := valueType.Elem()
		val, err := convertToType(value, elemType)
		if err != nil {
			return nil, fmt.Errorf("invalid pointer type: %w", err)
		}

		// We cannot just return &val here as this will return an *interface{} that the Clickhouse Go client won't be
		// able to convert on inserting. Instead, we create a new variable using the type that valueType has been
		// pointing to, assign the converted value from convertToType to that and then return a pointer to the new variable.
		result := reflect.New(elemType).Elem()
		result.Set(reflect.ValueOf(val))
		return result.Addr().Interface(), nil

	default:
		return value, nil
	}
}
