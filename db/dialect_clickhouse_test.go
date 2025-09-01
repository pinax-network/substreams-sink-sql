package db

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_replaceEngineWithReplicated(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "MergeTree with parameters",
			input:    "ENGINE = MergeTree()",
			expected: "ENGINE = ReplicatedMergeTree()",
		},
		{
			name:     "MergeTree without parameters",
			input:    "ENGINE = MergeTree ORDER BY id",
			expected: "ENGINE = ReplicatedMergeTree ORDER BY id",
		},
		{
			name:     "Full SQL with MergeTree engine with params",
			input:    "CREATE TABLE mytable (id UInt32) ENGINE = MergeTree() ORDER BY id",
			expected: "CREATE TABLE mytable (id UInt32) ENGINE = ReplicatedMergeTree() ORDER BY id",
		},
		{
			name:     "SummingMergeTree with parameters",
			input:    "ENGINE = SummingMergeTree(val)",
			expected: "ENGINE = ReplicatedSummingMergeTree(val)",
		},
		{
			name:     "Already Replicated engine",
			input:    "ENGINE = ReplicatedMergeTree()",
			expected: "ENGINE = ReplicatedMergeTree()",
		},
		{
			name:     "Case insensitive ENGINE",
			input:    "engine = MergeTree()",
			expected: "engine = ReplicatedMergeTree()",
		},
		{
			name:     "Mixed spacing around equals",
			input:    "ENGINE=MergeTree()",
			expected: "ENGINE=ReplicatedMergeTree()",
		},
		{
			name:     "Multiple engines in one SQL",
			input:    "CREATE TABLE t1 ENGINE = MergeTree(); CREATE TABLE t2 ENGINE = SummingMergeTree(val)",
			expected: "CREATE TABLE t1 ENGINE = ReplicatedMergeTree(); CREATE TABLE t2 ENGINE = ReplicatedSummingMergeTree(val)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceEngineWithReplicated(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_patchClickhouseQuery(t *testing.T) {
	clusterName := "test_cluster"

	tests := []struct {
		name             string
		input            string
		expectedOutput   string
		expectedStmtType string
	}{
		// CREATE DATABASE tests
		{
			name:             "CREATE DATABASE - no cluster",
			input:            "CREATE DATABASE mydb",
			expectedOutput:   "CREATE DATABASE IF NOT EXISTS mydb ON CLUSTER \"test_cluster\"",
			expectedStmtType: "CREATE DATABASE",
		},
		{
			name:             "CREATE DATABASE - with IF NOT EXISTS",
			input:            "CREATE DATABASE IF NOT EXISTS mydb",
			expectedOutput:   "CREATE DATABASE IF NOT EXISTS mydb ON CLUSTER \"test_cluster\"",
			expectedStmtType: "CREATE DATABASE",
		},
		{
			name:             "CREATE SCHEMA",
			input:            "CREATE SCHEMA my_schema",
			expectedOutput:   "CREATE SCHEMA IF NOT EXISTS my_schema ON CLUSTER \"test_cluster\"",
			expectedStmtType: "CREATE DATABASE",
		},
		{
			name:             "CREATE DATABASE - already has cluster",
			input:            "CREATE DATABASE mydb ON CLUSTER \"other_cluster\"",
			expectedOutput:   "CREATE DATABASE mydb ON CLUSTER \"other_cluster\"",
			expectedStmtType: "CREATE DATABASE",
		},

		// CREATE TABLE tests
		{
			name:             "CREATE TABLE - simple",
			input:            "CREATE TABLE mytable (id UInt32)",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS mytable ON CLUSTER \"test_cluster\" (id UInt32)",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE with quotes - with IF NOT EXISTS",
			input:            "CREATE TABLE IF NOT EXISTS 'mytable' (id UInt32)",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS 'mytable' ON CLUSTER \"test_cluster\" (id UInt32)",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE with space in name - with IF NOT EXISTS",
			input:            "CREATE TABLE IF NOT EXISTS 'my table' (id UInt32)",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS 'my table' ON CLUSTER \"test_cluster\" (id UInt32)",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE - with MergeTree engine with params",
			input:            "CREATE TABLE mytable (id UInt32) ENGINE = MergeTree() ORDER BY id",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS mytable ON CLUSTER \"test_cluster\" (id UInt32) ENGINE = ReplicatedMergeTree() ORDER BY id",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE - with already Replicated engine",
			input:            "CREATE TABLE mytable (id UInt32) ENGINE = ReplicatedMergeTree() ORDER BY id",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS mytable ON CLUSTER \"test_cluster\" (id UInt32) ENGINE = ReplicatedMergeTree() ORDER BY id",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE - with MergeTree engine without params",
			input:            "CREATE TABLE mytable (id UInt32) ENGINE = MergeTree ORDER BY id",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS mytable ON CLUSTER \"test_cluster\" (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id",
			expectedStmtType: "CREATE TABLE",
		},
		{
			name:             "CREATE TABLE - with SummingMergeTree engine",
			input:            "CREATE TABLE mytable (id UInt32, val UInt64) ENGINE = SummingMergeTree(val) ORDER BY id",
			expectedOutput:   "CREATE TABLE IF NOT EXISTS mytable ON CLUSTER \"test_cluster\" (id UInt32, val UInt64) ENGINE = ReplicatedSummingMergeTree(val) ORDER BY id",
			expectedStmtType: "CREATE TABLE",
		},

		// CREATE MATERIALIZED VIEW tests
		{
			name:             "CREATE MATERIALIZED VIEW - simple",
			input:            "CREATE MATERIALIZED VIEW myview AS SELECT * FROM mytable",
			expectedOutput:   "CREATE MATERIALIZED VIEW IF NOT EXISTS myview ON CLUSTER \"test_cluster\" AS SELECT * FROM mytable",
			expectedStmtType: "CREATE MATERIALIZED VIEW",
		},
		{
			name:             "CREATE MATERIALIZED VIEW - with MergeTree engine",
			input:            "CREATE MATERIALIZED VIEW myview ENGINE = MergeTree() ORDER BY id AS SELECT id FROM mytable",
			expectedOutput:   "CREATE MATERIALIZED VIEW IF NOT EXISTS myview ON CLUSTER \"test_cluster\" ENGINE = ReplicatedMergeTree() ORDER BY id AS SELECT id FROM mytable",
			expectedStmtType: "CREATE MATERIALIZED VIEW",
		},

		// CREATE VIEW tests
		{
			name:             "CREATE VIEW - simple",
			input:            "CREATE VIEW myview AS SELECT * FROM mytable",
			expectedOutput:   "CREATE VIEW IF NOT EXISTS myview ON CLUSTER \"test_cluster\" AS SELECT * FROM mytable",
			expectedStmtType: "CREATE VIEW",
		},
		{
			name:             "CREATE OR REPLACE VIEW - simple",
			input:            "CREATE OR REPLACE VIEW myview AS SELECT * FROM mytable",
			expectedOutput:   "CREATE VIEW IF NOT EXISTS myview ON CLUSTER \"test_cluster\" AS SELECT * FROM mytable",
			expectedStmtType: "CREATE VIEW",
		},
		{
			name:             "CREATE VIEW IF NOT EXISTS - simple",
			input:            "CREATE VIEW IF NOT EXISTS myview AS SELECT * FROM mytable",
			expectedOutput:   "CREATE VIEW IF NOT EXISTS myview ON CLUSTER \"test_cluster\" AS SELECT * FROM mytable",
			expectedStmtType: "CREATE VIEW",
		},

		// CREATE FUNCTION tests
		{
			name:             "CREATE FUNCTION - simple",
			input:            "CREATE FUNCTION myfunc AS (a, b) -> a + b",
			expectedOutput:   "CREATE FUNCTION IF NOT EXISTS myfunc ON CLUSTER \"test_cluster\" AS (a, b) -> a + b",
			expectedStmtType: "CREATE FUNCTION",
		},
		{
			name:             "CREATE FUNCTION - with quotes",
			input:            "CREATE FUNCTION \"my func\" AS (a, b) -> a + b",
			expectedOutput:   "CREATE FUNCTION IF NOT EXISTS \"my func\" ON CLUSTER \"test_cluster\" AS (a, b) -> a + b",
			expectedStmtType: "CREATE FUNCTION",
		},
		{
			name:             "CREATE OR REPLACE FUNCTION - simple",
			input:            "CREATE OR REPLACE FUNCTION myfunc AS (a, b) -> a + b",
			expectedOutput:   "CREATE FUNCTION IF NOT EXISTS myfunc ON CLUSTER \"test_cluster\" AS (a, b) -> a + b",
			expectedStmtType: "CREATE FUNCTION",
		},
		{
			name:             "CREATE FUNCTION IF NOT EXISTS- simple",
			input:            "CREATE FUNCTION IF NOT EXISTS myfunc AS (a, b) -> a + b",
			expectedOutput:   "CREATE FUNCTION IF NOT EXISTS myfunc ON CLUSTER \"test_cluster\" AS (a, b) -> a + b",
			expectedStmtType: "CREATE FUNCTION",
		},

		// ALTER TABLE tests
		{
			name:             "ALTER TABLE - simple",
			input:            "ALTER TABLE mytable ADD COLUMN newcol UInt32",
			expectedOutput:   "ALTER TABLE mytable ON CLUSTER \"test_cluster\" ADD COLUMN newcol UInt32",
			expectedStmtType: "ALTER TABLE",
		},

		// Non-matching statement
		{
			name:             "Non-matching statement - SELECT",
			input:            "SELECT * FROM mytable",
			expectedOutput:   "SELECT * FROM mytable",
			expectedStmtType: "",
		},
		{
			name:             "Non-matching statement - INSERT",
			input:            "INSERT INTO mytable VALUES (1, 2, 3)",
			expectedOutput:   "INSERT INTO mytable VALUES (1, 2, 3)",
			expectedStmtType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, stmtType := patchClickhouseQuery(tt.input, clusterName)
			assert.Equal(t, tt.expectedOutput, output)
			assert.Equal(t, tt.expectedStmtType, stmtType)
		})
	}
}

func Test_stripSQLComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single line comment",
			input:    "-- This is a comment\nCREATE TABLE test",
			expected: "CREATE TABLE test",
		},
		{
			name:     "multi-line comment",
			input:    "/* ──────────────────────────────\n   comment \n   ───────────────────────────── */\nCREATE TABLE test",
			expected: "CREATE TABLE test",
		},
		{
			name:     "multi-line comment nested",
			input:    "/* ─────────────────\n\n────────────────── */\nCREATE TABLE test/* ───────────────────────────*/",
			expected: "CREATE TABLE test",
		},
		{
			name:     "multi-line comment nested with other comments",
			input:    "/* one-time DDL -----------------------------------------------------------*/\nCREATE TABLE test/* ───────────────────────────*/",
			expected: "CREATE TABLE test",
		},
		{
			name:     "mixed comments",
			input:    "-- Header comment\n/* Block comment\nMultiple lines\n*/CREATE TABLE test -- inline comment",
			expected: "CREATE TABLE test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripSQLComments(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func Test_convertToType(t *testing.T) {

	tests := []struct {
		name      string
		value     string
		expect    any
		expectErr error
		valueType reflect.Type
	}{
		{
			name:      "Date",
			value:     "2021-01-01",
			expect:    "2021-01-01",
			expectErr: nil,
			valueType: reflect.TypeOf(time.Time{}),
		}, {
			name:      "Invalid Date",
			value:     "2021-99-01",
			expect:    nil,
			expectErr: errors.New(`could not convert 2021-99-01 to date: parsing time "2021-99-01": month out of range`),
			valueType: reflect.TypeOf(time.Time{}),
		},
		{
			name:      "ISO 8601 datetime",
			value:     "2021-01-01T00:00:00Z",
			expect:    int64(1609459200),
			expectErr: nil,
			valueType: reflect.TypeOf(time.Time{}),
		},
		{
			name:      "common datetime",
			value:     "2021-01-01 00:00:00",
			expect:    int64(1609459200),
			expectErr: nil,
			valueType: reflect.TypeOf(time.Time{}),
		},
		{
			name:      "String Slice Double Quoted",
			value:     `["field1", "field2"]`,
			expect:    []string{"field1", "field2"},
			expectErr: nil,
			valueType: reflect.TypeOf([]string{}),
		}, {
			name:      "Int Slice",
			value:     `[1, 2]`,
			expect:    []int{1, 2},
			expectErr: nil,
			valueType: reflect.TypeOf([]int{}),
		}, {
			name:      "Float Slice",
			value:     `[1.0, 2.0]`,
			expect:    []float64{1, 2},
			expectErr: nil,
			valueType: reflect.TypeOf([]float64{}),
		}, {
			name:      "Invalid Type Slice Struct",
			value:     `[""]`,
			expect:    nil,
			expectErr: errors.New(`"Time" is not supported as Clickhouse Array type`),
			valueType: reflect.TypeOf([]time.Time{}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := convertToType(test.value, test.valueType)
			if test.expectErr != nil {
				assert.EqualError(t, err, test.expectErr.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expect, res)
			}
		})
	}
}
