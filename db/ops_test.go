package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPrimaryKey(t *testing.T) {
	tests := []struct {
		name        string
		in          []*ColumnInfo
		expectOut   map[string]string
		expectError bool
	}{
		{
			name:        "no primkey error",
			expectError: true,
		},
		{
			name: "more than one primkey error",
			in: []*ColumnInfo{
				{
					name: "one",
				},
				{
					name: "two",
				},
			},
			expectError: true,
		},
		{
			name: "single than primkey ok",
			in: []*ColumnInfo{
				{
					name: "id",
				},
			},
			expectOut: map[string]string{
				"id": "testval",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := &Loader{
				tables: map[string]*TableInfo{
					"test": {
						primaryColumns: test.in,
					},
				},
			}
			out, err := l.GetPrimaryKey("test", "testval")
			if test.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectOut, out)
			}

		})
	}

}

func TestUpsertUsesInsertForInsertOnlyDialects(t *testing.T) {
	l, err := NewLoader("clickhouse://user:pass@localhost:9000/testschema", 0, 0, 0, OnModuleHashMismatchIgnore, nil, zlog, tracer)
	require.NoError(t, err)

	l.tables = TestTables("testschema")

	err = l.Upsert("xfer", map[string]string{"id": "1234"}, map[string]string{"from": "sender1", "to": "receiver1"}, 10, nil)
	require.NoError(t, err)

	entry, found := l.entries.Get("xfer")
	require.True(t, found)

	op, found := entry.Get("1234@10")
	require.True(t, found)
	assert.Equal(t, OperationTypeInsert, op.opType)
	assert.Equal(t, map[string]string{"from": "sender1", "id": "1234", "to": "receiver1"}, op.data)
}

func TestInsertKeepsARowPerBlockForInsertOnlyDialects(t *testing.T) {
	l, err := NewLoader("clickhouse://user:pass@localhost:9000/testschema", 0, 0, 0, OnModuleHashMismatchIgnore, nil, zlog, tracer)
	require.NoError(t, err)

	l.tables = TestTables("testschema")

	// Blocks 10 to 12 wait for the same flush. Key 1234 is written in each of them, and twice in
	// block 12; key 2345 only in block 10.
	insert := func(id, from string, blockNum uint64) {
		require.NoError(t, l.Insert("xfer", map[string]string{"id": id}, map[string]string{"from": from, "to": "receiver"}, blockNum, nil))
	}
	insert("1234", "sender10", 10)
	insert("2345", "sender10", 10)
	require.NoError(t, l.Upsert("xfer", map[string]string{"id": "1234"}, map[string]string{"from": "sender11", "to": "receiver"}, 11, nil))
	insert("1234", "sender12", 12)
	insert("1234", "sender12-last", 12)

	assert.Equal(t, 4, l.GetBufferedRowCount())

	// The rows exactly as the ClickHouse flush sends them, columns sorted by name (from, id, to):
	// one per key and block, in block order, the last write winning within a block.
	entry, found := l.entries.Get("xfer")
	require.True(t, found)

	var rows [][]any
	for pair := entry.Oldest(); pair != nil; pair = pair.Next() {
		values, err := convertOpToClickhouseValues(pair.Value)
		require.NoError(t, err)
		rows = append(rows, values)
	}

	assert.Equal(t, [][]any{
		{"sender10", "1234", "receiver"},
		{"sender10", "2345", "receiver"},
		{"sender11", "1234", "receiver"},
		{"sender12-last", "1234", "receiver"},
	}, rows)
}
