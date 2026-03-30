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

	err = l.Upsert("xfer", map[string]string{"id": "1234"}, map[string]string{"from": "sender1", "to": "receiver1"}, nil)
	require.NoError(t, err)

	entry, found := l.entries.Get("xfer")
	require.True(t, found)

	op, found := entry.Get("1234")
	require.True(t, found)
	assert.Equal(t, OperationTypeInsert, op.opType)
	assert.Equal(t, map[string]string{"from": "sender1", "id": "1234", "to": "receiver1"}, op.data)
}
