package sinker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	sink "github.com/streamingfast/substreams-sink"
	pbdatabase "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	"github.com/streamingfast/substreams-sink-sql/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A balances table like the evm-balances one: a ReplacingMergeTree keyed by address, and a
// materialized view that counts every row inserted into it.
const clickhouseBalancesSchema = `
CREATE TABLE balances (
	address   String,
	block_num UInt32,
	balance   UInt64
) ENGINE = ReplacingMergeTree(block_num) ORDER BY address;

CREATE TABLE balance_changes (
	address      String,
	transactions UInt64
) ENGINE = SummingMergeTree ORDER BY address;

CREATE MATERIALIZED VIEW balance_changes_mv TO balance_changes AS
SELECT address, count() AS transactions FROM balances GROUP BY address;
`

func TestClickhouseFlushesKeepEveryRow(t *testing.T) {
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip(`CLICKHOUSE_DSN not set, please specify CLICKHOUSE_DSN to run this test (it creates and drops its own databases), example: CLICKHOUSE_DSN="clickhouse://default:@localhost:9000/default"`)
	}

	type block struct {
		num          uint64
		balances     map[string]uint64
		expectCursor uint64 // block of the last flush once this block is handled, 0 for none
	}

	tests := []struct {
		name                    string
		batchBlockFlushInterval int
		blocks                  []block
		expectTransactions      map[string]uint64
		expectFinalBalances     map[string]uint64
	}{
		{
			name:                    "flush interval 1",
			batchBlockFlushInterval: 1,
			blocks: []block{
				{num: 10, balances: map[string]uint64{"a": 100}, expectCursor: 10},
				{num: 11, balances: map[string]uint64{"a": 110}, expectCursor: 11},
			},
			expectTransactions:  map[string]uint64{"a": 2},
			expectFinalBalances: map[string]uint64{"a": 110},
		},
		{
			name:                    "flush interval 3",
			batchBlockFlushInterval: 3,
			blocks: []block{
				{num: 20, balances: map[string]uint64{"a": 200}, expectCursor: 0},
				{num: 21, balances: map[string]uint64{"a": 210, "b": 211}, expectCursor: 0},
				{num: 22, balances: map[string]uint64{"a": 220}, expectCursor: 22},
			},
			expectTransactions:  map[string]uint64{"a": 3, "b": 1},
			expectFinalBalances: map[string]uint64{"a": 220, "b": 211},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			loader, err := db.NewLoader(newClickhouseTestDatabase(t, dsn), test.batchBlockFlushInterval, 1000, 1, db.OnModuleHashMismatchIgnore, nil, logger, tracer)
			require.NoError(t, err)
			t.Cleanup(func() { loader.Close() })

			require.NoError(t, loader.Setup(ctx, clickhouseBalancesSchema, false))
			require.NoError(t, loader.LoadTables())

			s, err := sink.New(sink.SubstreamsModeDevelopment, false, testPackage, testPackage.Modules.Modules[0], []byte("unused"), testClientConfig, logger, nil)
			require.NoError(t, err)
			sinker, _ := New(s, loader, logger, nil, "", 0)

			isLive := false
			for _, block := range test.blocks {
				var changes []*pbdatabase.TableChange
				for address, balance := range block.balances {
					changes = append(changes, insertRowSinglePK("balances", address, "block_num", fmt.Sprint(block.num), "balance", fmt.Sprint(balance)))
				}

				err := sinker.HandleBlockScopedData(ctx, blockScopedData("db_out", changes, block.num, block.num), &isLive, sink.MustNewCursor(simpleCursor(block.num, block.num)))
				require.NoError(t, err)

				// A flush writes the cursor as an asynchronous insert that it does not wait for.
				_, err = loader.ExecContext(ctx, "SYSTEM FLUSH ASYNC INSERT QUEUE")
				require.NoError(t, err)

				var cursorBlock uint64
				require.NoError(t, loader.QueryRowContext(ctx, "SELECT toUInt64(max(block_num)) FROM cursors").Scan(&cursorBlock))
				assert.Equal(t, block.expectCursor, cursorBlock, "last flushed block after block %d", block.num)
			}

			assert.Equal(t, test.expectTransactions, queryClickhouseTotals(t, loader, "SELECT address, sum(transactions) FROM balance_changes GROUP BY address"))
			assert.Equal(t, test.expectFinalBalances, queryClickhouseTotals(t, loader, "SELECT address, balance FROM balances FINAL"))
		})
	}
}

// newClickhouseTestDatabase creates a database on the server of dsn, drops it when the test ends,
// and returns dsn with that database.
func newClickhouseTestDatabase(t *testing.T, dsn string) string {
	t.Helper()

	server, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })

	database := fmt.Sprintf("sink_sql_test_%d", time.Now().UnixNano())
	_, err = server.Exec("CREATE DATABASE " + database)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := server.Exec("DROP DATABASE " + database + " SYNC")
		assert.NoError(t, err)
	})

	databaseDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	databaseDSN.Path = "/" + database

	return databaseDSN.String()
}

func queryClickhouseTotals(t *testing.T, loader *db.Loader, query string) map[string]uint64 {
	t.Helper()

	rows, err := loader.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	totals := map[string]uint64{}
	for rows.Next() {
		var address string
		var total uint64
		require.NoError(t, rows.Scan(&address, &total))
		totals[address] = total
	}
	require.NoError(t, rows.Err())

	return totals
}
