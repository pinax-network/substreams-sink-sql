package sinker

import (
	"github.com/streamingfast/dmetrics"
)

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var FlushCount = metrics.NewCounter("substreams_sink_postgres_store_flush_count", "The amount of flush that happened so far")
var FlushedRowsCount = metrics.NewCounter("substreams_sink_postgres_flushed_rows_count", "The number of flushed rows so far")
var FlushDuration = metrics.NewCounter("substreams_sink_postgres_store_flush_duration", "The amount of time spent flushing cache to db (in nanoseconds)")

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams_sink_postgres")
var HeadBlockTimeDrift = metrics.NewHeadTimeDrift("substreams_sink_postgres")

var SinkInfo = metrics.NewGaugeVec("substreams_sink_sql_info", []string{
	"endpoint",           // Substreams endpoint
	"database",           // Database name
	"schema",             // Schema name
	"db_host",            // Database host
	"manifest",           // Manifest path or URL
	"output_module",      // Output module name
	"module_hash",        // Output module hash
	"block_start",        // Start block
	"block_end",          // End block
	"block_restarted_at", // Restarted at block
	"substreams_run",     // Substreams run command: substreams run <manifest> <output_module> -e <endpoint> -s <block_restarted_at> --limit-processed-blocks 0 --production-mode --noop-mode
}, "Information about the SQL sink configuration")
