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
