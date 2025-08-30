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

// Performance profiling metrics
var ProtobufDecodeDuration = metrics.NewCounter("substreams_sink_postgres_proto_decode_duration", "Time spent decoding protobuf messages (in nanoseconds)")
var ProtobufMessageSize = metrics.NewCounter("substreams_sink_postgres_proto_message_size", "Size of protobuf messages in bytes")
var DatabaseChangesDuration = metrics.NewCounter("substreams_sink_postgres_db_changes_duration", "Time spent applying database changes (in nanoseconds)")
var DatabaseChangesCount = metrics.NewCounter("substreams_sink_postgres_db_changes_count", "Number of database changes processed")

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams_sink_postgres")
var HeadBlockTimeDrift = metrics.NewHeadTimeDrift("substreams_sink_postgres")
