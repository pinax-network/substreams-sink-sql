package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/v1"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

var (
	onModuleHashMistmatchFlag = "on-module-hash-mistmatch"
	flushRetryCountFlag       = "flush-retry-count"
	flushRetryDelayFlag       = "flush-retry-delay"
	tooManyPartsMaxWaitFlag   = "clickhouse-too-many-parts-max-wait"
)

var supportedOutputTypes = "sf.substreams.sink.database.v1.DatabaseChanges,sf.substreams.database.v1.DatabaseChanges"

var (
	supportedDeployableUnits   []string
	supportedDeployableService = "sf.substreams.sink.sql.v1.Service"
)

func init() {
	supportedDeployableUnits = []string{
		supportedDeployableService,
	}
}

func extractSinkConfig(pkg *pbsubstreams.Package) (*pbsql.Service, error) {
	if pkg.SinkConfig == nil {
		return nil, fmt.Errorf("no sink config found in spkg")
	}

	switch pkg.SinkConfig.TypeUrl {
	case supportedDeployableService:
		service := &pbsql.Service{}
		if err := pkg.SinkConfig.UnmarshalTo(service); err != nil {
			return nil, fmt.Errorf("failed to proto unmarshal: %w", err)
		}
		return service, nil
	}

	return nil, fmt.Errorf("invalid config type %q, supported configs are %q", pkg.SinkConfig.TypeUrl, strings.Join(supportedDeployableUnits, ", "))
}

func newDBLoader(
	cmd *cobra.Command,
	psqlDSN string,
	batchBlockFlushInterval int,
	batchRowFlushInterval int,
	liveBlockFlushInterval int,
	handleReorgs bool,
) (*db.Loader, error) {
	moduleMismatchMode, err := db.ParseOnModuleHashMismatch(sflags.MustGetString(cmd, onModuleHashMistmatchFlag))
	cli.NoError(err, "invalid mistmatch mode")

	dbLoader, err := db.NewLoader(psqlDSN, batchBlockFlushInterval, batchRowFlushInterval, liveBlockFlushInterval, moduleMismatchMode, &handleReorgs, zlog, tracer)
	if err != nil {
		return nil, fmt.Errorf("new psql loader: %w", err)
	}
	dbLoader.SetFlushRetries(sflags.MustGetInt(cmd, flushRetryCountFlag), sflags.MustGetDuration(cmd, flushRetryDelayFlag))
	dbLoader.SetTooManyPartsMaxWait(sflags.MustGetDuration(cmd, tooManyPartsMaxWaitFlag))

	if err := dbLoader.LoadTables(); err != nil {
		var e *db.SystemTableError
		if errors.As(err, &e) {
			fmt.Printf("Error validating the system table: %s\n", e)
			fmt.Println("Did you run setup ?")
			return nil, e
		}

		return nil, fmt.Errorf("load psql table: %w", err)
	}

	return dbLoader, nil
}

// AddCommonSinkerFlags adds the flags common to all command that needs to create a sinker,
// namely the `run` and `generate-csv` commands.
func AddCommonSinkerFlags(flags *pflag.FlagSet) {
	flags.Int(flushRetryCountFlag, 3, "Attempts a flush gets before the sink fails, for errors other than ClickHouse's TOO_MANY_PARTS")
	flags.Duration(flushRetryDelayFlag, 5*time.Second, "Wait between two attempts of a failed flush")
	flags.Duration(tooManyPartsMaxWaitFlag, db.DefaultTooManyPartsMaxWait, cli.FlagDescription(`
		Total time a flush waits for ClickHouse's merges when an insert fails with TOO_MANY_PARTS (code 252) before it
		fails; 0 treats the error like any other. The waits start at %s and double up to %s each.
	`, db.DefaultTooManyPartsBackoff, db.DefaultTooManyPartsMaxBackoff))
	flags.String(onModuleHashMistmatchFlag, "error", cli.FlagDescription(`
		What to do when the module hash in the manifest does not match the one in the database, can be 'error', 'warn' or 'ignore'

		- If 'error' is used (default), it will exit with an error explaining the problem and how to fix it.
		- If 'warn' is used, it does the same as 'ignore' but it will log a warning message when it happens.
		- If 'ignore' is set, we pick the cursor at the highest block number and use it as the starting point. Subsequent
		updates to the cursor will overwrite the module hash in the database.
	`))
}

func readBlockRangeArgument(in string) (blockRange *bstream.Range, err error) {
	return sink.ReadBlockRange(&pbsubstreams.Module{
		Name:         "dummy",
		InitialBlock: 0,
	}, in)
}

type cliApplication struct {
	appCtx  context.Context
	shutter *shutter.Shutter
}

func (a *cliApplication) WaitForTermination(logger *zap.Logger, unreadyPeriodAfterSignal, gracefulShutdownDelay time.Duration) error {
	// On any exit path, we synchronize the logger one last time
	defer func() {
		logger.Sync()
	}()

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(unreadyPeriodAfterSignal, logger)
	select {
	case <-signalHandler:
		go a.shutter.Shutdown(nil)
		break
	case <-a.shutter.Terminating():
		logger.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", a.shutter.Err() != nil))
		break
	}

	logger.Info("waiting for run termination")
	select {
	case <-a.shutter.Terminated():
	case <-time.After(gracefulShutdownDelay):
		logger.Warn("application did not terminate within graceful period of " + gracefulShutdownDelay.String() + ", forcing termination")
	}

	if err := a.shutter.Err(); err != nil {
		return err
	}

	logger.Info("run terminated gracefully")
	return nil
}
