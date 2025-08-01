package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastFinalBlock uint64) (rowFlushedCount int, err error) {
	var lastErr error
	startAt := time.Now()

	for attempt := range l.maxFlushRetries {
		rowFlushedCount, lastErr = l.attemptFlush(ctx, outputModuleHash, cursor, lastFinalBlock)
		if lastErr == nil {
			l.logger.Debug("flushed table(s) rows to database",
				zap.Int("table_count", l.entries.Len()+1),
				zap.Int("row_count", rowFlushedCount),
				zap.Duration("took", time.Since(startAt)),
			)
			return rowFlushedCount, nil
		}
		l.logger.Warn("Flush attempt failed",
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", l.maxFlushRetries),
			zap.Error(lastErr),
		)
		if attempt < l.maxFlushRetries-1 {
			time.Sleep(l.sleepBetweenFlushRetries)
		}
	}

	return 0, fmt.Errorf("failed to flush after %d attempts: %w", l.maxFlushRetries, lastErr)
}

// attemptFlush performs a single attempt at flushing data to the database
func (l *Loader) attemptFlush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastFinalBlock uint64) (rowFlushedCount int, err error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithStdAsync(false))
	tx, err := l.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin db transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	rowFlushedCount, err = l.getDialect().Flush(tx, ctx, l, outputModuleHash, lastFinalBlock)
	if err != nil {
		return 0, fmt.Errorf("dialect flush: %w", err)
	}

	rowFlushedCount += 1
	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return 0, fmt.Errorf("update cursor: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit db transaction: %w", err)
	}
	l.reset()

	return rowFlushedCount, nil
}

func (l *Loader) Revert(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastValidBlock uint64) error {
	tx, err := l.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to being db transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				l.logger.Warn("failed to rollback transaction", zap.Error(err))
			}
		}
	}()

	if err := l.getDialect().Revert(tx, ctx, l, lastValidBlock); err != nil {
		return err
	}

	if err := l.UpdateCursor(ctx, tx, outputModuleHash, cursor); err != nil {
		return fmt.Errorf("update cursor after revert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}

	l.logger.Debug("reverted changes to database", zap.Uint64("last_valid_block", lastValidBlock))
	return nil
}

func (l *Loader) reset() {
	for entriesPair := l.entries.Oldest(); entriesPair != nil; entriesPair = entriesPair.Next() {
		l.entries.Set(entriesPair.Key, NewOrderedMap[string, *Operation]())
	}
}
