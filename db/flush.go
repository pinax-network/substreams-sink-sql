package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

func (l *Loader) Flush(ctx context.Context, outputModuleHash string, cursor *sink.Cursor, lastFinalBlock uint64) (rowFlushedCount int, err error) {
	startAt := time.Now()
	rowFlushedCount, err = l.retryFlush(ctx, func() (int, error) {
		return l.attemptFlush(ctx, outputModuleHash, cursor, lastFinalBlock)
	})
	if err != nil {
		return 0, err
	}
	l.logger.Debug("flushed table(s) rows to database",
		zap.Int("table_count", l.entries.Len()+1),
		zap.Int("row_count", rowFlushedCount),
		zap.Duration("took", time.Since(startAt)),
	)
	return rowFlushedCount, nil
}

// clickhouseTooManyParts is ClickHouse's TOO_MANY_PARTS error code: a partition
// of a table the insert writes to, a materialized view's target included, has
// more active parts than its parts_to_throw_insert because merges fall behind
// inserts. Waiting lets the merges catch up.
const clickhouseTooManyParts = 252

// isTooManyParts reports whether err is ClickHouse's TOO_MANY_PARTS error.
func isTooManyParts(err error) bool {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		return exception.Code == clickhouseTooManyParts
	}
	return strings.Contains(err.Error(), fmt.Sprintf("code: %d,", clickhouseTooManyParts))
}

// retryFlush runs attempt until it succeeds. A TOO_MANY_PARTS error is
// backpressure, not a failure: it waits, doubling the wait from
// tooManyPartsBackoff up to tooManyPartsMaxBackoff, for at most
// tooManyPartsMaxWait in all, without using up the maxFlushRetries attempts
// that any other error gets, sleepBetweenFlushRetries apart. A failed attempt
// leaves the buffered rows in place, so a retry sends the same rows again.
func (l *Loader) retryFlush(ctx context.Context, attempt func() (int, error)) (int, error) {
	waited := time.Duration(0)
	backoff := l.tooManyPartsBackoff
	failures := 0
	for {
		rows, err := attempt()
		if err == nil {
			return rows, nil
		}
		if isTooManyParts(err) && waited < l.tooManyPartsMaxWait {
			wait := min(backoff, l.tooManyPartsMaxWait-waited)
			l.logger.Warn("ClickHouse has too many parts, waiting for merges before retrying the flush",
				zap.Duration("wait", wait),
				zap.Duration("waited", waited),
				zap.Duration("max_wait", l.tooManyPartsMaxWait),
				zap.Error(err),
			)
			if err := l.sleep(ctx, wait); err != nil {
				return 0, err
			}
			waited += wait
			backoff = min(backoff*2, l.tooManyPartsMaxBackoff)
			continue
		}
		failures++
		l.logger.Warn("Flush attempt failed",
			zap.Int("attempt", failures),
			zap.Int("max_retries", l.maxFlushRetries),
			zap.Error(err),
		)
		if failures >= l.maxFlushRetries {
			return 0, fmt.Errorf("failed to flush after %d attempts: %w", failures, err)
		}
		if err := l.sleep(ctx, l.sleepBetweenFlushRetries); err != nil {
			return 0, err
		}
	}
}

// sleepContext waits for d, or returns the context's error if it ends first.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
