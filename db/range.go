package db

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

var ErrRangeNotFound = errors.New("range not found")

// UpdateProcessedRange updates an existing processed range.
func (l *Loader) UpdateProcessedRange(ctx context.Context, tx Tx, moduleHash string, blockEnd uint64) error {

	if l.processedRangeTable == nil {
		return nil
	}

	l.logger.Debug("updating processed range",
		zap.String("module_hash", moduleHash),
		zap.Uint64("block_start", l.sinkRange.StartBlock()),
		zap.Uint64("block_end", blockEnd))

	query := l.getDialect().GetUpdateProcessedRangeQuery(l.schema, moduleHash, l.sinkRange.StartBlock(), blockEnd)

	var executor sqlExecutor = l.DB
	if tx != nil {
		executor = tx
	}

	result, err := executor.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("update processed range: %w", err)
	}

	if l.getDialect().DriverSupportRowsAffected() {
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rows affected: %w", err)
		}

		if rowsAffected <= 0 {
			return ErrRangeNotFound
		}
	}

	return nil
}
