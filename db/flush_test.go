package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIsTooManyParts(t *testing.T) {
	tooManyParts := &clickhouse.Exception{Code: 252, Name: "DB::Exception", Message: "Too many parts (3006 with average size of 235.70 MiB) in table"}
	assert.True(t, isTooManyParts(tooManyParts))
	assert.True(t, isTooManyParts(fmt.Errorf("dialect flush: failed to commit db transaction: %w", tooManyParts)))
	assert.True(t, isTooManyParts(errors.New("failed to commit db transaction: code: 252, message: Too many parts")))
	assert.False(t, isTooManyParts(&clickhouse.Exception{Code: 241, Message: "Memory limit exceeded"}))
	assert.False(t, isTooManyParts(errors.New("failed to commit db transaction: code: 2520, message: other")))
	assert.False(t, isTooManyParts(errors.New("connection reset by peer")))
}

// retryLoader is a loader with the default retry settings and a sleep that
// records its waits instead of waiting.
func retryLoader(waits *[]time.Duration) *Loader {
	return &Loader{
		logger:                   zap.NewNop(),
		maxFlushRetries:          3,
		sleepBetweenFlushRetries: 5 * time.Second,
		tooManyPartsBackoff:      DefaultTooManyPartsBackoff,
		tooManyPartsMaxBackoff:   DefaultTooManyPartsMaxBackoff,
		tooManyPartsMaxWait:      DefaultTooManyPartsMaxWait,
		sleep: func(ctx context.Context, d time.Duration) error {
			*waits = append(*waits, d)
			return ctx.Err()
		},
	}
}

// failing returns an attempt that fails with errs in order, then flushes 7 rows.
func failing(errs ...error) (func() (int, error), *int) {
	calls := 0
	return func() (int, error) {
		calls++
		if calls <= len(errs) {
			return 0, errs[calls-1]
		}
		return 7, nil
	}, &calls
}

func TestRetryFlushWaitsOutTooManyParts(t *testing.T) {
	tooManyParts := &clickhouse.Exception{Code: 252}
	var waits []time.Duration
	l := retryLoader(&waits)

	// Five TOO_MANY_PARTS errors wait 10s, 20s, 40s, 80s and then 120s (the
	// longest single wait), and use none of the three attempts.
	attempt, calls := failing(tooManyParts, tooManyParts, tooManyParts, tooManyParts, tooManyParts)
	rows, err := l.retryFlush(context.Background(), attempt)
	require.NoError(t, err)
	assert.Equal(t, 7, rows)
	assert.Equal(t, 6, *calls)
	assert.Equal(t, []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 120 * time.Second}, waits)

	// Other errors still get three attempts, 5s apart; a TOO_MANY_PARTS wait
	// between them does not count as one.
	waits = nil
	reset := errors.New("connection reset")
	attempt, calls = failing(reset, tooManyParts, reset)
	rows, err = l.retryFlush(context.Background(), attempt)
	require.NoError(t, err)
	assert.Equal(t, 7, rows)
	assert.Equal(t, 4, *calls)
	assert.Equal(t, []time.Duration{5 * time.Second, 10 * time.Second, 5 * time.Second}, waits)

	waits = nil
	attempt, calls = failing(reset, reset, reset)
	_, err = l.retryFlush(context.Background(), attempt)
	assert.ErrorContains(t, err, "failed to flush after 3 attempts: connection reset")
	assert.Equal(t, 3, *calls)
	assert.Equal(t, []time.Duration{5 * time.Second, 5 * time.Second}, waits)
}

func TestRetryFlushTooManyPartsMaxWait(t *testing.T) {
	tooManyParts := &clickhouse.Exception{Code: 252}
	var waits []time.Duration
	l := retryLoader(&waits)
	l.SetTooManyPartsMaxWait(25 * time.Second)

	// After 25s of waits, the error counts as a failed attempt.
	errs := make([]error, 10)
	for i := range errs {
		errs[i] = tooManyParts
	}
	attempt, calls := failing(errs...)
	_, err := l.retryFlush(context.Background(), attempt)
	assert.ErrorContains(t, err, "failed to flush after 3 attempts")
	assert.True(t, isTooManyParts(err))
	assert.Equal(t, 5, *calls)
	assert.Equal(t, []time.Duration{10 * time.Second, 15 * time.Second, 5 * time.Second, 5 * time.Second}, waits)

	// 0 treats it like any other error.
	waits = nil
	l.SetTooManyPartsMaxWait(0)
	attempt, calls = failing(errs...)
	_, err = l.retryFlush(context.Background(), attempt)
	assert.ErrorContains(t, err, "failed to flush after 3 attempts")
	assert.Equal(t, 3, *calls)
	assert.Equal(t, []time.Duration{5 * time.Second, 5 * time.Second}, waits)
}

func TestRetryFlushSettings(t *testing.T) {
	var waits []time.Duration
	l := retryLoader(&waits)
	l.SetFlushRetries(1, time.Second)

	attempt, calls := failing(errors.New("connection reset"))
	_, err := l.retryFlush(context.Background(), attempt)
	assert.ErrorContains(t, err, "failed to flush after 1 attempts")
	assert.Equal(t, 1, *calls)
	assert.Empty(t, waits)

	// At least one attempt.
	l.SetFlushRetries(0, time.Second)
	assert.Equal(t, 1, l.maxFlushRetries)
}

func TestRetryFlushStopsWithTheContext(t *testing.T) {
	var waits []time.Duration
	l := retryLoader(&waits)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempt, calls := failing(&clickhouse.Exception{Code: 252})
	_, err := l.retryFlush(ctx, attempt)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, *calls)

	// The real sleep returns as soon as the context ends.
	start := time.Now()
	assert.ErrorIs(t, sleepContext(ctx, time.Hour), context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}
