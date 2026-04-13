package db

import (
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadClickhouseInsertableColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	loader := &Loader{DB: db}

	rows := sqlmock.NewRows([]string{"name", "type", "default_kind"}).
		AddRow("block_num", "UInt32", "").
		AddRow("block_hash", "String", "EPHEMERAL").
		AddRow("timestamp", "DateTime('UTC')", "").
		AddRow("minute", "UInt32", "MATERIALIZED").
		AddRow("contract_alias", "String", "ALIAS")

	mock.ExpectQuery("FROM\\s+system\\.columns").
		WithArgs("default", "erc20_balances").
		WillReturnRows(rows)

	columns, err := loader.loadClickhouseInsertableColumns("default", "erc20_balances")
	require.NoError(t, err)

	assert.Contains(t, columns, "block_num")
	assert.Contains(t, columns, "block_hash")
	assert.Contains(t, columns, "timestamp")
	assert.NotContains(t, columns, "minute")
	assert.NotContains(t, columns, "contract_alias")

	assert.Equal(t, reflect.TypeOf(uint32(0)), columns["block_num"].scanType)
	assert.Equal(t, reflect.TypeOf(""), columns["block_hash"].scanType)
	assert.Equal(t, reflect.TypeOf(time.Time{}), columns["timestamp"].scanType)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestShouldSkipClickhouseColumn(t *testing.T) {
	assert.False(t, shouldSkipClickhouseColumn(""))
	assert.False(t, shouldSkipClickhouseColumn("DEFAULT"))
	assert.False(t, shouldSkipClickhouseColumn("EPHEMERAL"))
	assert.True(t, shouldSkipClickhouseColumn("materialized"))
	assert.True(t, shouldSkipClickhouseColumn("ALIAS"))
}
