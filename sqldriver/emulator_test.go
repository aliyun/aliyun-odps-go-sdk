// Licensed under the Apache License, Version 2.0.

package sqldriver_test

import (
	"database/sql"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func TestEmulatorDriverReadsProtobufResults(t *testing.T) {
	endpoint := os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("MAXCOMPUTE_EMULATOR_ENDPOINT is unset")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback(), "emulator must run on loopback")
	config := sqldriver.NewConfig()
	config.AccessId = "test-ak"
	config.AccessKey = "test-sk"
	config.ProjectName = "test_project"
	config.Endpoint = endpoint + "/api"
	config.TunnelEndpoint = endpoint
	config.HttpTimeout = 5 * time.Second
	db, err := sql.Open("odps", config.FormatDsn())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	rows, err := db.QueryContext(t.Context(), "select cast(42 as bigint) as id, 'ok' as note, cast(null as bigint) as missing")
	require.NoError(t, err)
	defer rows.Close()
	fields, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, []string{"id", "note", "missing"}, fields)
	require.True(t, rows.Next(), "%v", rows.Err())
	var id int64
	var note string
	var missing sql.NullInt64
	require.NoError(t, rows.Scan(&id, &note, &missing))
	require.EqualValues(t, 42, id)
	require.Equal(t, "ok", note)
	require.False(t, missing.Valid)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	_, err = db.QueryContext(t.Context(), "select from")
	require.Error(t, err)
}
