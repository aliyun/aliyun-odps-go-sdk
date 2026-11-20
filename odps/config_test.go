// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

const minimalConfig = `[odps]
access_id = fake-id
access_key = fake-secret
project = fixture
`

func writeConfig(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "odps.ini")
	require.NoError(t, os.WriteFile(path, []byte(text), 0o600))
	return path
}

func TestConfigFromIniAndDSN(t *testing.T) {
	path := writeConfig(t, minimalConfig+`endpoint = http://127.0.0.1:8080/api
sts_token = fake-token
tcp_connection_timeout = 3
http_timeout = 9
tunnel_endpoint = http://127.0.0.1:8081
tunnel_quota_name = batch
enableLogview = false
hints.odps.sql.type.system.odps2 = true
hints = literal
`)
	config, err := odps.NewConfigFromIni(path)
	require.NoError(t, err)
	require.Equal(t, time.Duration(3)*time.Second, config.TcpConnectionTimeout)
	require.Equal(t, time.Duration(9)*time.Second, config.HttpTimeout)
	require.Equal(t, map[string]string{"odps.sql.type.system.odps2": "true", "hints": "literal"}, config.Hints)
	require.Equal(t, map[string]string{"enableLogview": "false"}, config.Others)
	require.Equal(t, account2.STS, config.GenAccount().GetType())
	require.Equal(t, "fixture", config.GenOdps().DefaultProjectName())
	client := config.GenRestClient()
	require.Equal(t, config.Endpoint, client.Endpoint())

	dsn, err := url.Parse(config.FormatDsn())
	require.NoError(t, err)
	require.Equal(t, "fake-id", dsn.User.Username())
	require.Equal(t, "/api", dsn.Path)
	require.Equal(t, "fixture", dsn.Query().Get("project"))
	require.Equal(t, "fake-token", dsn.Query().Get("stsToken"))
	require.Equal(t, "batch", dsn.Query().Get("tunnelQuotaName"))
	require.Equal(t, "true", dsn.Query().Get("odps.sql.type.system.odps2"))
	require.Equal(t, "false", dsn.Query().Get("enableLogview"))
}

func TestInvalidConfigFromIni(t *testing.T) {
	_, err := odps.NewConfigFromIni(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	_, err = odps.NewConfigFromIni(writeConfig(t, minimalConfig))
	require.ErrorContains(t, err, "endpoint")
	_, err = odps.NewConfigFromIni(writeConfig(t, minimalConfig+"endpoint = http://%\n"))
	require.ErrorContains(t, err, "invalid endpoint")

	config, err := odps.NewConfigFromIni(writeConfig(t, minimalConfig+`endpoint = http://127.0.0.1:8080/api
tcp_connection_timeout = invalid
http_timeout = invalid
`))
	require.NoError(t, err)
	require.Equal(t, odps.NewConfig().TcpConnectionTimeout, config.TcpConnectionTimeout)
	require.Equal(t, odps.NewConfig().HttpTimeout, config.HttpTimeout)
	require.Equal(t, account2.Aliyun, config.GenAccount().GetType())
	dsn, err := url.Parse(config.FormatDsn())
	require.NoError(t, err)
	require.False(t, dsn.Query().Has("stsToken"))
	require.False(t, dsn.Query().Has("httpTimeout"))
}
