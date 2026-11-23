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

	for _, key := range []string{"tcp_connection_timeout", "http_timeout"} {
		_, err := odps.NewConfigFromIni(writeConfig(t, minimalConfig+"endpoint = http://127.0.0.1:8080/api\n"+key+" = invalid\n"))
		require.ErrorContains(t, err, key)
	}
}

func TestFormatDSNValidatesAndReservesConfigurationFields(t *testing.T) {
	config := odps.NewConfig()
	config.ProjectName = "fixture"
	config.Endpoint = "https://fixture-secret@%"
	_, err := config.FormatDSN()
	require.ErrorContains(t, err, "endpoint")
	require.NotContains(t, err.Error(), "fixture-secret")
	require.Empty(t, config.FormatDsn())

	config.Endpoint = "http://127.0.0.1:8080/api"
	config.Hints = map[string]string{"project": "other", "stsToken": "other"}
	config.TcpConnectionTimeout = 0
	s, err := config.FormatDSN()
	require.NoError(t, err)
	u, err := url.Parse(s)
	require.NoError(t, err)
	require.Nil(t, u.User)
	require.Equal(t, "fixture", u.Query().Get("project"))
	require.Equal(t, "0", u.Query().Get("tcpConnectionTimeout"))
	require.False(t, u.Query().Has("stsToken"))
}
