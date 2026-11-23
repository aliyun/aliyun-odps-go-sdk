// Licensed under the Apache License, Version 2.0.

package sqldriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDSNIncludesExplicitOptions(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env-id")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env-secret")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "env-token")
	config, err := ParseDSN("http://dsn-id:dsn-secret@127.0.0.1:8080/api?project=fixture&stsToken=dsn-token" +
		"&tcpConnectionTimeout=4&httpTimeout=9&tunnelEndpoint=http%3A%2F%2F127.0.0.1%3A8081" +
		"&enableLogview=false&odps.sql.type.system.odps2=true")
	require.NoError(t, err)
	require.Equal(t, "dsn-id", config.AccessId)
	require.Equal(t, "dsn-secret", config.AccessKey)
	require.Equal(t, "dsn-token", config.StsToken)
	require.Equal(t, "fixture", config.ProjectName)
	require.Equal(t, "http://127.0.0.1:8080/api", config.Endpoint)
	require.Equal(t, "http://127.0.0.1:8081", config.TunnelEndpoint)
	require.Empty(t, config.TunnelQuotaName)
	require.Equal(t, 4*time.Second, config.TcpConnectionTimeout)
	require.Equal(t, 9*time.Second, config.HttpTimeout)
	require.Equal(t, map[string]string{"enableLogview": "false"}, config.Others)
	require.Equal(t, map[string]string{"odps.sql.type.system.odps2": "true"}, config.Hints)

	config, err = ParseDSN("http://127.0.0.1:8080/api?project=fixture&tunnelQuotaName=batch")
	require.NoError(t, err)
	require.Equal(t, "batch", config.TunnelQuotaName)
	require.Equal(t, "env-id", config.AccessId)
	require.Equal(t, "env-secret", config.AccessKey)
	require.Equal(t, "env-token", config.StsToken)
	require.Equal(t, NewConfig().TcpConnectionTimeout, config.TcpConnectionTimeout)
	require.Equal(t, NewConfig().HttpTimeout, config.HttpTimeout)
	require.Empty(t, config.Hints)
}

func TestParseDSNReservesTunnelOptionAliases(t *testing.T) {
	const dsn = "http://fixture-id:fixture-key@127.0.0.1:8080/api?project=fixture"
	const transfer = "http%3A%2F%2F127.0.0.1%3A8081"
	config, err := ParseDSN(dsn + "&TunnelEndpoint=" + transfer + "&odps.sql.type.system.odps2=true")
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:8080/api", config.Endpoint)
	require.Equal(t, "http://127.0.0.1:8081", config.TunnelEndpoint)
	require.Equal(t, map[string]string{"odps.sql.type.system.odps2": "true"}, config.Hints)

	config, err = ParseDSN(dsn + "&TunnelQuotaName=batch")
	require.NoError(t, err)
	require.Equal(t, "batch", config.TunnelQuotaName)
	require.Empty(t, config.Hints)

	_, err = ParseDSN(dsn + "&TunnelEndpoint=" + transfer + "&tunnelEndpoint=" + transfer)
	require.ErrorContains(t, err, "duplicate tunnelEndpoint")
	_, err = ParseDSN(dsn + "&TunnelEndpoint=" + transfer + "&TunnelQuotaName=batch")
	require.ErrorContains(t, err, "mutually exclusive")

	config.Hints = map[string]string{"TunnelEndpoint": "https://example.invalid"}
	config.Others = map[string]string{"TUNNELQUOTANAME": "wrong"}
	formatted, err := config.FormatDSN()
	require.NoError(t, err)
	reparsed, err := ParseDSN(formatted)
	require.NoError(t, err)
	require.Equal(t, "batch", reparsed.TunnelQuotaName)
	require.Empty(t, reparsed.TunnelEndpoint)
	require.Empty(t, reparsed.Hints)
}

func TestParseDSNRejectsIncompleteCredentials(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "")
	for _, tc := range []struct{ dsn, message string }{
		{"http://%", "invalid URL"},
		{"http://127.0.0.1/api?project=fixture", "AccessId"},
		{"http://fake-id@127.0.0.1/api?project=fixture", "AccessKey"},
		{"http://fake-id:fake-secret@127.0.0.1/api", "project name"},
	} {
		_, err := ParseDSN(tc.dsn)
		require.ErrorContains(t, err, tc.message)
	}
}

func TestParseDSNUsesCredentialsFromOneSource(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "fixture-env-id")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "fixture-env-secret")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "fixture-env-token")
	config, err := ParseDSN("https://explicit:explicit-secret@api.example.test/api?project=fixture")
	require.NoError(t, err)
	require.Equal(t, "explicit", config.AccessId)
	require.Empty(t, config.StsToken)

	for _, tt := range []struct{ dsn, message string }{
		{"https://explicit@api.example.test/api?project=fixture", "AccessKey"},
		{"https://api.example.test/api?project=fixture&stsToken=explicit", "explicit AccessId"},
		{"https://explicit:explicit-secret@api.example.test/api?project=fixture&httpTimeout=-5", "httpTimeout"},
		{"https://explicit:explicit-secret@api.example.test/api?project=a&project=b", "duplicate project"},
		{"https://explicit:explicit-secret@api.example.test/api?project=%", "query parameters"},
		{"https://explicit:explicit-secret@%/api?project=fixture", "invalid URL"},
	} {
		_, err := ParseDSN(tt.dsn)
		require.ErrorContains(t, err, tt.message)
		require.NotContains(t, err.Error(), "explicit-secret")
	}
}

func TestDSNCredentialWhitespaceAndInvalidRoute(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "fixture-id")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "fixture-secret")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "  \t")
	dsn := "https://api.example.test/api?project=fixture"
	config, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.Empty(t, config.StsToken)
	for _, invalid := range []string{
		"/api?project=fixture",
		dsn + "&tcpConnectionTimeout=invalid",
		dsn + "&tunnelEndpoint=ftp%3A%2F%2Fexample.test",
	} {
		_, err := ParseDSN(invalid)
		require.Error(t, err)
	}
	for _, idOrKey := range []struct{ id, key string }{{"  ", "real-key"}, {"real-id", "\t"}} {
		config.AccessId, config.AccessKey = idOrKey.id, idOrKey.key
		_, err := config.FormatDSN()
		require.ErrorContains(t, err, "incomplete explicit")
	}
}

func TestConnectorAppliesDSNDialTimeout(t *testing.T) {
	const dsn = "https://test-id:test-key@api.example.test/api?project=fixture"
	for _, tc := range []struct {
		param string
		want  time.Duration
	}{
		{"", 30 * time.Second},
		{"&tcpConnectionTimeout=0", 0},
		{"&tcpConnectionTimeout=4", 4 * time.Second},
	} {
		driverConnector, err := (OdpsDriver{}).OpenConnector(dsn + tc.param)
		require.NoError(t, err)
		connector := driverConnector.(*Connector)
		require.Equal(t, tc.want, connector.client.RestClient().TcpConnectionTimeout)
		require.NoError(t, connector.Close())
	}
}
