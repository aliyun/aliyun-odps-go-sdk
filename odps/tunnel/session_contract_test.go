// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

func tunnelContractClient(t *testing.T, handler http.HandlerFunc) restclient.RestClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := restclient.NewOdpsRestClient(account.NewAliyunAccount("test-ak", "test-sk"), server.URL)
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestSessionRequestConstructionErrors(t *testing.T) {
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), "http://%")
	_, err := NewTunnelFromProject(service.Project("test_project"))
	require.Error(t, err)
	for _, transfer := range []struct {
		name   string
		tunnel *Tunnel
	}{
		{"discovery", NewTunnel(service)},
		{"session endpoint", NewTunnel(service, "http://%")},
	} {
		t.Run(transfer.name, func(t *testing.T) {
			tu := transfer.tunnel
			_, err := tu.CreateUploadSession("test_project", "sample")
			require.Error(t, err)
			_, err = tu.AttachToExistedUploadSession("test_project", "sample", "upload")
			require.Error(t, err)
			_, err = tu.CreateDownloadSession("test_project", "sample")
			require.Error(t, err)
			_, err = tu.AttachToExistedDownloadSession("test_project", "sample", "download")
			require.Error(t, err)
			_, err = tu.CreateInstanceResultDownloadSession("test_project", "instance")
			require.Error(t, err)
			_, err = tu.CreateStreamUploadSession("test_project", "sample")
			require.Error(t, err)
		})
	}
}

func TestSessionRejectsInvalidMetadata(t *testing.T) {
	protocol := []struct {
		name   string
		create func(restclient.RestClient) error
	}{
		{"upload", func(client restclient.RestClient) error {
			_, err := CreateUploadSession("test_project", "sample", "", client)
			return err
		}},
		{"download", func(client restclient.RestClient) error {
			_, err := CreateDownloadSession("test_project", "sample", "", client)
			return err
		}},
		{"instance", func(client restclient.RestClient) error {
			_, err := CreateInstanceResultDownloadSession("test_project", "instance", "", client)
			return err
		}},
		{"stream", func(client restclient.RestClient) error {
			_, err := CreateStreamUploadSession("test_project", "sample", client)
			return err
		}},
	}
	for _, tc := range []struct {
		name, payload, want string
	}{
		{"bad JSON", "{", "unexpected EOF"},
		{"bad data column", `{"Schema":{"columns":[{"name":"id","type":"no_such_type"}]}}`, "no_such_type"},
		{"bad partition column", `{"Schema":{"columns":[{"name":"id","type":"bigint"}],"partitionKeys":[{"name":"ds","type":"no_such_type"}]}}`, "no_such_type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tunnelContractClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				_, _ = io.WriteString(w, tc.payload)
			})
			for _, test := range protocol {
				t.Run(test.name, func(t *testing.T) {
					require.ErrorContains(t, test.create(client), tc.want)
				})
			}
		})
	}
}

func TestStreamUploadMetadataRouting(t *testing.T) {
	for _, tc := range []struct {
		name, status, schemaVersion, slots, want string
	}{
		{"number fields", "normal", "4", `[[0,"127.0.0.1:8080"]]`, ""},
		{"string fields", "normal", `"4"`, `[["0","127.0.0.1:8080"]]`, ""},
		{"pending", "init", "4", "[]", "initiating"},
		{"empty slots", "normal", "4", "[]", "No slot"},
		{"short slot tuple", "normal", "4", "[[0]]", "expected an ID and server"},
		{"invalid slot id", "normal", "4", `[["invalid","127.0.0.1:8080"]]`, "slot 0 ID"},
		{"missing slot id", "normal", "4", `[[null,"127.0.0.1:8080"]]`, "missing stream slot"},
		{"invalid server type", "normal", "4", "[[0,123]]", "slot 0 server"},
		{"invalid server address", "normal", "4", `[[0,"invalid"]]`, "invalid slot format"},
		{"invalid schema version", "normal", "1.25", `[[0,"127.0.0.1:8080"]]`, "invalid stream schema version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := tunnelContractClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.True(t, r.URL.Query().Has("create_partition"))
				w.Header().Set("x-odps-request-id", "fixture-request")
				_, _ = fmt.Fprintf(w, `{"session_name":"fixture","schema_version":%s,"slots":%s,"status":%q,"quota_name":"fixture-quota","schema":{"columns":[{"name":"id","type":"bigint"}]}}`,
					tc.schemaVersion, tc.slots, tc.status)
			})
			session, err := CreateStreamUploadSession("test_project", "sample", client, SessionCfg.WithCreatePartition())
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 4, session.SchemaVersion())
			require.Equal(t, "fixture-quota", session.QuotaName)
			route, err := session.slotSelector.NextSlot()
			require.NoError(t, err)
			require.Equal(t, "0", route.id)
			require.Equal(t, "127.0.0.1:8080", route.Server())
		})
	}
}

func TestTunnelSessionStatusRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		want UploadStatus
	}{
		{"NORMAL", UploadStatusNormal},
		{"CLOSING", UploadStatusClosing},
		{"CLOSED", UploadStatusClosed},
		{"CANCELED", UploadStatusCanceled},
		{"EXPIRED", UploadStatusExpired},
		{"CRITICAL", UploadStatusCritical},
		{"COMMITTING", UploadStatusCommitting},
		{"UNKNOWN", UploadStatusUnknown},
	} {
		require.Equal(t, tc.want, UploadStatusFromStr(strings.ToLower(tc.name)))
		require.Equal(t, tc.name, tc.want.String())
	}
	require.Equal(t, UploadStatusUnknown.String(), UploadStatus(-1).String())

	for _, tc := range []struct {
		name string
		want DownLoadStatus
	}{
		{"NORMAL", DownloadStatusNormal},
		{"CLOSED", DownloadStatusClosed},
		{"EXPIRED", DownloadStatusExpired},
		{"INITIATING", DownloadStatusInitiating},
		{"UNKNOWN", DownloadStatusUnknown},
	} {
		require.Equal(t, tc.want, DownloadStatusFromStr(strings.ToLower(tc.name)))
		require.Equal(t, tc.name, tc.want.String())
	}
	require.Equal(t, DownloadStatusUnknown, DownloadStatusFromStr("future status"))
	require.Equal(t, DownloadStatusUnknown.String(), DownLoadStatus(-1).String())
}
