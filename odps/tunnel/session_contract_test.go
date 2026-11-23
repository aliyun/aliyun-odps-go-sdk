// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
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

func TestPreviewUsesStorageQuotaAndHonorsExplicitEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		switch r.URL.Path {
		case "/api/projects/test_project/tunnel":
			assert.Equal(t, http.MethodGet, r.Method)
			if query.Has("service") {
				fmt.Fprint(w, r.Host+"/legacy")
				return
			}
			assert.Equal(t, "dedicated", query.Get("quota"))
			fmt.Fprint(w, r.Host+"/dedicated")
		case "/api/projects/test_project/tables/fixture":
			fmt.Fprint(w, `<Table><Name>fixture</Name><Project>test_project</Project><Schema>{"columns":[{"name":"id","type":"bigint"}]}</Schema></Table>`)
		case "/dedicated/api/storage/v3", "/override/api/storage/v3":
			assert.Equal(t, "TablePreview", query.Get("Action"))
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, `<Error><Code>FixtureAccessDenied</Code><Message>%s</Message></Error>`, r.URL.Path)
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	service.SetDefaultProjectName("test_project")
	table := service.Table("fixture")
	discovered, err := NewTunnelFromProject(service.DefaultProject())
	require.NoError(t, err)
	require.Equal(t, server.URL+"/legacy", discovered.GetEndpoint())
	discovered.SetQuotaName("dedicated")
	_, err = discovered.Preview(table, "", 1)
	require.ErrorContains(t, err, "/dedicated/api/storage/v3")
	require.Equal(t, server.URL+"/legacy", discovered.GetEndpoint())

	explicit := NewTunnel(service, server.URL+"/override")
	explicit.SetQuotaName("dedicated")
	_, err = explicit.Preview(table, "", 1)
	require.ErrorContains(t, err, "/override/api/storage/v3")
}

func TestTunnelDiscoveryTracksProjectAndQuota(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if path[0] == "api" && len(path) == 4 && path[3] == "tunnel" {
			quota := r.URL.Query().Get("quotaName")
			if quota == "" {
				quota = "default"
			}
			_, _ = fmt.Fprintf(w, "%s/routes/%s/%s", r.Host, path[2], quota)
			return
		}
		if len(path) == 7 && path[0] == "routes" && path[3] == "projects" && path[1] == path[4] {
			quota := r.URL.Query().Get("quotaName")
			if quota == "" {
				quota = "default"
			}
			if quota == path[2] {
				_, _ = fmt.Fprintf(w, "{\"UploadID\":%q,\"Status\":\"NORMAL\",\"Schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}}", path[1]+"/"+quota)
				return
			}
		}
		http.Error(w, "the request used the wrong project or quota route", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	transfer, err := NewTunnelFromProject(service.Project("alpha"))
	require.NoError(t, err)
	for _, tc := range []struct {
		project string
		quota   string
		want    string
	}{
		{"alpha", "", "alpha/default"},
		{"alpha", "batch", "alpha/batch"},
		{"beta", "batch", "beta/batch"},
		{"beta", "", "beta/default"},
	} {
		transfer.SetQuotaName(tc.quota)
		session, err := transfer.CreateUploadSession(tc.project, "sample")
		require.NoError(t, err)
		require.Equal(t, tc.want, session.Id)
		require.Equal(t, server.URL+"/routes/"+tc.want, transfer.GetEndpoint())
	}

	shared := NewTunnel(service)
	shared.SetQuotaName("batch")
	var group sync.WaitGroup
	for _, project := range []string{"alpha", "beta", "alpha", "beta"} {
		group.Go(func() {
			session, err := shared.CreateUploadSession(project, "sample")
			if assert.NoError(t, err) {
				assert.Equal(t, project+"/batch", session.Id)
				session.RestClient.CloseIdleConnections()
			}
		})
	}
	group.Wait()
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

func TestStreamP2PFlushUsesWorkerAddress(t *testing.T) {
	const routedServer = "127.0.0.1:4321"
	const resource = "/tunnel-route/projects/test_project/tables/sample/streams"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, resource, r.URL.Path)
		host, _, err := net.SplitHostPort(r.Host)
		assert.NoError(t, err)
		if r.Method == http.MethodPost {
			assert.Equal(t, "localhost", host)
			_, _ = fmt.Fprintf(w, "{\"session_name\":\"fixture\",\"schema_version\":4,\"slots\":[[0,%q]],\"status\":\"normal\",\"schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}}", routedServer)
			return
		}
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "127.0.0.1", host)
		assert.Equal(t, routedServer, r.Header.Get(common.HttpHeaderRoutedServer))
		assert.Equal(t, "stream-test", r.Header.Get(common.HttpHeaderXOdpsUserAgent))
		assert.Equal(t, "fixture", r.URL.Query().Get("uploadid"))
		payload, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.NotEmpty(t, payload)
		w.Header().Set(common.HttpHeaderOdpsRequestId, "worker-receipt")
		w.Header().Set(common.HttpHeaderOdpsSlotNum, "1")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)

	client := restclient.NewOdpsRestClient(account.NewAliyunAccount("test-ak", "test-sk"), "http://localhost:"+port+"/tunnel-route")
	client.SetUserAgent("stream-test")
	session, err := CreateStreamUploadSession("test_project", "sample", client)
	require.NoError(t, err)
	t.Cleanup(session.RestClient.CloseIdleConnections)
	session.P2PMode = true
	writer := session.OpenRecordPackWriter()
	require.NoError(t, writer.Append(data.Record{data.BigInt(7)}))
	receipt, count, _, err := writer.Flush(2 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "worker-receipt", receipt)
	require.EqualValues(t, 1, count)
}

func TestUploadOverwriteOptionsOnTheWire(t *testing.T) {
	client := tunnelContractClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/projects/test_project/schemas/reporting/tables/fixture", r.URL.Path)
		query := r.URL.Query()
		assert.True(t, query.Has("uploads"))
		assert.Equal(t, "ds=20261120", query.Get("partition"))
		assert.Equal(t, "true", query.Get("create_partition"))
		assert.Equal(t, "true", query.Get("overwrite"))
		_, _ = fmt.Fprint(w, `{"UploadID":"fixture","Status":"NORMAL","IsOverwrite":true,"Schema":{"columns":[{"name":"id","type":"bigint"}]}}`)
	})
	session, err := CreateUploadSession("test_project", "fixture", "", client,
		SessionCfg.WithSchemaName("reporting"), SessionCfg.WithPartitionKey("ds='20261120'"), SessionCfg.WithCreatePartition(),
		SessionCfg.Overwrite())
	require.NoError(t, err)
	require.True(t, session.Overwrite)
	require.Equal(t, "fixture", session.Id)
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

func TestAttachedSessionsKeepNamedQuota(t *testing.T) {
	const quota = "dedicated"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, quota, q.Get("quotaName"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/tunnel"):
			_, _ = fmt.Fprint(w, r.Host+"/routed")
		case q.Has("uploadid"):
			fmt.Fprint(w, "{\"UploadID\":\"u\",\"Status\":\"NORMAL\",\"Schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}}")
		case q.Has("downloadid") && q.Has("data"):
			w.WriteHeader(http.StatusForbidden)
		case q.Has("downloadid"):
			fmt.Fprint(w, "{\"DownloadID\":\"d\",\"Status\":\"NORMAL\",\"RecordCount\":1,\"Schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}}")
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	client := NewTunnel(service)
	client.SetQuotaName(quota)
	upload, err := client.AttachToExistedUploadSession("project", "table", "u")
	require.NoError(t, err)
	require.Equal(t, quota, upload.QuotaName)
	download, err := client.AttachToExistedDownloadSession("project", "table", "d")
	require.NoError(t, err)
	require.Equal(t, quota, download.QuotaName)
	_, err = download.OpenRecordReader(0, 1, nil)
	require.Error(t, err)
}

func TestScopedSQLResultPoolLifetime(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("keepalive=%t", reuse), func(t *testing.T) {
			var idle, closed atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/projects/project/instances/known", r.URL.Path)
				assert.Equal(t, "project", r.URL.Query().Get("curr_project"))
				fmt.Fprint(w, "{\"DownloadID\":\"session\",\"Status\":\"NORMAL\",\"RecordCount\":0,\"Schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}}")
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateIdle {
					idle.Add(1)
				}
				if state == http.StateClosed {
					closed.Add(1)
				}
			}
			server.Start()
			t.Cleanup(server.Close)
			client, err := odps.NewClient(account.NewAliyunAccount("fixture-ak", "fixture-sk"), odps.ClientOptions{
				Endpoint: server.URL, Project: "project",
			})
			require.NoError(t, err)
			t.Cleanup(client.CloseIdleConnections)
			session, err := OpenSQLResult(t.Context(), client, odps.SQLJobRef{InstanceID: "known"},
				ResultOptions{Endpoint: server.URL, KeepAlive: reuse})
			require.NoError(t, err)
			if reuse {
				require.Eventually(t, func() bool { return idle.Load() > 0 }, time.Second, time.Millisecond)
				session.CloseIdleConnections()
			}
			require.Eventually(t, func() bool { return closed.Load() > 0 }, time.Second, time.Millisecond)
		})
	}
}

func TestOpenSQLResultRejectsMaxQAReferenceBeforeDiscovery(t *testing.T) {
	client, err := odps.NewClient(account.NewAliyunAccount("test-ak", "test-sk"),
		odps.ClientOptions{Endpoint: "https://api.example.test", Project: "project"})
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	_, err = OpenSQLResult(t.Context(), client, odps.SQLJobRef{InstanceID: "known", MaxQA: true}, ResultOptions{})
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestSQLResultKeepsTheReferencedTaskAcrossTunnelProtocols(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if requests.Add(1) == 1 {
			assert.False(t, q.Has("cached"))
			assert.False(t, q.Has("taskname"))
		} else {
			assert.True(t, q.Has("cached"))
			assert.Equal(t, "original", q.Get("taskname"))
			assert.Equal(t, "7", q.Get("queryid"))
		}
		fmt.Fprint(w, `{"DownloadID":"session","Status":"NORMAL","RecordCount":0,"Schema":{"columns":[]}}`)
	}))
	defer server.Close()
	client, err := odps.NewClient(account.NewAliyunAccount("test-ak", "test-sk"), odps.ClientOptions{Endpoint: server.URL, Project: "project"})
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	ref := odps.SQLJobRef{InstanceID: "job", TaskName: "original"}
	route := ResultOptions{Endpoint: server.URL}
	_, err = OpenSQLResult(t.Context(), client, ref, route, InstanceSessionCfg.WithTaskName("other"))
	require.ErrorContains(t, err, "task name conflicts")
	_, err = OpenSQLResult(t.Context(), client, odps.SQLJobRef{InstanceID: "job"}, route, InstanceSessionCfg.WithQueryId(7))
	require.ErrorContains(t, err, "requires a task name")
	require.Zero(t, requests.Load())
	for _, options := range [][]InstanceOption{nil, {InstanceSessionCfg.WithQueryId(7)}} {
		session, err := OpenSQLResult(t.Context(), client, ref, route, options...)
		require.NoError(t, err)
		session.CloseIdleConnections()
	}
	require.EqualValues(t, 2, requests.Load())
}
