// Licensed under the Apache License, Version 2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func localClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	service.SetDefaultProjectName("test_project")
	client, err := NewClient(t.Context(), service, ClientOptions{TunnelEndpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func TestClientConfigurationAndDiscovery(t *testing.T) {
	ctx := t.Context()
	_, err := NewClient(ctx, nil, ClientOptions{})
	require.ErrorContains(t, err, "credentials")
	_, err = NewClient(ctx, odps.NewOdps(nil, "http://example.test"), ClientOptions{})
	require.ErrorContains(t, err, "credentials")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		project := r.URL.Query().Get("curr_project")
		assert.Contains(t, []string{"test_project", "another_project"}, project)
		assert.Equal(t, "/api/projects/"+project+"/tunnel", r.URL.Path)
		assert.Equal(t, "named", r.URL.Query().Get("quota"))
		assert.False(t, r.URL.Query().Has("quotaName"))
		assert.False(t, r.URL.Query().Has("service"))
		assert.Contains(t, r.Header.Get("Authorization"), "test-ak")
		fmt.Fprintln(w, r.Host)
	}))
	t.Cleanup(server.Close)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	_, err = NewClient(ctx, service, ClientOptions{TunnelEndpoint: server.URL})
	require.ErrorContains(t, err, "invalid project")
	service.SetDefaultProjectName("test_project")
	client, err := NewClient(ctx, service, ClientOptions{QuotaName: "named"})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.Equal(t, server.URL, client.rest.Endpoint())
	require.Equal(t, "default", client.schema)
	require.Equal(t, service.RestClient().HttpTimeout, client.rest.HttpTimeout)
	require.Equal(t, service.RestClient().TcpConnectionTimeout, client.rest.TcpConnectionTimeout)

	otherProject, err := NewClient(ctx, service, ClientOptions{Project: "another_project", QuotaName: "named"})
	require.NoError(t, err)
	t.Cleanup(otherProject.Close)
	require.Equal(t, server.URL, otherProject.rest.Endpoint())
	require.Equal(t, "another_project", otherProject.project)
	require.Equal(t, "test_project", service.DefaultProjectName())

	for _, endpoint := range []string{"http://%", "http:/path", "ftp://example.test", "http://u@example.test", "http://example.test?q=v", "http://example.test#hash"} {
		_, err := NewClient(ctx, service, ClientOptions{TunnelEndpoint: endpoint})
		require.ErrorContains(t, err, "invalid Tunnel endpoint")
	}
	explicit, err := NewClient(ctx, service, ClientOptions{
		Project: "another_project", TunnelEndpoint: server.URL + "/api/",
		HTTPTimeout: time.Minute, TCPConnectionTimeout: 2 * time.Second,
		UserAgent: "storage-test", DisableKeepAlives: true,
	})
	require.NoError(t, err)
	t.Cleanup(explicit.Close)
	require.Equal(t, server.URL, explicit.rest.Endpoint())
	require.Equal(t, "another_project", explicit.project)
	require.Equal(t, time.Minute, explicit.rest.HttpTimeout)
	require.Equal(t, 2*time.Second, explicit.rest.TcpConnectionTimeout)
	require.Equal(t, "storage-test", explicit.rest.UserAgent())
	require.True(t, explicit.rest.DisableKeepAlives)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = NewClient(canceled, service, ClientOptions{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestEndpointDiscoveryRejectsIncompleteResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.False(t, r.URL.Query().Has("quota"))
		w.Header().Set("Content-Length", "100")
		fmt.Fprint(w, r.Host)
	}))
	t.Cleanup(server.Close)
	credentials := account.NewAliyunAccount("test-ak", "test-sk")
	for _, tc := range []struct {
		endpoint string
		want     string
	}{
		{"http://%", "invalid URL escape"},
		{server.URL, "unexpected EOF"},
	} {
		service := odps.NewOdps(credentials, tc.endpoint)
		_, err := NewClient(t.Context(), service, ClientOptions{Project: "test_project"})
		require.ErrorContains(t, err, "resolve Tunnel endpoint")
		require.ErrorContains(t, err, tc.want)
	}
}

func TestSessionDescriptorsAndRejectedRequests(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/storage/v3", r.URL.Path)
		switch r.URL.Query().Get("Action") {
		case "InstanceGetReadSession":
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "route-old", r.Header.Get(routeTokenHeader))
			fmt.Fprint(w, `{"DownloadID":"i","RecordCount":0}`)
		case "TableCreateReadSession":
			w.Header().Set(routeTokenHeader, "route-new")
			fmt.Fprint(w, `{"SessionId":"s","RecordCount":3}`)
		case "TableGetReadSession":
			assert.Equal(t, "false", r.URL.Query().Get("session_refresh"))
			fmt.Fprint(w, `{"SessionId":"s","RecordCount":-1}`)
		case "TableCreateWriteSession":
			fmt.Fprint(w, `{}`)
		case "TablePreview":
			assert.Equal(t, "1", r.URL.Query().Get("Limit"))
			fmt.Fprint(w, "invalid Arrow header")
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ctx := t.Context()
	valid := Table{Name: "table"}
	bad := Table{Schema: "invalid.schema", Name: "table"}
	tableSession, err := client.CreateTableReadSession(ctx, valid, TableReadOptions{})
	require.NoError(t, err)
	require.Equal(t, "route-new", tableSession.Session().RouteToken)
	_, err = tableSession.OpenReader(ctx, &RowRange{Offset: -1, Count: 1})
	require.ErrorContains(t, err, "nonnegative")
	_, err = tableSession.OpenReader(ctx, &RowRange{Count: -1})
	require.ErrorContains(t, err, "nonnegative")
	_, err = client.GetTableReadSession(ctx, valid, Session{ID: "s"})
	require.ErrorContains(t, err, "invalid session descriptor")
	_, err = client.GetTableReadSession(ctx, valid, Session{})
	require.ErrorContains(t, err, "session ID")
	_, err = client.CreateTableReadSession(ctx, bad, TableReadOptions{})
	require.ErrorContains(t, err, "invalid schema")
	_, err = client.GetTableReadSession(ctx, bad, Session{})
	require.ErrorContains(t, err, "invalid schema")
	_, err = client.CreateTableReadSession(ctx, Table{Name: "bad table"}, TableReadOptions{})
	require.ErrorContains(t, err, "invalid table")

	instanceSession, err := client.GetInstanceReadSession(ctx, "instance", Session{ID: "i", RouteToken: "route-old"}, InstanceReadOptions{TaskName: "task"})
	require.NoError(t, err)
	require.Equal(t, "route-old", instanceSession.Session().RouteToken)
	_, err = client.GetInstanceReadSession(ctx, "instance", Session{}, InstanceReadOptions{})
	require.ErrorContains(t, err, "session ID")
	_, err = client.CreateInstanceReadSession(ctx, "invalid/instance", InstanceReadOptions{})
	require.ErrorContains(t, err, "invalid instance")
	_, err = client.CreateTableWriteSession(ctx, valid, TableWriteOptions{})
	require.ErrorContains(t, err, "missing session ID")
	_, err = client.CreateTableWriteSession(ctx, bad, TableWriteOptions{})
	require.ErrorContains(t, err, "invalid schema")
	_, err = client.GetTableWriteSession(ctx, valid, Session{})
	require.ErrorContains(t, err, "session ID")
	_, err = client.PreviewTable(ctx, valid, "", 1)
	require.Error(t, err)
	_, err = client.PreviewTable(ctx, bad, "", 1)
	require.ErrorContains(t, err, "invalid schema")
}

func makeArrowBatch(t *testing.T, values ...int64) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	defer column.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
	t.Cleanup(batch.Release)
	return batch
}

func makeIPC(t *testing.T, batch arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()), ipc.WithLZ4())
	require.NoError(t, writer.Write(batch))
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func TestOpenEmptyInstanceResult(t *testing.T) {
	schema := makeArrowBatch(t).Schema()
	var payload bytes.Buffer
	require.NoError(t, ipc.NewWriter(&payload, ipc.WithSchema(schema)).Close())
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		switch params.Get("Action") {
		case "InstanceCreateReadSession":
			fmt.Fprint(w, `{"DownloadID":"empty","RecordCount":0}`)
		case "InstanceRead":
			assert.Equal(t, "0", params.Get("Offset"))
			if params.Get("Count") != "1" {
				http.Error(w, "read count must be positive", http.StatusBadRequest)
				return
			}
			_, _ = w.Write(payload.Bytes())
		default:
			http.NotFound(w, r)
		}
	})

	ctx := t.Context()
	session, err := client.CreateInstanceReadSession(ctx, "instance", InstanceReadOptions{})
	require.NoError(t, err)
	require.Zero(t, session.RecordCount())
	reader, err := session.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Schema().Equal(schema))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func TestArrowReadLengthAndTruncation(t *testing.T) {
	payload := makeIPC(t, makeArrowBatch(t, 8))
	schemaEnd := 8 + int(binary.LittleEndian.Uint32(payload[4:8]))
	for _, tc := range []struct {
		name  string
		count int64
		input []byte
		first bool
		want  error
	}{
		{"short logical stream", 2, payload, true, io.ErrUnexpectedEOF},
		{"excess rows", 0, payload, false, nil},
		{"truncated message", 1, payload[:schemaEnd+3], false, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := newArrowReader(io.NopCloser(bytes.NewReader(tc.input)), tc.count)
			require.NoError(t, err)
			defer r.Release()
			require.Equal(t, tc.first, r.Next())
			require.False(t, r.Next())
			if tc.want != nil {
				require.ErrorIs(t, r.Err(), tc.want)
			} else {
				require.ErrorContains(t, r.Err(), "range")
			}
		})
	}
}

func TestWriteValidationAndMalformedAcknowledgements(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("Action") == "TableWrite" && r.URL.Query().Get("RowOffset") == "0" {
			fmt.Fprint(w, `{"ExactlyOnceRowOffset":0}`)
		} else {
			fmt.Fprint(w, "malformed response")
		}
	})
	session := &WriteSession{client: client, session: Session{ID: "s"}, target: "projects.test_project.schemas.default.tables.table"}
	_, err := session.CreateStream(t.Context(), Stream{Version: 1})
	require.ErrorContains(t, err, "stream ID")
	_, err = session.GetStream(t.Context(), Stream{ID: "s", Version: -1})
	require.ErrorContains(t, err, "version")
	writer := &WriteStream{session: session, stream: Stream{ID: "s", Version: 1}}
	_, err = writer.WriteBatch(t.Context(), 0, nil)
	require.ErrorContains(t, err, "nonempty")
	_, err = writer.WriteBatch(t.Context(), 0, makeArrowBatch(t))
	require.ErrorContains(t, err, "nonempty")
	batch := makeArrowBatch(t, 1)
	_, err = writer.WriteBatch(t.Context(), -1, batch)
	require.ErrorContains(t, err, "offset")
	_, err = writer.WriteBatch(t.Context(), 0, schemaLessBatch{batch})
	require.ErrorContains(t, err, "cannot write IPC stream without a schema")
	_, err = writer.WriteBatch(t.Context(), 0, batch)
	require.ErrorContains(t, err, "before the end")
	_, err = writer.WriteBatch(t.Context(), 1, batch)
	require.ErrorContains(t, err, "decode response")
	_, err = session.CreateStream(t.Context(), Stream{ID: "s", Version: 1})
	require.ErrorContains(t, err, "decode response")
}

type schemaLessBatch struct{ arrow.RecordBatch }

func (schemaLessBatch) Schema() *arrow.Schema { return nil }

func TestWriteSessionWireFormat(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		args := r.URL.Query()
		assert.Equal(t, "Batch", args.Get("WriteMode"))
		switch args.Get("Action") {
		case "TableCreateWriteSession":
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.JSONEq(t, `{"PartialPartitionSpec":"ds=20261119","Flags":{"overwrite":"true"}}`, string(body))
			fmt.Fprint(w, `{"SessionId":"s"}`)
		case "TableGetWriteStream":
			assert.Equal(t, "s", args.Get("SessionId"))
			assert.Equal(t, "worker", args.Get("StreamId"))
			assert.Equal(t, "2", args.Get("StreamVersion"))
			assert.Equal(t, "true", args.Get("ExactlyOnceMode"))
			var input struct {
				Stream
				ExactlyOnceMode bool
			}
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&input))
			assert.Equal(t, Stream{ID: "worker", Version: 2}, input.Stream)
			assert.True(t, input.ExactlyOnceMode)
			fmt.Fprint(w, `{"RowOffset":10}`)
		case "TableCommitWriteSession":
			w.Header().Set("Content-Length", "20")
			fmt.Fprint(w, "{}")
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	ctx := t.Context()
	session, err := client.CreateTableWriteSession(ctx, Table{Name: "table"}, TableWriteOptions{Partition: "ds=20261119", Overwrite: true})
	require.NoError(t, err)
	stream, err := session.GetStream(ctx, Stream{ID: "worker", Version: 2})
	require.NoError(t, err)
	require.EqualValues(t, 10, stream.InitialOffset())
	require.ErrorIs(t, session.Commit(ctx), io.ErrUnexpectedEOF)
}

func TestCommitStreamsRequiresExplicitVersions(t *testing.T) {
	var commits atomic.Int32
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		assert.Equal(t, "Batch", params.Get("WriteMode"))
		assert.Equal(t, "session-1", params.Get("SessionId"))
		assert.Equal(t, "route-1", r.Header.Get(routeTokenHeader))
		switch params.Get("Action") {
		case "TableGetWriteSession":
			fmt.Fprint(w, `{"SessionId":"session-1","SessionStatus":"NORMAL"}`)
		case "TableCommitWriteSession":
			commits.Add(1)
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.JSONEq(t, `{"StreamIds":["shard-b","shard-a"],"StreamVersions":[17,3]}`, string(body))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s", r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	ctx := t.Context()
	session, err := client.GetTableWriteSession(ctx, Table{Name: "table"}, Session{ID: "session-1", RouteToken: "route-1"})
	require.NoError(t, err)
	require.ErrorContains(t, session.CommitStreams(ctx), "at least one stream")
	require.ErrorContains(t, session.CommitStreams(ctx, Stream{}), "stream ID is required")
	require.ErrorContains(t, session.CommitStreams(ctx, Stream{ID: "shard-a", Version: -1}), "nonnegative")
	require.ErrorContains(t, session.CommitStreams(ctx, Stream{ID: "shard-a"}, Stream{ID: "shard-a", Version: 3}), "only be committed once")
	require.Zero(t, commits.Load())
	require.NoError(t, session.CommitStreams(ctx, Stream{ID: "shard-b", Version: 17}, Stream{ID: "shard-a", Version: 3}))
	require.EqualValues(t, 1, commits.Load())
}

func TestRequestEncodingErrors(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid operations should be rejected before transport")
		w.WriteHeader(http.StatusBadRequest)
	})
	op := operation{action: "TableRead", target: "projects.test_project.schemas.default.tables.table"}
	_, err := client.jsonRequest(t.Context(), http.MethodPost, op, make(chan string), nil)
	require.ErrorContains(t, err, "unsupported type")
	_, err = client.arrowRequest(t.Context(), op, make(chan string), 1)
	require.ErrorContains(t, err, "unsupported type")
	_, err = client.request(t.Context(), "not a method", op, strings.NewReader(""), nil)
	require.ErrorContains(t, err, "invalid method")
}
