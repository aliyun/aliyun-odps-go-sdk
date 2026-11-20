// Licensed under the Apache License, Version 2.0.

package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/storage"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

type emulator struct {
	endpoint string
	service  *odps.Odps
	storage  *storage.Client
}

func newEmulator(t *testing.T) emulator {
	t.Helper()
	endpoint := os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("MAXCOMPUTE_EMULATOR_ENDPOINT is unset")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	host := net.ParseIP(u.Hostname())
	require.True(t, u.Scheme == "http" && host.IsLoopback(), "emulator must use a loopback HTTP endpoint")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+"/capabilities", nil)
	require.NoError(t, err)
	localHTTP := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := localHTTP.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var capabilities struct {
		Version    string
		TestFaults bool `json:"test_faults"`
		StorageV2  bool `json:"storage_v2"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&capabilities))
	require.Equal(t, "1.2.0", capabilities.Version)
	require.True(t, capabilities.StorageV2)
	service := odps.NewOdps(account.NewAliyunAccount("test-ak", "test-sk"), endpoint+"/api")
	service.SetDefaultProjectName("test_project")
	service.SetCurrentSchemaName("default")
	client, err := storage.NewClient(t.Context(), service, storage.ClientOptions{})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return emulator{endpoint: endpoint, service: service, storage: client}
}

func (e emulator) sql(t *testing.T, sql string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	instance, err := e.service.ExecSQl(sql)
	require.NoError(t, err, "%s", sql)
	require.NoError(t, instance.WaitForSuccessContext(ctx), "%s", sql)
	return instance.Id()
}

func (e emulator) table(t *testing.T, columns string) storage.Table {
	t.Helper()
	return e.createTable(t, "("+columns+")")
}

func (e emulator) createTable(t *testing.T, definition string) storage.Table {
	t.Helper()
	name := fmt.Sprintf("go_storage_%d", time.Now().UnixNano())
	e.sql(t, fmt.Sprintf("create table %s %s", name, definition))
	t.Cleanup(func() { e.sql(t, "drop table "+name) })
	return storage.Table{Name: name}
}

func serviceError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var got restclient.HttpError
	require.ErrorAs(t, err, &got)
	require.Equal(t, status, got.StatusCode)
	require.NotNil(t, got.ErrorMessage)
	require.Equal(t, code, got.ErrorMessage.ErrorCode)
}

func TestEmulatorInstanceReadSession(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	id := e.sql(t, "select id from (select cast(10 as bigint) as id union all select cast(20 as bigint) as id) q order by id")
	session, err := e.storage.CreateInstanceReadSession(ctx, id, storage.InstanceReadOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 2, session.RecordCount())
	require.Equal(t, "NORMAL", strings.ToUpper(session.Status()))
	resumed, err := e.storage.GetInstanceReadSession(ctx, id, session.Session(), storage.InstanceReadOptions{})
	require.NoError(t, err)
	reader, err := resumed.OpenReader(ctx, &storage.RowRange{Offset: 1, Count: 1})
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, "id", reader.Schema().Field(0).Name)
	require.True(t, reader.Next(), "%v", reader.Err())
	require.EqualValues(t, 20, reader.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())

	_, err = e.storage.CreateInstanceReadSession(ctx, "missing_instance", storage.InstanceReadOptions{})
	serviceError(t, err, http.StatusNotFound, "NoSuchInstance")
	_, err = e.storage.GetInstanceReadSession(ctx, id, storage.Session{ID: "missing_session"}, storage.InstanceReadOptions{})
	serviceError(t, err, http.StatusNotFound, "NoSuchSession")
}

func TestEmulatorEmptyInstanceReadSession(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	id := e.sql(t, "select cast(1 as bigint) as id where 1 = 0")
	session, err := e.storage.CreateInstanceReadSession(ctx, id, storage.InstanceReadOptions{})
	require.NoError(t, err)
	require.Zero(t, session.RecordCount())

	reopened, err := e.storage.GetInstanceReadSession(ctx, id, session.Session(), storage.InstanceReadOptions{})
	require.NoError(t, err)
	reader, err := reopened.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, "id", reader.Schema().Field(0).Name)
	require.Equal(t, arrow.PrimitiveTypes.Int64, reader.Schema().Field(0).Type)
	for reader.Next() {
		require.Zero(t, reader.RecordBatch().NumRows())
	}
	require.NoError(t, reader.Err())
}

func testBatch(t *testing.T, id int64, note string) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(id)
	builder.Field(1).(*array.StringBuilder).Append(note)
	batch := builder.NewRecordBatch()
	t.Cleanup(batch.Release)
	return batch
}

func TestEmulatorTableBatchUpload(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	table := e.table(t, "id bigint, note string")
	upload, err := e.storage.CreateTableWriteSession(ctx, table, storage.TableWriteOptions{})
	require.NoError(t, err)
	require.Equal(t, "NORMAL", strings.ToUpper(upload.Status()))
	key := storage.Stream{ID: "batch", Version: 1}
	stream, err := upload.CreateStream(ctx, key)
	require.NoError(t, err)
	require.Zero(t, stream.InitialOffset())
	serviceError(t, upload.Commit(ctx), http.StatusConflict, "InvalidState")

	first := testBatch(t, 1, "first")
	second := testBatch(t, 2, "second")
	offset, err := stream.WriteBatch(ctx, 0, first)
	require.NoError(t, err)
	require.EqualValues(t, 1, offset)
	offset, err = stream.WriteBatch(ctx, 0, first)
	require.NoError(t, err)
	require.EqualValues(t, 1, offset)
	_, err = stream.WriteBatch(ctx, 0, second)
	serviceError(t, err, http.StatusConflict, "RowOffsetConflict")
	_, err = stream.WriteBatch(ctx, 3, second)
	serviceError(t, err, http.StatusConflict, "RowOffsetConflict")

	before, err := e.storage.CreateTableReadSession(ctx, table, storage.TableReadOptions{})
	require.NoError(t, err)
	require.Zero(t, before.RecordCount())
	empty, err := before.OpenReader(ctx, nil)
	require.NoError(t, err)
	require.False(t, empty.Next())
	require.NoError(t, empty.Err())
	empty.Release()

	resumed, err := e.storage.GetTableWriteSession(ctx, table, upload.Session())
	require.NoError(t, err)
	writer, err := resumed.GetStream(ctx, key)
	require.NoError(t, err)
	require.EqualValues(t, 1, writer.InitialOffset())
	offset, err = writer.WriteBatch(ctx, writer.InitialOffset(), second)
	require.NoError(t, err)
	require.EqualValues(t, 2, offset)
	require.NoError(t, writer.Close(ctx))
	_, err = writer.WriteBatch(ctx, offset, second)
	serviceError(t, err, http.StatusConflict, "InvalidState")
	require.NoError(t, resumed.Commit(ctx))
	require.NoError(t, resumed.Commit(ctx))
	serviceError(t, resumed.Abort(ctx), http.StatusConflict, "InvalidState")

	after, err := e.storage.CreateTableReadSession(ctx, table, storage.TableReadOptions{Columns: []string{"note"}})
	require.NoError(t, err)
	require.EqualValues(t, 2, after.RecordCount())
	require.Equal(t, "NORMAL", strings.ToUpper(after.Status()))
	snapshot, err := e.storage.GetTableReadSession(ctx, table, after.Session())
	require.NoError(t, err)
	read, err := snapshot.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer read.Release()
	read.Retain()
	read.Release()
	require.Equal(t, "note", read.Schema().Field(0).Name)
	var values []string
	for read.Next() {
		column := read.Record().Column(0).(*array.String)
		for i := 0; i < column.Len(); i++ {
			values = append(values, column.Value(i))
		}
	}
	require.NoError(t, read.Err())
	require.Equal(t, []string{"first", "second"}, values)

	tun := tunnel.NewTunnel(e.service, e.endpoint)
	records, err := tun.Preview(e.service.Table(table.Name), "", 1)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.EqualValues(t, first.Column(0).(*array.Int64).Value(0), records[0][0].(data.BigInt))
}

func TestEmulatorAbortAndRejectedReads(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	table := e.table(t, "id bigint, note string")
	session, err := e.storage.CreateTableWriteSession(ctx, table, storage.TableWriteOptions{})
	require.NoError(t, err)
	stream, err := session.CreateStream(ctx, storage.Stream{ID: "a", Version: 1})
	require.NoError(t, err)
	_, err = stream.WriteBatch(ctx, 0, testBatch(t, 1, "discard"))
	require.NoError(t, err)
	require.NoError(t, session.Abort(ctx))
	serviceError(t, session.Commit(ctx), http.StatusConflict, "InvalidState")

	preview, err := e.storage.PreviewTable(ctx, table, "", 1)
	require.NoError(t, err)
	require.False(t, preview.Next())
	require.NoError(t, preview.Err())
	preview.Release()
	_, err = e.storage.CreateTableReadSession(ctx, storage.Table{Name: "missing_go_storage"}, storage.TableReadOptions{})
	serviceError(t, err, http.StatusNotFound, "NoSuchTable")
	_, err = e.storage.GetTableReadSession(ctx, table, storage.Session{ID: "missing_session"})
	require.Error(t, err)
}

func (e emulator) fault(t *testing.T, table storage.Table, action string, effect map[string]any) func() {
	t.Helper()
	path := e.endpoint + "/__test/faults/" + table.Name + "-" + action
	send := func(method string, data []byte) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, path, bytes.NewReader(data))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		require.NoError(t, err)
		return resp
	}
	payload, err := json.Marshal(map[string]any{
		"match": map[string]string{
			"plane": "storage", "object": "write", "action": action,
			"project": "test_project", "table": table.Name,
		},
		"effect": effect, "times": 1, "ttl_seconds": 60,
	})
	require.NoError(t, err)
	resp := send(http.MethodPut, payload)
	defer resp.Body.Close()
	require.Less(t, resp.StatusCode, 300)
	t.Cleanup(func() { send(http.MethodDelete, nil).Body.Close() })
	return func() {
		t.Helper()
		resp := send(http.MethodGet, nil)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var state struct{ Hits int }
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&state))
		require.Equal(t, 1, state.Hits)
	}
}

func TestEmulatorWriteAndCommitLostAcknowledgement(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	table := e.createTable(t, "(id bigint, note string) tblproperties ('transactional'='true')")
	metadata := e.service.Table(table.Name)
	require.NoError(t, metadata.Load())
	require.True(t, metadata.Transactional())
	upload, err := e.storage.CreateTableWriteSession(ctx, table, storage.TableWriteOptions{})
	require.NoError(t, err)
	throttled := e.fault(t, table, "create_stream", map[string]any{"type": "http_error", "status": 429})
	key := storage.Stream{ID: "retry", Version: 3}
	_, err = upload.CreateStream(ctx, key)
	serviceError(t, err, http.StatusTooManyRequests, "FlowExceeded")
	throttled()
	stream, err := upload.CreateStream(ctx, key)
	require.NoError(t, err)

	writeLost := e.fault(t, table, "write", map[string]any{"type": "lost_ack"})
	batch := testBatch(t, 7, "only once")
	_, err = stream.WriteBatch(ctx, 0, batch)
	require.Error(t, err)
	writeLost()
	offset, err := stream.WriteBatch(ctx, 0, batch)
	require.NoError(t, err)
	require.EqualValues(t, 1, offset)
	serviceError(t, upload.CommitStreams(ctx, key), http.StatusConflict, "InvalidState")
	require.NoError(t, stream.Close(ctx))

	discarded, err := upload.CreateStream(ctx, storage.Stream{ID: "discarded", Version: 1})
	require.NoError(t, err)
	_, err = discarded.WriteBatch(ctx, 0, testBatch(t, 99, "exclude this stream"))
	require.NoError(t, err)
	require.NoError(t, discarded.Close(ctx))

	coordinator, err := e.storage.GetTableWriteSession(ctx, table, upload.Session())
	require.NoError(t, err)
	serviceError(t, coordinator.CommitStreams(ctx, storage.Stream{ID: key.ID, Version: 2}), http.StatusConflict, "InvalidState")
	commitLost := e.fault(t, table, "commit", map[string]any{"type": "lost_ack"})
	require.Error(t, coordinator.CommitStreams(ctx, key))
	commitLost()
	reopened, err := e.storage.GetTableWriteSession(ctx, table, upload.Session())
	require.NoError(t, err)
	require.Equal(t, "COMMITTED", strings.ToUpper(reopened.Status()))
	require.NoError(t, reopened.CommitStreams(ctx, key))

	snapshot, err := e.storage.CreateTableReadSession(ctx, table, storage.TableReadOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 1, snapshot.RecordCount())
	reader, err := snapshot.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next(), "%v", reader.Err())
	require.EqualValues(t, 7, reader.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.Equal(t, "only once", reader.RecordBatch().Column(1).(*array.String).Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func TestEmulatorPartitionOverwrite(t *testing.T) {
	e := newEmulator(t)
	ctx := t.Context()
	table := e.createTable(t, "(id bigint, note string) partitioned by (ds string)")
	for _, day := range []string{"a", "b"} {
		e.sql(t, fmt.Sprintf("alter table %s add partition (ds='%s')", table.Name, day))
	}

	write := func(partition string, id int64, overwrite bool) {
		t.Helper()
		session, err := e.storage.CreateTableWriteSession(ctx, table, storage.TableWriteOptions{
			Partition: partition,
			Overwrite: overwrite,
		})
		require.NoError(t, err)
		stream, err := session.CreateStream(ctx, storage.Stream{ID: "writer"})
		require.NoError(t, err)
		_, err = stream.WriteBatch(ctx, 0, testBatch(t, id, partition))
		require.NoError(t, err)
		require.NoError(t, stream.Close(ctx))
		require.NoError(t, session.Commit(ctx))
	}
	write("ds=a", 1, false)
	write("ds=b", 2, false)
	write("ds=a", 3, true)

	preview, err := e.storage.PreviewTable(ctx, table, "ds=a", 1)
	require.NoError(t, err)
	defer preview.Release()
	require.True(t, preview.Next(), "%v", preview.Err())
	require.EqualValues(t, 3, preview.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.False(t, preview.Next())
	require.NoError(t, preview.Err())

	for _, tc := range []struct {
		partition string
		id        int64
	}{{"ds=a", 3}, {"ds=b", 2}} {
		snapshot, err := e.storage.CreateTableReadSession(ctx, table, storage.TableReadOptions{
			Columns:    []string{"id"},
			Partitions: []string{tc.partition},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, snapshot.RecordCount())
		reader, err := snapshot.OpenReader(ctx, nil)
		require.NoError(t, err)
		defer reader.Release()
		require.True(t, reader.Next(), "%v", reader.Err())
		require.EqualValues(t, tc.id, reader.RecordBatch().Column(0).(*array.Int64).Value(0))
		require.False(t, reader.Next())
		require.NoError(t, reader.Err())
	}

	oldAPI := tunnel.NewTunnel(e.service, e.endpoint)
	rows, err := oldAPI.Preview(e.service.Table(table.Name), "ds=a", 1)
	require.NoError(t, err)
	require.Equal(t, []data.Record{{data.BigInt(3), data.String("ds=a"), data.String("a")}}, rows)

	missing := storage.Table{Name: table.Name + "_missing"}
	_, err = e.storage.PreviewTable(ctx, missing, "", 1)
	serviceError(t, err, http.StatusNotFound, "NoSuchTable")
	_, err = e.storage.CreateTableWriteSession(ctx, missing, storage.TableWriteOptions{})
	serviceError(t, err, http.StatusNotFound, "NoSuchTable")
}
