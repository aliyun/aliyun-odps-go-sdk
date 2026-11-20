// Licensed under the Apache License, Version 2.0.

package tunnel_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

const emulatorProject = "test_project"

type tunnelFixture struct {
	endpoint string
	service  *odps.Odps
	tunnel   *tunnel.Tunnel
}

func newTunnelFixture(t *testing.T) tunnelFixture {
	t.Helper()
	endpoint := os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("MAXCOMPUTE_EMULATOR_ENDPOINT is unset")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback(), "emulator must run on loopback")
	service := odps.NewOdps(account2.NewAliyunAccount("test-ak", "test-sk"), endpoint+"/api")
	service.SetDefaultProjectName(emulatorProject)
	service.SetCurrentSchemaName("default")
	service.SetHttpTimeout(5 * time.Second)
	transfer, err := tunnel.NewTunnelFromProject(service.DefaultProject())
	require.NoError(t, err)
	require.Equal(t, endpoint, transfer.GetEndpoint())
	transfer.SetHttpTimeout(5 * time.Second)
	transfer.SetTcpConnectionTimeout(2 * time.Second)
	transfer.SetOdpsUserAgent("go-sdk-emulator-test")
	transfer.SetQuotaName("default")
	return tunnelFixture{endpoint, service, transfer}
}

func (e tunnelFixture) sql(t *testing.T, sql string) *odps.Instance {
	t.Helper()
	instance, err := e.service.ExecSQl(sql)
	require.NoError(t, err, "%s", sql)
	require.NoError(t, instance.WaitForSuccessContext(t.Context()), "%s", sql)
	return instance
}

func (e tunnelFixture) table(t *testing.T, schema string, partitioned bool) string {
	t.Helper()
	name := fmt.Sprintf("go_proto_%d", time.Now().UnixNano())
	tables := odps.NewTables(e.service, emulatorProject, schema)
	builder := tableschema.NewSchemaBuilder().Name(name).Columns(
		tableschema.Column{Name: "id", Type: datatype.BigIntType},
		tableschema.Column{Name: "note", Type: datatype.StringType},
	)
	if partitioned {
		builder.PartitionColumn(tableschema.Column{Name: "ds", Type: datatype.StringType})
	}
	require.NoError(t, tables.Create(builder.Build(), false, nil, nil))
	t.Cleanup(func() { require.NoError(t, tables.Delete(name, true)) })
	return name
}

func (e tunnelFixture) fault(t *testing.T, match, effect map[string]any) func() {
	t.Helper()
	path := fmt.Sprintf("%s/__test/faults/go-proto-%d", e.endpoint, time.Now().UnixNano())
	client := &http.Client{
		Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	request := func(method string, body io.Reader) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, path, body)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		require.NoError(t, err)
		if resp.StatusCode >= 300 {
			message, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			t.Fatalf("emulator fault %s: %s: %s", method, resp.Status, message)
		}
		return resp
	}
	payload, err := json.Marshal(map[string]any{"match": match, "effect": effect, "times": 1, "ttl_seconds": 60})
	require.NoError(t, err)
	require.NoError(t, request(http.MethodPut, bytes.NewReader(payload)).Body.Close())
	t.Cleanup(func() { require.NoError(t, request(http.MethodDelete, nil).Body.Close()) })
	return func() {
		t.Helper()
		resp := request(http.MethodGet, nil)
		defer resp.Body.Close()
		var rule struct{ Hits int }
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&rule))
		require.Equal(t, 1, rule.Hits)
	}
}

func requireTunnelError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var got restclient.HttpError
	require.ErrorAs(t, err, &got)
	require.Equal(t, status, got.StatusCode)
	require.NotNil(t, got.ErrorMessage)
	require.Equal(t, code, got.ErrorMessage.ErrorCode)
}

func putProtobufBlock(t *testing.T, upload *tunnel.UploadSession, block int, records ...data.Record) error {
	t.Helper()
	writer, err := upload.OpenRecordWriter(block)
	require.NoError(t, err)
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			_ = writer.Close()
			return err
		}
	}
	return writer.Close()
}

func readProtobufRows(t *testing.T, reader *tunnel.RecordProtocReader) ([]data.Record, error) {
	t.Helper()
	defer func() { require.NoError(t, reader.Close()) }()
	var rows []data.Record
	err := reader.Iterator(func(row data.Record, err error) {
		if err == nil {
			rows = append(rows, row)
		}
	})
	return rows, err
}

func TestEmulatorProtobufBlocks(t *testing.T) {
	testProtobufBlocks(t, newTunnelFixture(t), "default")
}

func TestEmulatorProtobufBlocksInNamedSchema(t *testing.T) {
	e := newTunnelFixture(t)
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(e.endpoint + "/capabilities")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var capabilities struct{ Schemas []string }
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&capabilities))
	if len(capabilities.Schemas) == 0 {
		t.Skip("emulator image does not expose schema creation")
	}
	schema := fmt.Sprintf("go_tunnel_%d", time.Now().UnixNano())
	schemas := e.service.DefaultProject().Schemas()
	require.NoError(t, schemas.Create(schema, false, "protobuf integration"))
	t.Cleanup(func() { require.NoError(t, schemas.Delete(schema)) })
	testProtobufBlocks(t, e, schema)
}

func testProtobufBlocks(t *testing.T, e tunnelFixture, schema string) {
	t.Helper()
	table := e.table(t, schema, true)
	options := []tunnel.Option{
		tunnel.SessionCfg.WithSchemaName(schema),
		tunnel.SessionCfg.WithPartitionKey("ds='today'"),
	}

	_, err := e.tunnel.CreateUploadSession(emulatorProject, table, tunnel.SessionCfg.WithSchemaName(schema))
	requireTunnelError(t, err, http.StatusBadRequest, "InvalidPartition")
	upload, err := e.tunnel.CreateUploadSession(emulatorProject, table,
		append(options, tunnel.SessionCfg.WithCreatePartition(), tunnel.SessionCfg.WithDefaultDeflateCompressor())...)
	require.NoError(t, err)
	require.NotEmpty(t, upload.Id)
	require.Equal(t, tunnel.UploadStatusNormal, upload.Status())
	require.Equal(t, "ds=today", upload.PartitionKey())
	require.Len(t, upload.Schema().Columns, 2)

	first := data.Record{data.BigInt(7), data.String("first")}
	second := data.Record{data.BigInt(9), data.String("second")}
	require.NoError(t, putProtobufBlock(t, upload, 4, first))
	require.ErrorContains(t, upload.Commit(nil), "blocks server got")
	require.ErrorContains(t, upload.Commit([]int{5}), "blocks server got")

	resumed, err := e.tunnel.AttachToExistedUploadSession(emulatorProject, table, upload.Id,
		append(options, tunnel.SessionCfg.WithDefaultZstdCompressor())...)
	require.NoError(t, err)
	require.Equal(t, schema, resumed.SchemaName)
	require.Equal(t, []int{4}, resumed.BlockIds())
	require.NoError(t, putProtobufBlock(t, resumed, 5, second))
	require.ErrorContains(t, resumed.Commit([]int{4, 4}), "blocks server got")
	require.NoError(t, resumed.Commit([]int{5, 4}))
	require.NoError(t, resumed.Load())
	require.Equal(t, tunnel.UploadStatusClosed, resumed.Status())
	requireTunnelError(t, putProtobufBlock(t, resumed, 6, first), http.StatusConflict, "InvalidState")

	snapshot, err := e.tunnel.CreateDownloadSession(emulatorProject, table,
		append(options, tunnel.SessionCfg.Async(), tunnel.SessionCfg.WithDefaultDeflateCompressor())...)
	require.NoError(t, err)
	require.Equal(t, tunnel.DownloadStatusNormal, snapshot.Status())
	require.Equal(t, 2, snapshot.RecordCount())
	snapshot.SetPartitionKey(`ds="today"`)
	require.Equal(t, "ds=today", snapshot.PartitionKey())
	_, err = snapshot.OpenRecordReader(0, 1, []string{"missing"})
	require.ErrorContains(t, err, "missing")
	_, err = snapshot.OpenRecordReader(-1, 1, nil)
	requireTunnelError(t, err, http.StatusBadRequest, "InvalidParameter")

	identity, err := e.tunnel.AttachToExistedDownloadSession(emulatorProject, table, snapshot.Id, options...)
	require.NoError(t, err)
	require.Equal(t, snapshot.Id, identity.Id)
	require.Equal(t, snapshot.Schema(), identity.Schema())
	reader, err := identity.OpenRecordReader(0, 2, nil)
	require.NoError(t, err)
	rows, err := readProtobufRows(t, reader)
	require.NoError(t, err)
	require.Equal(t, []data.Record{first, second}, rows)
	reader, err = snapshot.OpenRecordReader(1, 1, []string{"note"})
	require.NoError(t, err)
	require.Equal(t, tunnel.DeflateName, reader.HttpRes().Header.Get("Content-Encoding"))
	rows, err = readProtobufRows(t, reader)
	require.NoError(t, err)
	require.Equal(t, []data.Record{{data.String("second")}}, rows)

	_, err = e.tunnel.AttachToExistedUploadSession(emulatorProject, table, "missing", options...)
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchUpload")
	_, err = e.tunnel.AttachToExistedDownloadSession(emulatorProject, table, "missing", options...)
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchDownload")
}

func TestEmulatorProtobufUploadLostAcknowledgements(t *testing.T) {
	e := newTunnelFixture(t)
	name := e.table(t, "", false)
	match := func(action string) map[string]any {
		return map[string]any{
			"plane": "tunnel", "object": "upload", "action": action, "project": emulatorProject, "table": name,
		}
	}
	createWasThrottled := e.fault(t, match("create"), map[string]any{"type": "http_error", "status": 429})
	_, err := e.tunnel.CreateUploadSession(emulatorProject, name)
	requireTunnelError(t, err, http.StatusTooManyRequests, "FlowExceeded")
	createWasThrottled()

	upload, err := e.tunnel.CreateUploadSession(emulatorProject, name)
	require.NoError(t, err)
	record := data.Record{data.BigInt(11), data.String("once")}
	writeWasCommitted := e.fault(t, match("write"), map[string]any{"type": "lost_ack"})
	require.Error(t, putProtobufBlock(t, upload, 3, record))
	writeWasCommitted()
	require.NoError(t, upload.Load())
	require.Equal(t, []int{3}, upload.BlockIds())
	require.NoError(t, putProtobufBlock(t, upload, 3, record))

	beforeCommit, err := e.tunnel.CreateDownloadSession(emulatorProject, name)
	require.NoError(t, err)
	require.Zero(t, beforeCommit.RecordCount())
	commitWasApplied := e.fault(t, match("commit"), map[string]any{"type": "lost_ack"})
	require.NoError(t, upload.Commit([]int{3}))
	commitWasApplied()
	require.NoError(t, upload.Commit([]int{3}))

	afterCommit, err := e.tunnel.CreateDownloadSession(emulatorProject, name, tunnel.SessionCfg.WithDefaultZstdCompressor())
	require.NoError(t, err)
	require.Equal(t, 1, afterCommit.RecordCount())
	reader, err := afterCommit.OpenRecordReader(0, 1, nil)
	require.NoError(t, err)
	require.Equal(t, tunnel.ZstdName, reader.HttpRes().Header.Get("Content-Encoding"))
	rows, err := readProtobufRows(t, reader)
	require.NoError(t, err)
	require.Equal(t, []data.Record{record}, rows)
}

func TestEmulatorProtobufInstanceDownload(t *testing.T) {
	e := newTunnelFixture(t)
	instance := e.sql(t, "select cast(7 as bigint) as id union all select cast(9 as bigint) as id order by id")
	opts := []tunnel.InstanceOption{
		tunnel.InstanceSessionCfg.EnableLimit(),
		tunnel.InstanceSessionCfg.WithTaskName(instance.TaskNameCommitted()),
		tunnel.InstanceSessionCfg.WithDefaultDeflateCompressor(),
	}
	session, err := e.tunnel.CreateInstanceResultDownloadSession(emulatorProject, instance.Id(), opts...)
	require.NoError(t, err)
	require.Equal(t, 2, session.RecordCount())
	require.Equal(t, tunnel.DownloadStatusNormal, session.Status())
	require.Equal(t, "id", session.Schema().Columns[0].Name)
	_, err = session.OpenRecordReader(0, 1, 0, []string{"invalid"})
	require.ErrorContains(t, err, "invalid")

	client := restclient.NewOdpsRestClient(e.service.Account(), e.endpoint)
	resumed, err := tunnel.AttachToExistedIRDownloadSession(session.Id, emulatorProject, instance.Id(), client, opts...)
	require.NoError(t, err)
	require.Equal(t, session.Id, resumed.Id)
	reader, err := resumed.OpenRecordReader(1, 1, 0, nil)
	require.NoError(t, err)
	rows, err := readProtobufRows(t, reader)
	require.NoError(t, err)
	require.Equal(t, []data.Record{{data.BigInt(9)}}, rows)

	_, err = resumed.OpenRecordReader(0, -1, 0, nil)
	requireTunnelError(t, err, http.StatusBadRequest, "InvalidParameter")
	_, err = tunnel.AttachToExistedIRDownloadSession("missing", emulatorProject, instance.Id(), client)
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchDownload")
	_, err = e.tunnel.CreateInstanceResultDownloadSession(emulatorProject, "missing")
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchInstance")
}

func TestEmulatorProtobufReadFaults(t *testing.T) {
	e := newTunnelFixture(t)
	table := e.table(t, "", false)
	e.sql(t, fmt.Sprintf("insert into %s values (1, 'one'), (2, 'two'), (3, 'three')", table))

	for _, tc := range []struct {
		name       string
		effect     map[string]any
		wantPrefix int
		cause      error
		message    string
		compress   bool
	}{
		{name: "early EOF", effect: map[string]any{"type": "early_eof", "rows": 1}, wantPrefix: 1, cause: io.ErrUnexpectedEOF},
		{name: "disconnect", effect: map[string]any{"type": "disconnect_after_bytes", "bytes": 1}, cause: io.ErrUnexpectedEOF},
		{name: "truncated deflate header", effect: map[string]any{"type": "disconnect_after_bytes", "bytes": 1}, cause: io.ErrUnexpectedEOF, compress: true},
		{name: "checksum", effect: map[string]any{"type": "crc_mismatch"}, wantPrefix: 3, message: "checksum is invalid"},
		{name: "malformed protobuf", effect: map[string]any{"type": "malformed_protobuf"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []tunnel.Option
			if tc.compress {
				opts = append(opts, tunnel.SessionCfg.WithDefaultDeflateCompressor())
			}
			snapshot, err := e.tunnel.CreateDownloadSession(emulatorProject, table, opts...)
			require.NoError(t, err)
			wasApplied := e.fault(t, map[string]any{
				"action": "read", "project": emulatorProject, "table": table, "format": "protobuf",
			}, tc.effect)
			reader, err := snapshot.OpenRecordReader(0, 3, nil)
			require.NoError(t, err)
			prefix, err := readProtobufRows(t, reader)
			require.Error(t, err)
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
			}
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
			}
			require.Len(t, prefix, tc.wantPrefix)
			wasApplied()

			reader, err = snapshot.OpenRecordReader(len(prefix), 3-len(prefix), nil)
			require.NoError(t, err)
			suffix, err := readProtobufRows(t, reader)
			require.NoError(t, err)
			require.Equal(t, []data.Record{
				{data.BigInt(1), data.String("one")},
				{data.BigInt(2), data.String("two")},
				{data.BigInt(3), data.String("three")},
			}, append(prefix, suffix...))
		})
	}

	snapshot, err := e.tunnel.CreateDownloadSession(emulatorProject, table)
	require.NoError(t, err)
	expired := e.fault(t, map[string]any{
		"action": "read", "project": emulatorProject, "table": table, "format": "protobuf",
	}, map[string]any{"type": "expire_session"})
	_, err = snapshot.OpenRecordReader(0, 1, nil)
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchDownload")
	expired()
	_, err = e.tunnel.AttachToExistedDownloadSession(emulatorProject, table, snapshot.Id)
	requireTunnelError(t, err, http.StatusNotFound, "NoSuchDownload")
}

func TestEmulatorProtobufStreamRetryAndReset(t *testing.T) {
	e := newTunnelFixture(t)
	table := e.table(t, "", false)
	stream, err := e.tunnel.CreateStreamUploadSession(emulatorProject, table,
		tunnel.SessionCfg.WithSlotNum(1),
		tunnel.SessionCfg.WithAllowSchemaMismatch(false),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stream.SchemaVersion(), 0)
	require.Len(t, stream.Schema().Columns, 2)

	writer := stream.OpenRecordPackWriter()
	_, count, size, err := writer.Flush()
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, size)
	require.ErrorContains(t, writer.Append(data.Record{data.BigInt(1)}), "schema not match")
	_, _, _, err = writer.Flush()
	require.ErrorContains(t, err, "unsuccessful append")
	writer.Reset()

	first := data.Record{data.BigInt(1), data.String("retry")}
	second := data.Record{data.BigInt(2), data.String("reset")}
	rejected := func() func() {
		return e.fault(t, map[string]any{
			"plane": "tunnel", "object": "stream", "action": "write", "project": emulatorProject, "table": table,
		}, map[string]any{"type": "http_error", "status": 429})
	}

	require.NoError(t, writer.Append(first))
	require.Positive(t, writer.DataSize())
	firstWasRejected := rejected()
	_, _, _, err = writer.Flush(3 * time.Second)
	requireTunnelError(t, err, http.StatusTooManyRequests, "FlowExceeded")
	firstWasRejected()
	require.EqualValues(t, 1, writer.RecordCount())
	require.ErrorContains(t, writer.Append(second), "unsuccessful flush")
	requestID, count, size, err := writer.Flush()
	require.NoError(t, err)
	require.NotEmpty(t, requestID)
	require.EqualValues(t, 1, count)
	require.Positive(t, size)
	require.Zero(t, writer.RecordCount())

	require.NoError(t, writer.Append(data.Record{data.BigInt(-1), data.String("drop")}))
	secondWasRejected := rejected()
	_, _, _, err = writer.Flush()
	requireTunnelError(t, err, http.StatusTooManyRequests, "FlowExceeded")
	secondWasRejected()
	writer.Reset()
	require.NoError(t, writer.Append(second))
	_, count, _, err = writer.Flush()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	third := data.Record{data.BigInt(3), data.String("lost acknowledgement")}
	require.NoError(t, writer.Append(third))
	ackWasLost := e.fault(t, map[string]any{
		"plane": "tunnel", "object": "stream", "action": "write", "project": emulatorProject, "table": table,
	}, map[string]any{"type": "lost_ack"})
	_, _, _, err = writer.Flush()
	require.Error(t, err)
	ackWasLost()
	_, count, _, err = writer.Flush()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	download, err := e.tunnel.CreateDownloadSession(emulatorProject, table)
	require.NoError(t, err)
	require.Equal(t, 3, download.RecordCount())
	reader, err := download.OpenRecordReader(0, 3, nil)
	require.NoError(t, err)
	rows, err := readProtobufRows(t, reader)
	require.NoError(t, err)
	require.Equal(t, []data.Record{first, second, third}, rows)
}
