// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/storage"
)

type trackedTransport struct {
	*http.Transport
	requests atomic.Int32
	closes   atomic.Int32
}

func (tr *trackedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.requests.Add(1)
	request := req.Clone(req.Context())
	request.Header.Set("X-Transport-Test", "custom")
	return tr.Transport.RoundTrip(request)
}

func (tr *trackedTransport) CloseIdleConnections() {
	tr.closes.Add(1)
	tr.Transport.CloseIdleConnections()
}

func TestCustomTransportIsUsedForDiscoveryAndData(t *testing.T) {
	wire := characterRecordFixture(protowire.AppendString(nil, "hello"), []byte("hello"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "custom", r.Header.Get("X-Transport-Test"))
		assert.Contains(t, r.Header.Get("Authorization"), "test-ak:")
		q := r.URL.Query()
		switch r.URL.Path {
		case "/api/projects/sample/tunnel":
			fmt.Fprint(w, r.Host+"/data")
		case "/data/projects/sample/instances/instance":
			if r.Method == http.MethodPost {
				assert.True(t, q.Has("downloads"))
				fmt.Fprint(w, `{"DownloadID":"session","Status":"NORMAL","RecordCount":1,"Schema":{"columns":[{"name":"message","type":"string"}]}}`)
				return
			}
			assert.True(t, q.Has("data"))
			assert.Equal(t, "session", q.Get("downloadid"))
			_, _ = w.Write(wire)
		case "/data/api/storage/v3":
			assert.Equal(t, "TableCreateReadSession", q.Get("Action"))
			fmt.Fprint(w, `{"SessionId":"snapshot","SessionStatus":"NORMAL","RecordCount":0}`)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	transport := &trackedTransport{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	t.Cleanup(transport.Transport.CloseIdleConnections)
	service := odps.NewOdpsWithHTTPTransport(account.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api", transport)
	service.SetDefaultProjectName("sample")
	ctx := t.Context()
	transfer := NewTunnel(service)
	session, err := transfer.CreateInstanceResultDownloadSessionContext(ctx, "sample", "instance")
	require.NoError(t, err)
	reader, err := session.OpenRecordReaderContext(ctx, 0, 1, 0, nil)
	require.NoError(t, err)
	defer reader.Close()
	row, err := reader.Read()
	require.NoError(t, err)
	require.Equal(t, "hello", row[0].String())
	_, err = reader.Read()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 3, transport.requests.Load())

	storageClient, err := storage.NewClient(ctx, service, storage.ClientOptions{TunnelEndpoint: transfer.GetEndpoint()})
	require.NoError(t, err)
	_, err = storageClient.CreateTableReadSession(ctx, storage.Table{Name: "fixture"}, storage.TableReadOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 4, transport.requests.Load())
	storageClient.Close()
	session.RestClient.CloseIdleConnections()
	require.Zero(t, transport.closes.Load())
}
