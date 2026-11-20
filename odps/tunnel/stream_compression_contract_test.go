// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

type closeCompressionWriter struct {
	io.Writer
	err error
}

func (w closeCompressionWriter) Close() error { return w.err }

type failCompressionClose struct {
	err error
}

func (c failCompressionClose) Name() string                            { return "identity" }
func (c failCompressionClose) NewReader(r io.ReadCloser) io.ReadCloser { return r }
func (c failCompressionClose) NewWriter(w io.WriteCloser) io.WriteCloser {
	return closeCompressionWriter{Writer: w, err: c.err}
}

func TestStreamFlushClosesPipeWhenCompressorCloseFails(t *testing.T) {
	compressorErr := errors.New("failed to finish compressed stream")
	for _, tc := range []struct {
		name   string
		status int
	}{{"local error", http.StatusOK}, {"server rejection", http.StatusBadGateway}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Equal(t, "record pack", string(payload))
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(server.Close)
			client := restclient.NewOdpsRestClient(account.NewAliyunAccount("fixture", "secret"), server.URL)
			client.HttpTimeout = time.Second
			t.Cleanup(client.CloseIdleConnections)
			session := &StreamUploadSession{
				RestClient:   client,
				Compressor:   failCompressionClose{err: compressorErr},
				slotSelector: newSlotSelect(newTestSlots(t, "127.0.0.1:80")),
			}
			writer := session.OpenRecordPackWriter()
			writer.buffer.WriteString("record pack")
			_, _, err := session.flushStream(writer, 0)
			if tc.status != http.StatusOK {
				var response restclient.HttpError
				require.ErrorAs(t, err, &response)
				require.Equal(t, tc.status, response.StatusCode)
			} else {
				require.ErrorIs(t, err, compressorErr)
			}
		})
	}
}

type bufferedWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (w *bufferedWriteCloser) Close() error {
	w.closed = true
	return nil
}

func TestDeflateCompressesAtRequestedLevel(t *testing.T) {
	raw := bytes.Repeat([]byte("protocol buffer records with repeated field contents"), 200)
	compress := func(level int) int {
		t.Helper()
		var compressed bufferedWriteCloser
		w := newDeflate(level).NewWriter(&compressed)
		_, err := w.Write(raw)
		require.NoError(t, err)
		require.NoError(t, w.Close())
		size := compressed.Len()
		r, err := zlib.NewReader(&compressed)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, raw, got)
		return size
	}
	require.Greater(t, compress(flate.NoCompression), compress(flate.BestCompression)*4)
	require.Positive(t, compress(100))
}

func TestCompressionClosesDestinationAfterEncoderFailure(t *testing.T) {
	cause := errors.New("encoder flush failed")
	destination := &bufferedWriteCloser{}
	writer := writeCloser{writeCloser: closeCompressionWriter{Writer: destination, err: cause}, closer: destination}
	require.ErrorIs(t, writer.Close(), cause)
	require.True(t, destination.closed)
}
