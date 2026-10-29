package tunnel

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestRecordProtocReaderTruncatedChecksum(t *testing.T) {
	var prefix bytes.Buffer
	writer := NewProtocStreamWriter(&prefix)
	if err := writer.WriteTag(MetaCount, protowire.VarintType); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteSInt64(0); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteTag(MetaChecksum, protowire.VarintType); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		suffix  []byte
		wantErr bool
	}{
		{"missing", nil, true},
		{"partial_varint", []byte{0x80}, true},
		{"complete_empty_stream", []byte{0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := append(append([]byte(nil), prefix.Bytes()...), tc.suffix...)
			reader := newRecordProtocReader(&http.Response{Body: ioutil.NopCloser(bytes.NewReader(payload))}, nil, false)
			err := reader.Iterator(func(_ data.Record, _ error) {})
			if tc.wantErr && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected unexpected EOF, got %v", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("valid stream: %v", err)
			}
		})
	}
}
