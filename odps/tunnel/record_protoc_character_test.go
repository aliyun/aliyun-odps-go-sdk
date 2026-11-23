// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// characterRecordFixture encodes one record and its trailer without using the
// SDK's writer or character constructors. Pass nil to omit the column (NULL).
func characterRecordFixture(field, crcInput []byte) []byte {
	var wire []byte
	if field != nil {
		wire = append(protowire.AppendTag(nil, 1, protowire.BytesType), field...)
		crcInput = append(binary.LittleEndian.AppendUint32(nil, 1), crcInput...)
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	recordCRC := crc32.Checksum(crcInput, table)
	streamCRC := crc32.Checksum(binary.LittleEndian.AppendUint32(nil, recordCRC), table)
	wire = protowire.AppendVarint(protowire.AppendTag(wire, EndRecord, protowire.VarintType), uint64(recordCRC))
	wire = protowire.AppendVarint(protowire.AppendTag(wire, MetaCount, protowire.VarintType), protowire.EncodeZigZag(1))
	return protowire.AppendVarint(protowire.AppendTag(wire, MetaChecksum, protowire.VarintType), uint64(streamCRC))
}

func TestProtobufCharacterWireValues(t *testing.T) {
	for _, typ := range []datatype.DataType{datatype.NewCharType(5), datatype.NewVarcharType(5)} {
		t.Run(typ.Name(), func(t *testing.T) {
			columns := []tableschema.Column{{Name: "value", Type: typ}}
			for _, test := range []struct {
				name, value string
				null        bool
				overflow    bool
			}{
				{name: "ASCII", value: "hello"},
				{name: "Arabic", value: "مرحبا"},
				{name: "CJK", value: "中文测试啊"},
				{name: "supplementary", value: "😀中文ab"},
				{name: "combining", value: "e\u0301abc"},
				{name: "empty"},
				{name: "null", null: true},
				{name: "overflow", value: "😀中文abc", overflow: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					var field []byte
					if !test.null {
						field = protowire.AppendString(nil, test.value)
					}
					wire := characterRecordFixture(field, []byte(test.value))
					reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(wire))}, columns, false)
					row, err := reader.Read()
					if test.overflow {
						var overflow data.CharOverflowError
						require.ErrorAs(t, err, &overflow)
						require.Nil(t, row)
						return
					}
					require.NoError(t, err)
					if test.null {
						require.Equal(t, data.Null, row[0])
					} else {
						require.Equal(t, typ, row[0].Type())
						require.Equal(t, test.value, row[0].String())
					}
					_, err = reader.Read()
					require.ErrorIs(t, err, io.EOF)
				})
			}

			const value = "😀中文ab"
			var typed data.Data
			var err error
			if typ.ID() == datatype.CHAR {
				typed, err = data.NewChar(5, value)
			} else {
				typed, err = data.NewVarChar(5, value)
			}
			require.NoError(t, err)
			var output bytes.Buffer
			writer := newRecordProtocWriter(&bufWriter{&output}, columns, false)
			require.NoError(t, writer.Write(data.Record{typed}))
			require.NoError(t, writer.Close())
			want := characterRecordFixture(protowire.AppendString(nil, value), []byte(value))
			require.Equal(t, want, output.Bytes())
		})
	}
}

func TestProtobufCharacterMapPreservesDistinctKeys(t *testing.T) {
	for _, typ := range []datatype.DataType{datatype.NewCharType(5), datatype.NewVarcharType(5)} {
		t.Run(typ.Name(), func(t *testing.T) {
			columns := []tableschema.Column{{Name: "value", Type: datatype.NewMapType(typ, datatype.StringType)}}
			for _, key := range []string{"中文测试啊", "中文测试啊好"} {
				var field, checksum []byte
				// MAP is two arrays. Each starts with its size, followed by each
				// entry's null flag and value. CRC accumulates the raw values.
				for _, array := range [][]string{{"مرحبا", key}, {"arabic", "cjk"}} {
					field = protowire.AppendVarint(field, uint64(len(array)))
					for _, value := range array {
						field = protowire.AppendString(append(field, 0), value)
						checksum = append(checksum, value...)
					}
				}
				wire := characterRecordFixture(field, checksum)
				reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(wire))}, columns, false)
				row, err := reader.Read()
				if key == "中文测试啊好" {
					var overflow data.CharOverflowError
					require.ErrorAs(t, err, &overflow)
					require.Nil(t, row)
					continue
				}
				require.NoError(t, err)
				entries := make(map[string]string)
				for k, value := range row[0].(*data.Map).ToGoMap() {
					require.Equal(t, typ, k.Type())
					entries[k.String()] = value.String()
				}
				require.Equal(t, map[string]string{"مرحبا": "arabic", key: "cjk"}, entries)
				_, err = reader.Read()
				require.ErrorIs(t, err, io.EOF)
			}
		})
	}
}
