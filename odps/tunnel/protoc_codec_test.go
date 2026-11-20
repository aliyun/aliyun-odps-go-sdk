// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func protoValues(t *testing.T) []data.Data {
	t.Helper()
	char, err := data.NewChar(8, "茶")
	require.NoError(t, err)
	varchar, err := data.NewVarChar(16, "café")
	require.NoError(t, err)
	list := data.NewArrayWithType(datatype.NewArrayType(datatype.BigIntType))
	list.UnSafeAppend(data.BigInt(1), nil, data.BigInt(-2))
	mapping := data.NewMapWithType(datatype.NewMapType(datatype.StringType, datatype.BigIntType))
	require.NoError(t, mapping.Set("one", data.BigInt(1)))
	require.NoError(t, mapping.Set("null", nil))
	structure := data.NewStructWithTyp(datatype.NewStructType(
		datatype.StructFieldType{Name: "items", Type: list.Type()},
		datatype.StructFieldType{Name: "extra", Type: datatype.StringType},
	))
	require.NoError(t, structure.SetField("items", list))
	require.NoError(t, structure.SetField("extra", nil))
	timestamp := time.Unix(-321, 654321000).UTC()
	return []data.Data{
		data.TinyInt(-2), data.SmallInt(-1000), data.Int(-32768), data.BigInt(-100000000000),
		data.Bool(true), data.Float(3.25), data.Double(3.5),
		data.String("你好"), *char, *varchar, data.Binary{1, 0, 255}, data.Geography{1, 2, 3},
		data.Date(time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)), data.DateTime(timestamp.Truncate(time.Millisecond)),
		data.Timestamp(timestamp), data.TimestampNtz(timestamp), data.IntervalYearMonth(-13),
		data.NewIntervalDayTime(-1, 123000000), data.NewDecimal(12, 4, "-0.0123"),
		&data.Json{Data: `{"valid":true}`, Valid: true}, list, mapping, structure,
	}
}

func TestProtobufCodecRoundTripAndTruncation(t *testing.T) {
	for _, value := range protoValues(t) {
		t.Run(value.Type().Name(), func(t *testing.T) {
			columns := []tableschema.Column{{Name: "value", Type: value.Type()}}
			var buf bytes.Buffer
			writer := newRecordProtocWriter(&bufWriter{&buf}, columns, false)
			require.NoError(t, writer.Write(data.Record{value}))
			require.NoError(t, writer.Close())
			payload := buf.Bytes()

			reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(payload))}, columns, false)
			row, err := reader.Read()
			require.NoError(t, err)
			switch expected := value.(type) {
			case interface{ Time() time.Time }:
				require.True(t, expected.Time().Equal(row[0].(interface{ Time() time.Time }).Time()))
			case *data.Map:
				require.Equal(t, expected.ToGoMap(), row[0].(*data.Map).ToGoMap())
			default:
				require.Equal(t, value.Sql(), row[0].Sql())
			}
			_, err = reader.Read()
			require.ErrorIs(t, err, io.EOF)
			_, err = reader.Read()
			require.ErrorIs(t, err, io.EOF)

			for n := range len(payload) {
				truncated := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(payload[:n]))}, columns, false)
				err := truncated.Iterator(func(data.Record, error) {})
				require.Error(t, err, "truncated at byte %d of %d", n, len(payload))
			}
		})
	}
}

type failingProtobufSink struct {
	failAt int
	writes int
	closed bool
	err    error
}

func (s *failingProtobufSink) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == s.failAt {
		return 0, s.err
	}
	return len(p), nil
}

func (s *failingProtobufSink) Close() error {
	s.closed = true
	return nil
}

func TestProtobufWriterReturnsSinkErrorsAndCloses(t *testing.T) {
	columns := make([]tableschema.Column, 0)
	row := data.Record(protoValues(t))
	for _, value := range row {
		columns = append(columns, tableschema.Column{Type: value.Type()})
	}
	healthy := new(failingProtobufSink)
	writer := newRecordProtocWriter(healthy, columns, false)
	require.NoError(t, writer.Write(row))
	require.NoError(t, writer.Close())
	require.True(t, healthy.closed)

	writeErr := errors.New("sink rejected bytes")
	for i := 1; i <= healthy.writes; i++ {
		sink := &failingProtobufSink{failAt: i, err: writeErr}
		writer := newRecordProtocWriter(sink, columns, false)
		err := writer.Write(row)
		closeErr := writer.Close()
		require.True(t, errors.Is(err, writeErr) || errors.Is(closeErr, writeErr), "write %d of %d", i, healthy.writes)
		require.True(t, sink.closed, "sink stayed open after write %d", i)
	}
	require.ErrorContains(t, writer.Close(), "closed")
}
