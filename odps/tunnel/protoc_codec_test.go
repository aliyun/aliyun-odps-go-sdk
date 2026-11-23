// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/big"
	"net/http"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

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
		data.String("你好"), *char, char, *varchar, data.Binary{1, 0, 255}, data.Geography{1, 2, 3},
		data.Date(time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)), data.DateTime(timestamp.Truncate(time.Millisecond)),
		data.Timestamp(timestamp), data.TimestampNtz(timestamp), data.IntervalYearMonth(-13),
		data.NewIntervalDayTime(-1, 123000000), data.NewDecimal(12, 4, "-0.0123"),
		*data.NewDecimalFromValue(12, 4, big.NewInt(-123)),
		&data.Json{Data: `{"valid":true}`, Valid: true}, data.Json{Data: `[1,2]`, Valid: true},
		list, *list, mapping, *mapping, structure, *structure,
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
			case data.Map:
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

func TestProtobufPayloadSizeAndVarintBoundaries(t *testing.T) {
	for _, size := range []int{0, 16, 8192} {
		want := bytes.Repeat([]byte{0x7f}, size)
		wire := protowire.AppendBytes(nil, want)
		wire = protowire.AppendVarint(wire, 19)
		reader := NewProtocStreamReader(bytes.NewReader(wire))
		got, err := reader.ReadBytes()
		require.NoError(t, err)
		require.Equal(t, want, got)
		next, err := reader.ReadVarint()
		require.NoError(t, err)
		require.EqualValues(t, 19, next)
	}

	truncated := protowire.AppendVarint(nil, 8192)
	truncated = append(truncated, 'a')
	_, err := NewProtocStreamReader(bytes.NewReader(truncated)).ReadBytes()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	transportErr := errors.New("download stream interrupted")
	failed := io.MultiReader(bytes.NewReader(truncated), iotest.ErrReader(transportErr))
	_, err = NewProtocStreamReader(failed).ReadBytes()
	require.ErrorIs(t, err, transportErr)
	_, err = NewProtocStreamReader(bytes.NewReader(protowire.AppendVarint(nil, math.MaxUint64))).ReadBytes()
	require.ErrorContains(t, err, "length exceeds int64")
	_, err = NewProtocStreamReader(bytes.NewReader(protowire.AppendVarint(nil, math.MaxUint32+1))).ReadUInt32()
	require.ErrorContains(t, err, "varint exceeds uint32")
	_, err = NewProtocStreamReader(bytes.NewReader(bytes.Repeat([]byte{0x80}, 12))).ReadVarint()
	require.ErrorContains(t, err, "overflows")
	_, _, err = NewProtocStreamReader(bytes.NewReader([]byte{0})).ReadTag()
	require.ErrorContains(t, err, "failed to read a tag")
}

func TestProtobufPrimitiveReaders(t *testing.T) {
	negative64 := int64(-257)
	negative32 := int32(negative64)
	signed := protowire.AppendVarint(nil, uint64(negative64))
	zigzag := protowire.AppendVarint(nil, protowire.EncodeZigZag(negative64))
	checkProtobufPrimitive(t, []byte{1}, (*ProtocStreamReader).ReadBool, true)
	checkProtobufPrimitive(t, signed, (*ProtocStreamReader).ReadInt32, negative32)
	checkProtobufPrimitive(t, signed, (*ProtocStreamReader).ReadInt64, negative64)
	checkProtobufPrimitive(t, zigzag, (*ProtocStreamReader).ReadSInt32, negative32)
	checkProtobufPrimitive(t, protowire.AppendFixed32(nil, uint32(negative32)), (*ProtocStreamReader).ReadSFixed32, negative32)
	checkProtobufPrimitive(t, protowire.AppendFixed64(nil, uint64(negative64)), (*ProtocStreamReader).ReadSFixed64, negative64)
	checkProtobufPrimitive(t, protowire.AppendVarint(nil, math.MaxUint32), (*ProtocStreamReader).ReadUInt32, uint32(math.MaxUint32))
}

func checkProtobufPrimitive[T comparable](t *testing.T, payload []byte, read func(*ProtocStreamReader) (T, error), want T) {
	t.Helper()
	got, err := read(NewProtocStreamReader(bytes.NewReader(payload)))
	require.NoError(t, err)
	require.Equal(t, want, got)
	for n := range len(payload) {
		_, err := read(NewProtocStreamReader(bytes.NewReader(payload[:n])))
		require.Error(t, err, "truncated at byte %d of %d", n, len(payload))
	}
}

func TestProtobufRecordAndTrailerValidation(t *testing.T) {
	columns := []tableschema.Column{{Name: "id", Type: datatype.BigIntType}}
	var buffer bytes.Buffer
	writer := newRecordProtocWriter(&bufWriter{&buffer}, columns, false)
	require.ErrorContains(t, writer.Write(data.Record{data.BigInt(1), data.String("surplus")}), "more than schema")
	require.NoError(t, writer.Write(data.Record{data.Null}))
	require.NoError(t, writer.Close())
	frame := buffer.Bytes()

	newReader := func(wire []byte) RecordProtocReader {
		return newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(wire))}, columns, false)
	}
	rowReader := newReader(frame)
	row, err := rowReader.Read()
	require.NoError(t, err)
	require.Equal(t, data.Record{data.Null}, row)
	_, err = rowReader.Read()
	require.ErrorIs(t, err, io.EOF)

	// A standalone trailer with no records must declare zero rows and contain
	// the checksum tag directly after the count.
	writeCount := func(count int64) []byte {
		wire := protowire.AppendTag(nil, MetaCount, protowire.VarintType)
		return protowire.AppendVarint(wire, protowire.EncodeZigZag(count))
	}
	for _, tc := range []struct {
		wire []byte
		msg  string
	}{
		{protowire.AppendTag(nil, 2, protowire.VarintType), "invalid protobuf tag"},
		{protowire.AppendVarint(protowire.AppendTag(nil, EndRecord, protowire.VarintType), 1), "crc value is error"},
		{writeCount(1), "record count does not match"},
		{protowire.AppendTag(writeCount(0), 1, protowire.VarintType), "invalid stream"},
	} {
		reader := newReader(tc.wire)
		_, err := reader.Read()
		require.ErrorContains(t, err, tc.msg)
	}

	reader := newReader(append(bytes.Clone(frame), 0))
	_, err = reader.Read()
	require.NoError(t, err)
	_, err = reader.Read()
	require.ErrorContains(t, err, "expect end of stream")

	reader = newReader(frame)
	reader.expectRange(0, 0, 1)
	_, err = reader.Read()
	require.ErrorContains(t, err, "more than 0 rows for the requested range")
}

func TestProtobufValidatesEntireRecordBeforeWriting(t *testing.T) {
	columns := []tableschema.Column{
		{Name: "label", Type: datatype.StringType, NotNull: true},
		{Name: "quantity", Type: datatype.TinyIntType},
		{Name: "nested", Type: datatype.NewArrayType(datatype.SmallIntType)},
	}
	var buffer bytes.Buffer
	writer := newRecordProtocWriter(&bufWriter{&buffer}, columns, false)
	tooWide := data.NewArrayWithType(datatype.NewArrayType(datatype.SmallIntType))
	tooWide.UnSafeAppend(data.Int(1 << 16))
	var nullString *data.String
	for _, tt := range []struct {
		values data.Record
		want   string
	}{
		{data.Record{data.String("ok"), data.BigInt(256)}, "quantity"},
		{data.Record{data.String("ok"), data.Float(1)}, "expected TINYINT"},
		{data.Record{data.String("ok"), data.TinyInt(1), tooWide}, "array element 0"},
		{data.Record{nullString}, "requires a value"},
	} {
		require.ErrorContains(t, writer.Write(tt.values), tt.want)
		require.Zero(t, buffer.Len())
		require.Zero(t, writer.RecordCount())
	}
	require.NoError(t, writer.Write(data.Record{data.String("ok"), data.Int(-128), data.Null}))
	require.NoError(t, writer.Close())
	reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(&buffer)}, columns, false)
	got, err := reader.Read()
	require.NoError(t, err)
	require.Equal(t, data.Record{data.String("ok"), data.TinyInt(-128), data.Null}, got)
	_, err = reader.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestProtobufRejectsWireIntegerOutsideColumnRange(t *testing.T) {
	columns := []tableschema.Column{{Name: "quantity", Type: datatype.TinyIntType}}
	wire := protowire.AppendTag(nil, 1, protowire.VarintType)
	wire = protowire.AppendVarint(wire, protowire.EncodeZigZag(256))
	reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(wire))}, columns, false)
	_, err := reader.Read()
	require.ErrorContains(t, err, "column \"quantity\": integer out of range for TINYINT")
}

type stalledRecordSink struct {
	writes int
	closed bool
}

func (s *stalledRecordSink) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == 1 {
		return len(p), nil
	}
	return 0, nil
}

func (s *stalledRecordSink) Close() error {
	s.closed = true
	return nil
}

func TestProtobufIOFailureLeavesStreamUncommittable(t *testing.T) {
	sink := &stalledRecordSink{}
	writer := newRecordProtocWriter(sink, []tableschema.Column{{Name: "id", Type: datatype.BigIntType}}, false)
	record := data.Record{data.BigInt(1)}
	require.ErrorIs(t, writer.Write(record), io.ErrShortWrite)
	require.ErrorIs(t, writer.Write(record), io.ErrShortWrite)
	require.ErrorIs(t, writer.Close(), io.ErrShortWrite)
	require.Equal(t, 2, sink.writes, "a partial record must not be followed by more data or a trailer")
	require.True(t, sink.closed)
}

func TestProtobufComplexValuesMatchTheColumnSchema(t *testing.T) {
	integer := datatype.IntType
	stringType := datatype.StringType
	mapType := datatype.NewMapType(stringType, integer)
	structType := datatype.NewStructType(datatype.NewStructFieldType("a", integer), datatype.NewStructFieldType("b", stringType))
	array := data.NewArrayWithType(datatype.NewArrayType(integer))
	badMapKey, badMapValue := data.NewMapWithType(mapType), data.NewMapWithType(mapType)
	require.NoError(t, badMapKey.Set(data.Int(1), data.Int(1)))
	require.NoError(t, badMapValue.Set(data.String("k"), data.String("v")))
	unknownField, wrongFieldType := data.NewStructWithTyp(structType), data.NewStructWithTyp(structType)
	require.NoError(t, unknownField.SetField("extra", data.Int(1)))
	require.NoError(t, wrongFieldType.SetField("a", data.String("not an integer")))
	char, err := data.MakeChar(3, "甲乙丙")
	require.NoError(t, err)
	varchar, err := data.MakeVarChar(3, "甲乙丙")
	require.NoError(t, err)
	decimalType := datatype.NewDecimalType(3, 1)

	for _, tt := range []struct {
		expected datatype.DataType
		value    data.Data
		want     string
	}{
		{nil, data.Int(1), "missing data type"},
		{datatype.BooleanType, data.Int(1), "got an integer"},
		{stringType, array, "got ARRAY"},
		{stringType, badMapKey, "got MAP"},
		{mapType, badMapKey, "map key"},
		{mapType, badMapValue, "map value"},
		{stringType, unknownField, "got STRUCT"},
		{structType, unknownField, "unexpected struct field"},
		{structType, wrongFieldType, "struct field"},
		{stringType, char, "got CHAR"},
		{datatype.NewCharType(2), char, "width exceeds"},
		{stringType, varchar, "got VARCHAR"},
		{stringType, data.NewDecimal(3, 1, "1.0"), "got DECIMAL"},
		{decimalType, data.NewDecimal(3, 1, "1e2"), "fixed-point"},
		{decimalType, data.NewDecimal(3, 1, "1."), "fixed-point"},
		{decimalType, data.NewDecimal(5, 2, "12.01"), "precision or scale"},
		{stringType, data.Bool(true), "expected STRING"},
	} {
		var output bytes.Buffer
		writer := newRecordProtocWriter(&bufWriter{&output}, []tableschema.Column{{Name: "typed", Type: tt.expected}}, false)
		require.ErrorContains(t, writer.Write(data.Record{tt.value}), tt.want)
		require.Empty(t, output.Bytes())
	}

	reordered := data.NewStructWithTyp(structType)
	require.NoError(t, reordered.SetField("b", data.String("text")))
	require.NoError(t, reordered.SetField("a", data.Int(5)))
	columns := []tableschema.Column{{Name: "typed", Type: structType}}
	var output bytes.Buffer
	writer := newRecordProtocWriter(&bufWriter{&output}, columns, false)
	require.NoError(t, writer.Write(data.Record{reordered}))
	require.NoError(t, writer.Close())
	reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(&output)}, columns, false)
	record, err := reader.Read()
	require.NoError(t, err)
	value := record[0].(*data.Struct)
	require.Equal(t, data.Int(5), value.GetField("a"))
	require.Equal(t, data.String("text"), value.GetField("b"))
}

func TestProtobufMapRejectsDuplicateKeysBeforeWriting(t *testing.T) {
	char1, err := data.MakeChar(1, "x")
	require.NoError(t, err)
	char2, err := data.MakeChar(2, "x ")
	require.NoError(t, err)
	now := time.Unix(12, 1234).UTC()
	for _, tc := range []struct {
		keyType       datatype.DataType
		first, second data.Data
	}{
		{datatype.BigIntType, data.Int(1), data.BigInt(1)},
		{datatype.IntType, data.SmallInt(1), data.TinyInt(1)},
		{datatype.StringType, nil, data.NullData{}},
		{datatype.StringType, data.String("text"), new(data.String("text"))},
		{datatype.NewCharType(4), char1, char2},
		{datatype.BinaryType, new(data.Binary{1, 2}), new(data.Binary{1, 2})},
		{datatype.GeographyType, new(data.Geography{1, 2}), new(data.Geography{1, 2})},
		{datatype.DoubleType, new(data.Double(math.NaN())), data.Double(math.NaN())},
		{datatype.NewDecimalType(10, 3), data.NewDecimal(5, 2, "-0.00"), data.NewDecimal(3, 1, "0.0")},
		{datatype.DateTimeType, data.DateTime(now), data.DateTime(now.Add(time.Microsecond))},
		{datatype.TimestampType, data.Timestamp(now), data.Timestamp(now.In(time.FixedZone("other", 3600)))},
	} {
		t.Run(tc.keyType.Name(), func(t *testing.T) {
			mapType := datatype.NewMapType(tc.keyType, datatype.StringType)
			m := data.NewMapWithType(mapType)
			require.NoError(t, m.Set(tc.first, "first"))
			require.NoError(t, m.Set(tc.second, "second"))
			columns := []tableschema.Column{{Name: "id", Type: datatype.BigIntType}, {Name: "attrs", Type: mapType}}
			var wire bytes.Buffer
			writer := newRecordProtocWriter(&bufWriter{&wire}, columns, false)
			row := data.Record{data.BigInt(42), m}
			err := writer.Write(row)
			require.ErrorContains(t, err, "duplicate map key")
			require.ErrorContains(t, err, "attrs")
			require.Empty(t, wire.Bytes())
			require.NoError(t, m.Delete(tc.second))
			require.NoError(t, writer.Write(row))
			require.NoError(t, writer.Close())
			reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(&wire)}, columns, false)
			got, err := reader.Read()
			require.NoError(t, err)
			require.Len(t, got[1].(*data.Map).ToGoMap(), 1)
			_, err = reader.Read()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestProtobufReaderRejectsDuplicateMapEntries(t *testing.T) {
	// Two non-null BIGINT keys with the same zigzag value, then two NULL values.
	field := []byte{2, 0, 2, 0, 2, 2, 1, 1}
	wire := characterRecordFixture(field, nil)
	columns := []tableschema.Column{{Name: "attrs", Type: datatype.NewMapType(datatype.BigIntType, datatype.StringType)}}
	reader := newRecordProtocReader(&http.Response{Body: io.NopCloser(bytes.NewReader(wire))}, columns, false)
	_, err := reader.Read()
	require.ErrorContains(t, err, "duplicate map key")
}

func TestProtobufMapPreservesDistinctBinaryAndVarcharKeys(t *testing.T) {
	short, err := data.MakeVarChar(4, "x")
	require.NoError(t, err)
	padded, err := data.MakeVarChar(4, "x ")
	require.NoError(t, err)
	for _, tc := range []struct {
		keyType       datatype.DataType
		first, second data.Data
	}{
		{datatype.BinaryType, new(data.Binary{0, 255}), new(data.Binary{0, 254})},
		{datatype.NewVarcharType(4), short, padded},
	} {
		typ := datatype.NewMapType(tc.keyType, datatype.StringType)
		input := data.NewMapWithType(typ)
		require.NoError(t, input.Set(tc.first, "first"))
		require.NoError(t, input.Set(tc.second, "second"))
		columns := []tableschema.Column{{Name: "attrs", Type: typ}}
		var wire bytes.Buffer
		w := newRecordProtocWriter(&bufWriter{&wire}, columns, false)
		require.NoError(t, w.Write(data.Record{input}))
		require.NoError(t, w.Close())
		r := newRecordProtocReader(&http.Response{Body: io.NopCloser(&wire)}, columns, false)
		row, err := r.Read()
		require.NoError(t, err)
		got := make(map[string]string)
		for key, value := range row[0].(*data.Map).ToGoMap() {
			got[key.String()] = value.String()
		}
		require.Equal(t, map[string]string{tc.first.String(): "first", tc.second.String(): "second"}, got)
		_, err = r.Read()
		require.ErrorIs(t, err, io.EOF)
	}
}
