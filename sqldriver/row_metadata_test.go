// Licensed under the Apache License, Version 2.0.

package sqldriver

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func TestRowValuesAndColumnMetadata(t *testing.T) {
	dt := time.Date(2000, 2, 29, 12, 0, 0, 123456789, time.UTC)
	char, err := data.MakeChar(4, "a")
	require.NoError(t, err)
	varchar, err := data.MakeVarChar(8, "你好")
	require.NoError(t, err)
	decimal := data.NewDecimal(15, 3, "-1.020")
	list := data.NewArrayWithType(datatype.NewArrayType(datatype.IntType))
	mapping := data.NewMapWithType(datatype.NewMapType(datatype.StringType, datatype.IntType))
	structure := data.NewStructWithTyp(datatype.NewStructType(datatype.StructFieldType{Name: "i", Type: datatype.IntType}))
	jsonValue := &data.Json{Data: "{}", Valid: true}

	for _, tc := range []struct {
		value data.Data
		want  driver.Value
		scan  any
		null  any
	}{
		{data.BigInt(-42), int64(-42), int64(0), NullInt64{}},
		{data.Int(-12), int(-12), int(0), NullInt32{}},
		{data.SmallInt(-2), int16(-2), int16(0), NullInt16{}},
		{data.TinyInt(7), int8(7), int8(0), NullInt8{}},
		{data.Float(3.25), float32(3.25), float32(0), NullFloat32{}},
		{data.Double(3.5), float64(3.5), float64(0), NullFloat64{}},
		{data.String("text"), "text", "", NullString{}},
		{char, char.Data(), "", NullString{}},
		{varchar, varchar.Data(), "", NullString{}},
		{data.Binary{0, 42}, []byte{0, 42}, Binary{}, Binary{}},
		{data.Bool(true), true, false, NullBool{}},
		{data.Date(dt), dt, NullDate{}, NullDate{}},
		{data.DateTime(dt), dt, NullDateTime{}, NullDateTime{}},
		{data.Timestamp(dt), dt, NullTimeStamp{}, NullTimeStamp{}},
		{data.TimestampNtz(dt), dt, NullTimeStampNtz{}, NullTimeStampNtz{}},
		{decimal, decimal, Decimal{}, Decimal{}},
		{list, list, Array{}, Array{}},
		{mapping, mapping, Map{}, Map{}},
		{structure, structure, Struct{}, Struct{}},
		{jsonValue, jsonValue, Json{}, Json{}},
		{data.NewIntervalDayTime(10, 250000000), data.NewIntervalDayTime(10, 250000000), data.IntervalDayTime{}, data.IntervalDayTime{}},
		{data.IntervalYearMonth(-2), data.IntervalYearMonth(-2), data.IntervalYearMonth(0), data.IntervalYearMonth(0)},
	} {
		t.Run(tc.value.Type().Name(), func(t *testing.T) {
			typ := tc.value.Type()
			reader := &rowsReader{
				columns: []tableschema.Column{
					{Name: "required", Type: typ, NotNull: true},
					{Name: "optional", Type: typ},
				},
				inner: &fakeRecordReader{records: []data.Record{{tc.value, nil}}},
			}
			require.Equal(t, []string{"required", "optional"}, reader.Columns())
			require.Equal(t, typ.Name(), reader.ColumnTypeDatabaseTypeName(0))
			require.Equal(t, reflect.TypeOf(tc.scan), reader.ColumnTypeScanType(0))
			require.Equal(t, reflect.TypeOf(tc.null), reader.ColumnTypeScanType(1))
			values := make([]driver.Value, 2)
			require.NoError(t, reader.Next(values))
			require.Equal(t, []driver.Value{tc.want, nil}, values)
			require.ErrorIs(t, reader.Next(values), io.EOF)
			require.NoError(t, reader.Close())
		})
	}
}

func TestRowReaderRejectsMismatchedColumnCount(t *testing.T) {
	reader := &rowsReader{
		columns: []tableschema.Column{{Name: "v", Type: datatype.BigIntType}},
		inner:   &fakeRecordReader{records: []data.Record{{data.BigInt(1), data.BigInt(2)}}},
	}
	require.ErrorContains(t, reader.Next(make([]driver.Value, 1)), "expect 1 columns, but get 2")
	require.NoError(t, reader.Close())
}

func TestNullNumericScanners(t *testing.T) {
	var i8 NullInt8
	require.NoError(t, i8.Scan(int8(-4)))
	require.Equal(t, NullInt8{Int8: -4, Valid: true}, i8)
	require.Error(t, i8.Scan("not a number"))
	require.NoError(t, i8.Scan(nil))
	require.Equal(t, NullInt8{}, i8)

	var i16 NullInt16
	require.NoError(t, i16.Scan(int16(-256)))
	require.Equal(t, NullInt16{Int16: -256, Valid: true}, i16)
	require.Error(t, i16.Scan("not a number"))
	require.NoError(t, i16.Scan(nil))
	require.Equal(t, NullInt16{}, i16)

	var f32 NullFloat32
	require.NoError(t, f32.Scan(float32(1.25)))
	require.Equal(t, NullFloat32{Float32: 1.25, Valid: true}, f32)
	require.Error(t, f32.Scan("not a number"))
	require.NoError(t, f32.Scan(nil))
	require.Equal(t, NullFloat32{}, f32)

	var binary Binary
	require.NoError(t, binary.Scan([]byte{1, 7}))
	require.Equal(t, Binary{1, 7}, binary)
	require.Error(t, binary.Scan("not bytes"))
	require.NoError(t, binary.Scan(nil))
	require.Nil(t, binary)
}
