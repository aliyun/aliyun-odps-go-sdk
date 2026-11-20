// Licensed under the Apache License, Version 2.0.

package tableschema

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestArrowSchemaForStorage(t *testing.T) {
	for _, tc := range []struct {
		odps string
		want arrow.DataType
	}{
		{"boolean", arrow.FixedWidthTypes.Boolean},
		{"tinyint", arrow.PrimitiveTypes.Int8},
		{"smallint", arrow.PrimitiveTypes.Int16},
		{"int", arrow.PrimitiveTypes.Int32},
		{"bigint", arrow.PrimitiveTypes.Int64},
		{"float", arrow.PrimitiveTypes.Float32},
		{"double", arrow.PrimitiveTypes.Float64},
		{"char(4)", arrow.BinaryTypes.String},
		{"varchar(12)", arrow.BinaryTypes.String},
		{"string", arrow.BinaryTypes.String},
		{"json", arrow.BinaryTypes.String},
		{"binary", arrow.BinaryTypes.Binary},
		{"date", arrow.FixedWidthTypes.Date32},
		{"datetime", arrow.FixedWidthTypes.Timestamp_ms},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{"timestamp_ntz", &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{"decimal(21,7)", &arrow.Decimal128Type{Precision: 21, Scale: 7}},
		{"array<string>", arrow.ListOf(arrow.BinaryTypes.String)},
		{"map<string,bigint>", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{"struct<value:bigint>", arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true})},
	} {
		t.Run(tc.odps, func(t *testing.T) {
			typ, err := datatype.ParseDataType(tc.odps)
			require.NoError(t, err)
			schema, err := (&TableSchema{Columns: []Column{{Name: "c", Type: typ, NotNull: true}}}).ToArrowSchema()
			require.NoError(t, err)
			require.True(t, arrow.TypeEqual(tc.want, schema.Field(0).Type), "got %s", schema.Field(0).Type)
			require.False(t, schema.Field(0).Nullable)
		})
	}

	for _, tc := range []struct {
		typ datatype.DataType
		msg string
	}{
		{nil, "missing MaxCompute type"},
		{datatype.NewPrimitiveType(datatype.VOID), "unsupported MaxCompute type"},
		{datatype.NewDecimalType(39, 0), "invalid MaxCompute type"},
		{datatype.NewArrayType(nil), "array element"},
		{datatype.NewMapType(nil, datatype.StringType), "map key"},
		{datatype.NewMapType(datatype.StringType, nil), "map value"},
		{datatype.NewStructType(datatype.StructFieldType{Name: "bad"}), "struct field"},
	} {
		_, err := (&TableSchema{Columns: []Column{{Name: "value", Type: tc.typ}}}).ToArrowSchema()
		require.ErrorContains(t, err, `column "value": `+tc.msg)
	}
}

func TestArrowRecordsOwnNestedValues(t *testing.T) {
	listType := datatype.NewArrayType(datatype.BigIntType)
	mapType := datatype.NewMapType(datatype.StringType, datatype.StringType)
	structType := datatype.NewStructType(datatype.StructFieldType{Name: "items", Type: listType})
	schema := TableSchema{Columns: []Column{
		{Name: "record", Type: structType},
		{Name: "attributes", Type: mapType},
		{Name: "payload", Type: datatype.BinaryType},
	}}
	physicalSchema, err := schema.ToArrowSchema()
	require.NoError(t, err)

	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	builder := array.NewRecordBuilder(allocator, physicalSchema)
	record := builder.Field(0).(*array.StructBuilder)
	list := record.FieldBuilder(0).(*array.ListBuilder)
	values := list.ValueBuilder().(*array.Int64Builder)
	attributes := builder.Field(1).(*array.MapBuilder)
	keys := attributes.KeyBuilder().(*array.StringBuilder)
	items := attributes.ItemBuilder().(*array.StringBuilder)
	payload := builder.Field(2).(*array.BinaryBuilder)

	record.Append(true)
	list.Append(true)
	values.Append(11)
	values.AppendNull()
	attributes.Append(true)
	keys.Append("first")
	items.AppendNull()
	payload.Append([]byte{1, 2})

	record.Append(true)
	list.Append(true)
	values.Append(22)
	attributes.Append(true)
	keys.Append("second")
	items.Append("value")
	payload.Append([]byte{3, 4})

	record.AppendNull()
	attributes.AppendNull()
	payload.AppendNull()

	batch := builder.NewRecordBatch()
	builder.Release()
	slice := batch.NewSlice(1, 3)
	all, err := ToMaxComputeRecords(batch, schema.Columns)
	require.NoError(t, err)
	sliced, err := ToMaxComputeRecords(slice, schema.Columns)
	slice.Release()
	batch.Release()
	require.NoError(t, err)
	allocator.AssertSize(t, 0)

	require.Equal(t, []data.Data{data.BigInt(11), data.Null}, all[0][0].(*data.Struct).GetField("items").(*data.Array).ToSlice())
	require.Equal(t, data.Null, all[0][1].(*data.Map).ToGoMap()[data.String("first")])
	require.Equal(t, data.Binary{1, 2}, all[0][2])
	require.Equal(t, []data.Data{data.BigInt(22)}, sliced[0][0].(*data.Struct).GetField("items").(*data.Array).ToSlice())
	require.Equal(t, map[data.Data]data.Data{data.String("second"): data.String("value")}, sliced[0][1].(*data.Map).ToGoMap())
	require.Equal(t, data.Binary{3, 4}, sliced[0][2])
	require.Equal(t, data.Record{data.Null, data.Null, data.Null}, sliced[1])
}

func TestArrowRecordTimeAndDecimal(t *testing.T) {
	schema := TableSchema{Columns: []Column{
		{Name: "dt", Type: datatype.DateTimeType},
		{Name: "ts", Type: datatype.TimestampType},
		{Name: "ntz", Type: datatype.TimestampNtzType},
		{Name: "n", Type: datatype.NewDecimalType(18, 4)},
	}}
	physicalSchema, err := schema.ToArrowSchema()
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, physicalSchema)
	defer builder.Release()
	dt := time.Date(1969, 12, 31, 23, 59, 59, 123000000, time.UTC)
	ts := time.Date(2001, 2, 3, 4, 5, 6, 123456789, time.UTC)
	ntz := time.Date(2001, 2, 3, 4, 5, 6, 987654321, time.UTC)
	builder.Field(0).(*array.TimestampBuilder).AppendTime(dt)
	builder.Field(1).(*array.TimestampBuilder).AppendTime(ts)
	builder.Field(2).(*array.TimestampBuilder).AppendTime(ntz)
	builder.Field(3).(*array.Decimal128Builder).Append(decimal128.FromI64(-123))
	batch := builder.NewRecordBatch()
	defer batch.Release()

	rows, err := ToMaxComputeRecords(batch, schema.Columns)
	require.NoError(t, err)
	require.True(t, dt.Equal(time.Time(rows[0][0].(data.DateTime))))
	require.True(t, ts.Equal(time.Time(rows[0][1].(data.Timestamp))))
	require.True(t, ntz.Equal(time.Time(rows[0][2].(data.TimestampNtz))))
	require.Equal(t, "-0.0123", rows[0][3].(*data.Decimal).Value())

	_, err = ToMaxComputeRecords(nil, schema.Columns)
	require.ErrorContains(t, err, "do not match")
	_, err = ToMaxComputeRecords(batch, schema.Columns[:1])
	require.ErrorContains(t, err, "do not match")
	wrong := append([]Column(nil), schema.Columns...)
	wrong[3].Type = datatype.NewDecimalType(18, 0)
	_, err = ToMaxComputeRecords(batch, wrong)
	require.ErrorContains(t, err, "decimal scale")
	wrong[3].Type = datatype.StringType
	_, err = ToMaxComputeRecords(batch, wrong)
	require.ErrorContains(t, err, "incompatible Arrow batch")
}

func TestArrowRecordIntervals(t *testing.T) {
	schema := TableSchema{Columns: []Column{
		{Name: "elapsed", Type: datatype.IntervalDayTimeType},
		{Name: "months", Type: datatype.IntervalYearMonthType},
	}}
	physicalSchema, err := schema.ToArrowSchema()
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, physicalSchema)
	defer builder.Release()
	builder.Field(0).(*array.DayTimeIntervalBuilder).Append(arrow.DayTimeInterval{Days: 1, Milliseconds: 1234})
	builder.Field(1).(*array.MonthIntervalBuilder).Append(-13)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	rows, err := ToMaxComputeRecords(batch, schema.Columns)
	require.NoError(t, err)
	interval := rows[0][0].(data.IntervalDayTime)
	require.EqualValues(t, 86401, interval.Seconds())
	require.EqualValues(t, 234000000, interval.NanosFraction())
	require.Equal(t, data.IntervalYearMonth(-13), rows[0][1])
}

func TestArrowRecordPrimitives(t *testing.T) {
	schema := TableSchema{Columns: []Column{
		{Name: "active", Type: datatype.BooleanType},
		{Name: "tiny", Type: datatype.TinyIntType},
		{Name: "small", Type: datatype.SmallIntType},
		{Name: "count", Type: datatype.IntType},
		{Name: "f", Type: datatype.FloatType},
		{Name: "d", Type: datatype.DoubleType},
		{Name: "day", Type: datatype.DateType},
	}}
	physical, err := schema.ToArrowSchema()
	require.NoError(t, err)
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, physical, strings.NewReader(
		`[{"active":true,"tiny":-8,"small":1024,"count":-100,"f":1.25,"d":-0.5,"day":"1969-12-31"}]`,
	))
	require.NoError(t, err)
	defer batch.Release()

	records, err := ToMaxComputeRecords(batch, schema.Columns)
	require.NoError(t, err)
	require.Equal(t, []data.Record{{
		data.Bool(true), data.TinyInt(-8), data.SmallInt(1024), data.Int(-100), data.Float(1.25), data.Double(-0.5),
		data.Date(time.Date(1969, time.December, 31, 0, 0, 0, 0, time.UTC)),
	}}, records)
}

func TestArrowIncompatibleNestedFieldsFailBeforeReturningRows(t *testing.T) {
	decimal := datatype.NewDecimalType(12, 2)
	wrongScale := datatype.NewDecimalType(12, 3)
	schema := TableSchema{Columns: []Column{
		{Name: "items", Type: datatype.NewArrayType(decimal)},
		{Name: "lookup", Type: datatype.NewMapType(decimal, decimal)},
		{Name: "record", Type: datatype.NewStructType(datatype.NewStructFieldType("amount", decimal))},
	}}
	physical, err := schema.ToArrowSchema()
	require.NoError(t, err)
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, physical, strings.NewReader(
		`[{"items":["1.23"],"lookup":[{"key":"4.56","value":"7.89"}],"record":{"amount":"0.10"}}]`,
	))
	require.NoError(t, err)
	defer batch.Release()

	for _, tc := range []struct {
		index int
		typ   datatype.DataType
		msg   string
	}{
		{0, datatype.NewArrayType(wrongScale), `column "items": incompatible Arrow decimal scale`},
		{1, datatype.NewMapType(wrongScale, decimal), `column "lookup": incompatible Arrow decimal scale`},
		{1, datatype.NewMapType(decimal, wrongScale), `column "lookup": incompatible Arrow decimal scale`},
		{2, datatype.NewStructType(), "struct field count"},
		{2, datatype.NewStructType(datatype.NewStructFieldType("amount", wrongScale)), `struct field "amount": incompatible Arrow decimal scale`},
		{0, datatype.NewPrimitiveType(datatype.VOID), "unsupported ODPS type"},
	} {
		columns := append([]Column(nil), schema.Columns...)
		columns[tc.index].Type = tc.typ
		records, err := ToMaxComputeRecords(batch, columns)
		require.ErrorContains(t, err, tc.msg)
		require.Nil(t, records)
	}
}
