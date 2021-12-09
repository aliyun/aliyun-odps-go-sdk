package datatype

import (
	"reflect"
	"testing"
)

func TestTryConvertGoToOdpsType(t *testing.T) {
	type SimpleStruct struct {
		_a string `odps:"a"`
		b  struct {
			x int32
			y int64
		}
	}

	simpleStructType := NewStructType(
		NewStructFieldType("a", StringType),
		NewStructFieldType(
			"b",
			NewStructType(
				NewStructFieldType("x", IntType),
				NewStructFieldType("y", BigIntType),
			),
		),
	)

	testData := []struct {
		input interface{}
		want  DataType
	}{
		{reflect.TypeOf(true), BooleanType},
		{reflect.TypeOf(int8(0)), TinyIntType},
		{reflect.TypeOf(int32(0)), IntType},
		{reflect.TypeOf(int16(0)), SmallIntType},
		{reflect.TypeOf(int64(0)), BigIntType},
		{reflect.TypeOf(float32(0)), FloatType},
		{reflect.TypeOf(float64(0)), DoubleType},
		{reflect.TypeOf(""), StringType},
		{reflect.TypeOf([]byte{0}), BinaryType},
		{reflect.TypeOf([]int8{0}), NewArrayType(TinyIntType)},
		{reflect.TypeOf(make(map[string]int32)), NewMapType(StringType, IntType)},
		{reflect.TypeOf(SimpleStruct{}), simpleStructType},
		{NewDecimalType(38, 18), NewDecimalType(38, 18)},
		{DateType, DateType},
		{DateTimeType, DateTimeType},
		{TimestampType, TimestampType},
		{IntervalDayTimeType, IntervalDayTimeType},
		{IntervalYearMonthType, IntervalYearMonthType},
		{NewCharType(10), NewCharType(10)},
		{NewVarcharType(10), NewVarcharType(10)},
	}

	for _, d := range testData {
		got, err := TryConvertGoToOdpsType(d.input)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if !IsTypeEqual(got, d.want) {
			t.Fatalf("expect %s type, but get %s", d.want, got)
		}
	}
}
