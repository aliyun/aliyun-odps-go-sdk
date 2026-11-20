// Licensed under the Apache License, Version 2.0.

package datatype_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestParseDataTypeRejectsIncompleteAndInvalidDefinitions(t *testing.T) {
	for _, name := range []string{
		"", "  ", "unknown",
		"int trailing", "array<bigint> trailing",
		"char", "char(", "char(hello)", "char(256)", "char(0)", "char(1",
		"varchar", "varchar(", "varchar(hello)", "varchar(65536)", "varchar(0)", "varchar(1",
		"decimal[", "decimal(", "decimal(x,1)", "decimal(12", "decimal(12,", "decimal(12,x)", "decimal(12,2",
		"array", "array<", "array<unknown>", "array<int",
		"map", "map<", "map<unknown,int>", "map<bigint", "map<bigint,", "map<bigint,unknown>", "map<bigint,string",
		"struct", "struct<", "struct<x", "struct<x:", "struct<x:unknown>", "struct<x:int", "struct<x:int,>",
		"struct<`field", "struct<``:int>",
	} {
		t.Run(name, func(t *testing.T) {
			typ, err := datatype.ParseDataType(name)
			require.Error(t, err)
			require.Nil(t, typ)
		})
	}
}

func TestStructTypesRoundTripAllFields(t *testing.T) {
	typ := datatype.NewStructType(
		datatype.NewStructFieldType("last seen", datatype.TimestampNtzType),
		datatype.NewStructFieldType("有效", datatype.NewArrayType(datatype.NewMapType(datatype.StringType, datatype.IntType))),
		datatype.NewStructFieldType("integer", datatype.IntType),
	)
	roundTripped, err := datatype.ParseDataType(typ.String())
	require.NoError(t, err)
	require.True(t, datatype.IsTypeEqual(typ, roundTripped))
	require.Equal(t, typ, roundTripped)
	require.Equal(t, datatype.TimestampNtzType, typ.FieldType("last seen"))
	require.Nil(t, typ.FieldType("missing"))
	reordered := datatype.NewStructType(typ.Fields[2], typ.Fields[0], typ.Fields[1])
	require.True(t, datatype.IsTypeEqual(typ, reordered))

	differentType := datatype.NewStructType(typ.Fields[0],
		datatype.NewStructFieldType("有效", datatype.NewArrayType(datatype.NewMapType(datatype.StringType, datatype.BigIntType))),
		typ.Fields[2])
	require.False(t, datatype.IsTypeEqual(typ, differentType))
	differentName := datatype.NewStructType(typ.Fields[0], typ.Fields[1], datatype.NewStructFieldType("other", datatype.IntType))
	require.False(t, datatype.IsTypeEqual(typ, differentName))
	require.False(t, datatype.IsTypeEqual(typ, datatype.NewStructType(typ.Fields[0])))
	require.Equal(t, "MAP<STRING,INT>", datatype.NewMapType(datatype.StringType, datatype.IntType).String())
	require.Equal(t, "VARCHAR(12)", datatype.NewVarcharType(12).String())
	require.Equal(t, "JSON", datatype.NewJsonType().String())
}

func TestWireTypeNamesAndJSON(t *testing.T) {
	for _, test := range []struct {
		name string
		id   datatype.TypeID
	}{
		{"BIGINT", datatype.BIGINT}, {"DOUBLE", datatype.DOUBLE}, {"BOOLEAN", datatype.BOOLEAN},
		{"DATETIME", datatype.DATETIME}, {"STRING", datatype.STRING}, {"DECIMAL", datatype.DECIMAL},
		{"MAP", datatype.MAP}, {"ARRAY", datatype.ARRAY}, {"VOID", datatype.VOID},
		{"TINYINT", datatype.TINYINT}, {"SMALLINT", datatype.SMALLINT}, {"INT", datatype.INT},
		{"FLOAT", datatype.FLOAT}, {"CHAR", datatype.CHAR}, {"VARCHAR", datatype.VARCHAR},
		{"DATE", datatype.DATE}, {"TIMESTAMP", datatype.TIMESTAMP}, {"TIMESTAMP_NTZ", datatype.TIMESTAMP_NTZ},
		{"BINARY", datatype.BINARY}, {"INTERVAL_DAY_TIME", datatype.IntervalDayTime},
		{"INTERVAL_YEAR_MONTH", datatype.IntervalYearMonth}, {"STRUCT", datatype.STRUCT},
		{"JSON", datatype.JSON}, {"GEOGRAPHY", datatype.GEOGRAPHY},
	} {
		require.Equal(t, test.name, test.id.String())
		var decoded datatype.TypeID
		require.NoError(t, json.Unmarshal([]byte("\""+test.name+"\""), &decoded))
		require.Equal(t, test.id, decoded)
	}
	var decoded datatype.TypeID
	require.NoError(t, json.Unmarshal([]byte(`"new wire type"`), &decoded))
	require.Equal(t, datatype.TypeUnknown, decoded)
	require.Equal(t, "TYPE_UNKNOWN", decoded.String())
	require.Error(t, json.Unmarshal([]byte("17"), &decoded))
	typ, err := datatype.ParseDataType("json")
	require.NoError(t, err)
	require.Equal(t, datatype.JSON, typ.ID())
}

func TestGoTypeConversionRejectsUnsupportedNestedFields(t *testing.T) {
	for _, value := range []any{
		nil, 17, reflect.TypeOf(make(chan int)), reflect.TypeOf([2]uint{}),
		reflect.TypeOf(map[uint]string{}), reflect.TypeOf(map[string]uint{}), reflect.TypeOf(struct{ Invalid uint }{}),
	} {
		_, err := datatype.TryConvertGoToOdpsType(value)
		require.Error(t, err, "%v", value)
	}
	arr, err := datatype.TryConvertGoToOdpsType(reflect.TypeOf([3]int16{}))
	require.NoError(t, err)
	require.Equal(t, datatype.NewArrayType(datatype.SmallIntType), arr)
}
