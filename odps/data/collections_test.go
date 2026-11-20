// Licensed under the Apache License, Version 2.0.

package data_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestConvertGoPrimitiveValues(t *testing.T) {
	type budget int64
	type flag bool
	type label string
	type ratio float64
	type payload []byte
	var missing *int64
	n := int32(17)
	for _, test := range []struct {
		value any
		want  data.Data
	}{
		{flag(true), data.Bool(true)},
		{int8(-2), data.TinyInt(-2)},
		{int16(16), data.SmallInt(16)},
		{int(42), data.Int(42)},
		{&n, data.Int(17)},
		{budget(1 << 40), data.BigInt(1 << 40)},
		{float32(1.25), data.Float(1.25)},
		{ratio(2.5), data.Double(2.5)},
		{label("example"), data.String("example")},
		{payload{0x01, 0xff}, data.Binary{0x01, 0xff}},
		{data.String("already converted"), data.String("already converted")},
		{nil, nil},
		{missing, nil},
	} {
		got, err := data.TryConvertGoToOdpsData(test.value)
		require.NoError(t, err, "%T", test.value)
		require.Equal(t, test.want, got, "%T", test.value)
	}

	for _, value := range []any{
		make(chan int), []any{1, make(chan int)}, map[uint]string{1: "unsupported key"},
		map[string]uint{"unsupported value": 1}, struct{ Value uint }{1},
	} {
		_, err := data.TryConvertGoToOdpsData(value)
		require.ErrorContains(t, err, "Cannot convert data", "%T", value)
	}
}

func TestArrayTypeCheckingInferenceAndScan(t *testing.T) {
	array := data.NewArray()
	require.Equal(t, "array()", array.String())
	require.Equal(t, "array()", array.Sql())
	_, err := array.TypeInfer()
	require.ErrorContains(t, err, "empty array")
	require.ErrorContains(t, array.SafeAppend(1), "element type")
	require.ErrorContains(t, array.Append(make(chan int)), "Cannot convert")

	typ := datatype.NewArrayType(datatype.IntType)
	array.SetType(typ)
	require.Equal(t, typ, array.Type())
	require.NoError(t, array.SafeAppend(nil))
	_, err = array.TypeInfer()
	require.ErrorContains(t, err, "all null")
	require.ErrorContains(t, array.SafeAppend(make(chan int)), "Cannot convert")
	require.ErrorContains(t, array.SafeAppend("wrong"), "expect INT")
	require.NoError(t, array.SafeAppend(3, 5))
	require.Equal(t, 3, array.Len())
	require.Equal(t, data.Int(3), array.Index(1))
	require.Equal(t, []data.Data{nil, data.Int(3), data.Int(5)}, array.ToSlice())
	require.Equal(t, "array(null, 3, 5)", array.Sql())
	inferred, err := array.TypeInfer()
	require.NoError(t, err)
	require.Equal(t, typ, inferred)

	var scanned data.Array
	require.NoError(t, scanned.Scan(array))
	require.Equal(t, array.ToSlice(), scanned.ToSlice())
	require.ErrorContains(t, scanned.Scan("wrong"), "cannot convert")
	mixed, err := data.ArrayFromSlice(int64(10), "different")
	require.NoError(t, err)
	_, err = mixed.TypeInfer()
	require.ErrorContains(t, err, "element type is not the same")
	_, err = data.ArrayFromSlice(make(chan int))
	require.Error(t, err)
}

func TestMapTypeCheckingInferenceAndScan(t *testing.T) {
	untyped := data.NewMap()
	require.Equal(t, "map()", untyped.Sql())
	_, err := untyped.TypeInfer()
	require.ErrorContains(t, err, "empty map")
	require.ErrorContains(t, untyped.SafeSet(data.String("key"), data.Int(1)), "element type")
	require.ErrorContains(t, untyped.Set(make(chan int), nil), "Cannot convert")
	require.ErrorContains(t, untyped.Set("key", make(chan int)), "Cannot convert")
	require.ErrorContains(t, untyped.Set(data.Binary{1}, 1), "not comparable")

	typ := datatype.NewMapType(datatype.StringType, datatype.IntType)
	dict := data.NewMapWithType(typ)
	require.Equal(t, typ, dict.Type())
	require.ErrorContains(t, dict.SafeSet(data.Int(1), data.Int(2)), "fail to set key")
	require.ErrorContains(t, dict.SafeSet(data.String("key"), data.Double(1)), "fail to set value")
	require.NoError(t, dict.SafeSet(nil, data.Int(7)))
	_, err = dict.TypeInfer()
	require.ErrorContains(t, err, "keys or values are all null")
	require.NoError(t, dict.SafeSet(data.String("key"), nil))
	require.NoError(t, dict.SafeSet(data.String("other"), data.Int(3)))
	inferred, err := dict.TypeInfer()
	require.NoError(t, err)
	require.Equal(t, typ, inferred)
	require.Contains(t, dict.String(), "null, 7")
	require.Contains(t, dict.String(), "'key', null")

	spread, err := data.MapFromGoMap(map[string]int{"one": 1, "two": 2})
	require.NoError(t, err)
	require.Equal(t, map[data.Data]data.Data{data.String("one"): data.Int(1), data.String("two"): data.Int(2)}, spread.ToGoMap())
	var scanned data.Map
	require.NoError(t, scanned.Scan(spread))
	require.True(t, data.IsDataEqual(spread, &scanned))
	require.ErrorContains(t, scanned.Scan([]int{1}), "cannot convert")

	require.NoError(t, spread.Set(3, 3))
	_, err = spread.TypeInfer()
	require.ErrorContains(t, err, "key type is not the same")
	require.NoError(t, untyped.Set("int", 1))
	require.NoError(t, untyped.Set("string", "mixed"))
	_, err = untyped.TypeInfer()
	require.ErrorContains(t, err, "value type is not the same")
	_, err = data.MapFromGoMap(nil)
	require.ErrorContains(t, err, "not a map")
	_, err = data.MapFromGoMap([]int{1})
	require.ErrorContains(t, err, "not a map")
	_, err = data.MapFromGoMap(map[uint]int{1: 1})
	require.ErrorContains(t, err, "Cannot convert")
}

func TestStructTypeCheckingAndConversion(t *testing.T) {
	typ := datatype.NewStructType(
		datatype.NewStructFieldType("id", datatype.BigIntType),
		datatype.NewStructFieldType("label", datatype.StringType),
	)
	untyped := data.NewStruct()
	_, err := untyped.TypeInfer()
	require.ErrorContains(t, err, "empty struct")
	require.ErrorContains(t, untyped.SafeSetField("id", int64(1)), "type of Struct")
	require.ErrorContains(t, untyped.SetField("bad", make(chan int)), "Cannot convert")
	structure := data.NewStructWithTyp(typ)
	require.ErrorContains(t, structure.SafeSetField("id", make(chan int)), "Cannot convert")
	require.ErrorContains(t, structure.SafeSetField("absent", nil), "cannot set absent")
	require.ErrorContains(t, structure.SafeSetField("id", "wrong"), "cannot set type")
	require.NoError(t, structure.SafeSetField("id", int64(7)))
	require.NoError(t, structure.SafeSetField("label", nil))
	_, err = structure.TypeInfer()
	require.ErrorContains(t, err, "null field label")
	require.NoError(t, structure.SafeSetField("label", "updated"))
	require.Nil(t, structure.GetField("absent"))
	require.Equal(t, []data.StructField{
		data.NewStructField("id", data.BigInt(7)), data.NewStructField("label", data.String("updated")),
	}, structure.Fields())
	require.Equal(t, "named_struct('id', 7L, 'label', 'updated')", structure.Sql())
	inferred, err := structure.TypeInfer()
	require.NoError(t, err)
	require.Equal(t, typ, inferred)

	var scanned data.Struct
	require.NoError(t, scanned.Scan(structure))
	require.Equal(t, structure.Fields(), scanned.Fields())
	require.ErrorContains(t, scanned.Scan("wrong"), "cannot convert")

	type child struct {
		ID int64 `odps:"id"`
	}
	type parent struct {
		Child     child
		Values    []data.Data
		Lookup    map[data.Data]data.Data
		Note      string
		untouched bool
	}
	input := parent{
		Child: child{ID: 9}, Note: "after child",
		Values: []data.Data{data.String("a"), nil},
		Lookup: map[data.Data]data.Data{data.String("id"): data.BigInt(9)},
	}
	converted, err := data.StructFromGoStruct(input)
	require.NoError(t, err)
	require.Nil(t, converted.GetField("untouched"))
	var output parent
	output.untouched = true
	require.NoError(t, converted.FillGoStruct(&output))
	input.untouched = true
	require.Equal(t, input, output)
	require.NoError(t, converted.SetField("Note", new(data.String("pointer string"))))
	require.NoError(t, converted.FillGoStruct(&output))
	require.Equal(t, "pointer string", output.Note)
	var invalidTarget struct{ Child string }
	require.ErrorContains(t, converted.FillGoStruct(&invalidTarget), "field Child is not a struct")
	for _, target := range []any{nil, (*parent)(nil), output, new(int)} {
		require.ErrorContains(t, converted.FillGoStruct(target), "non-nil pointer to a struct", "%T", target)
	}
	_, err = data.StructFromGoStruct(nil)
	require.ErrorContains(t, err, "not a struct")
	_, err = data.StructFromGoStruct([]int{1})
	require.ErrorContains(t, err, "not a struct")
	_, err = data.StructFromGoStruct(struct{ Bad uint }{1})
	require.ErrorContains(t, err, "Cannot convert")
}
