// Licensed under the Apache License, Version 2.0.

package data_test

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestDecimalFromStrPreservesPrecisionAndScale(t *testing.T) {
	for _, tc := range []struct {
		value            string
		precision, scale int
	}{
		{"0", 1, 0},
		{"+00120.45", 5, 2},
		{"-0.100", 3, 3},
		{"-12345678901234567890123456789012345678", 38, 0},
		{"0." + strings.Repeat("1", 38), 38, 38},
		{"9999999999999999.999999999999999999", 34, 18},
	} {
		decimal, err := data.DecimalFromStr(tc.value)
		require.NoError(t, err, tc.value)
		require.Equal(t, tc.value, decimal.String())
		require.Equal(t, tc.precision, decimal.Precision())
		require.Equal(t, tc.scale, decimal.Scale())
		require.Equal(t, datatype.NewDecimalType(int32(tc.precision), int32(tc.scale)), decimal.Type())
	}
	for _, invalid := range []string{strings.Repeat("9", 39), "1." + strings.Repeat("9", 38), "1.2.3", "1.", ".1", "-", "1.-2"} {
		_, err := data.DecimalFromStr(invalid)
		require.ErrorIs(t, err, data.InvalidDecimalErr, invalid)
	}
}

func TestConvertGoDataDoesNotNarrowIntSilently(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("Go int is already 32 bits")
	}
	for _, value := range []int64{math.MinInt32 - 1, math.MaxInt32 + 1} {
		_, err := data.TryConvertGoToOdpsData(int(value))
		require.ErrorContains(t, err, "out of range for INT")
		converted, err := data.TryConvertGoToOdpsData(value)
		require.NoError(t, err)
		require.Equal(t, data.BigInt(value), converted)
	}
	converted, err := data.TryConvertGoToOdpsData([2]int{math.MinInt32, math.MaxInt32})
	require.NoError(t, err)
	require.Equal(t, []data.Data{data.Int(math.MinInt32), data.Int(math.MaxInt32)}, converted.(*data.Array).ToSlice())
}

func TestScanHandlesTypedNilValues(t *testing.T) {
	target := data.Int(42)
	var source *data.Int
	require.NoError(t, target.Scan(source))
	require.NoError(t, target.Scan(nil))
	require.Equal(t, data.Int(42), target)
	var invalidTarget *data.Int
	require.ErrorContains(t, invalidTarget.Scan(data.Int(1)), "cannot assign")
}

func TestFillStructHandlesShortArrayConversion(t *testing.T) {
	record, err := data.StructFromGoStruct(struct {
		Values []int
		Null   *int
	}{Values: []int{1}})
	require.NoError(t, err)
	require.Contains(t, record.Sql(), "'Null', null")
	var tooShort struct {
		Values [2]data.Data
		Null   int
	}
	require.NotPanics(t, func() {
		require.NoError(t, record.FillGoStruct(&tooShort))
	})
	require.Equal(t, [2]data.Data{}, tooShort.Values)
	var exact struct{ Values [1]data.Data }
	require.NoError(t, record.FillGoStruct(&exact))
	require.Equal(t, [1]data.Data{data.Int(1)}, exact.Values)
}

func TestIsDataEqualHandlesTypedNilMapsAndDifferentEntries(t *testing.T) {
	var nilMap *data.Map
	first, second := data.NewMap(), data.NewMap()
	require.True(t, data.IsDataEqual(nilMap, nilMap))
	require.False(t, data.IsDataEqual(nilMap, first))
	require.False(t, data.IsDataEqual(first, nilMap))
	require.False(t, data.IsDataEqual(data.Int(1), first))
	require.False(t, data.IsDataEqual(first, data.Int(1)))
	require.NoError(t, first.Set("key", 1))
	require.False(t, data.IsDataEqual(first, second))
	require.NoError(t, second.Set("other key", 1))
	require.False(t, data.IsDataEqual(first, second))
	third := data.NewMapWithType(datatype.NewMapType(datatype.StringType, datatype.IntType))
	require.NoError(t, third.SafeSet(data.String("key"), data.Int(2)))
	require.False(t, data.IsDataEqual(first, third))
	require.NoError(t, third.SafeSet(data.String("key"), data.Int(1)))
	require.True(t, data.IsDataEqual(first, third))
}

func TestDataConversionPropagatesNestedErrors(t *testing.T) {
	key := []byte("non-comparable MaxCompute key")
	_, err := data.TryConvertGoToOdpsData(map[*[]byte]int{&key: 1})
	require.ErrorContains(t, err, "not comparable")

	source, err := data.StructFromGoStruct(struct {
		Nested struct{ Inner struct{ Value int } }
	}{})
	require.NoError(t, err)
	var destination struct{ Nested struct{ Inner string } }
	require.ErrorContains(t, source.FillGoStruct(&destination), "field Inner is not a struct")
}
