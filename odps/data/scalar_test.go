// Licensed under the Apache License, Version 2.0.

package data_test

import (
	"database/sql"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestScaledDecimalFormatting(t *testing.T) {
	for _, test := range []struct {
		unscaled int64
		scale    int
		want     string
	}{
		{12, 0, "12"}, {-12, 0, "-12"}, {12345, 2, "123.45"},
		{-12345, 2, "-123.45"}, {123, 3, "0.123"}, {-123, 3, "-0.123"}, {0, 4, "0.0000"},
	} {
		decimal := data.NewDecimalFromValue(12, test.scale, big.NewInt(test.unscaled))
		require.Equal(t, test.want, decimal.String())
		require.Equal(t, test.want, decimal.Value())
		require.Equal(t, test.want+"BD", decimal.Sql())
		require.Equal(t, 12, decimal.Precision())
		require.Equal(t, test.scale, decimal.Scale())
		var scanned data.Decimal
		require.NoError(t, scanned.Scan(decimal))
		require.Equal(t, test.want, scanned.String())
	}
	for _, invalid := range []string{"", "x.3", "3.x", "1.2.3.4"} {
		_, err := data.DecimalFromStr(invalid)
		require.ErrorIs(t, err, data.InvalidDecimalErr)
	}
}

func TestTemporalValueParsingAndScan(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  string
		typ    datatype.DataType
		parse  func(string) (data.Data, error)
		target sql.Scanner
	}{
		{"date", "2024-02-29", datatype.DateType, func(s string) (data.Data, error) { return data.NewDate(s) }, new(data.Date)},
		{"datetime", "2024-02-29 19:30:00", datatype.DateTimeType, func(s string) (data.Data, error) { return data.NewDateTime(s) }, new(data.DateTime)},
		{"timestamp", "2024-02-29 19:30:00.123", datatype.TimestampType, func(s string) (data.Data, error) { return data.NewTimestamp(s) }, new(data.Timestamp)},
		{"timestamp_ntz", "2024-02-29 19:30:00.456", datatype.TimestampNtzType, func(s string) (data.Data, error) { return data.NewTimestampNtz(s) }, new(data.TimestampNtz)},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := test.parse(test.value)
			require.NoError(t, err)
			require.Equal(t, test.typ, value.Type())
			require.Equal(t, test.value, value.String())
			require.Equal(t, test.name+"'"+test.value+"'", value.Sql())
			require.NoError(t, test.target.Scan(value))
			require.Equal(t, value.Sql(), test.target.(data.Data).Sql())
			require.ErrorContains(t, test.target.Scan(int64(0)), "cannot convert")
			_, err = test.parse("2023-02-29 invalid timestamp")
			require.Error(t, err)
		})
	}
}

func TestCharacterLengthsAndScan(t *testing.T) {
	for _, construct := range []struct {
		max  int
		make func(int, string) (data.Data, error)
	}{
		{255, func(n int, s string) (data.Data, error) { return data.NewChar(n, s) }},
		{255, func(n int, s string) (data.Data, error) { return data.MakeChar(n, s) }},
		{65536, func(n int, s string) (data.Data, error) { return data.NewVarChar(n, s) }},
		{65536, func(n int, s string) (data.Data, error) { return data.MakeVarChar(n, s) }},
	} {
		value, err := construct.make(3, "abc")
		require.NoError(t, err)
		require.Equal(t, "abc", value.String())
		_, err = construct.make(2, "abc")
		var overflow data.CharOverflowError
		require.ErrorAs(t, err, &overflow)
		require.ErrorContains(t, err, "bigger than the max length 2")
		_, err = construct.make(construct.max+1, "")
		require.ErrorContains(t, err, "max length")
	}
	charValue, err := data.MakeChar(3, "abc")
	require.NoError(t, err)
	var char data.Char
	require.NoError(t, char.Scan(charValue))
	require.Equal(t, "cast('abc' as char(3))", char.Sql())
	varcharValue, err := data.MakeVarChar(4, "abc")
	require.NoError(t, err)
	var varchar data.VarChar
	require.NoError(t, varchar.Scan(varcharValue))
	require.Equal(t, "cast('abc' as varchar(4))", varchar.Sql())
}

func TestJSONAndIntervalValues(t *testing.T) {
	encoded, err := data.NewJson(map[string]any{"name": "fixture"})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"fixture"}`, encoded.String())
	var decoded data.Json
	require.NoError(t, decoded.Scan(encoded))
	require.Equal(t, encoded.Sql(), decoded.Sql())
	_, err = data.NewJson(make(chan int))
	require.ErrorContains(t, err, "unsupported type")

	interval := data.NewIntervalDayTime(25*60*60+70, int32(1500*time.Millisecond))
	require.EqualValues(t, 1, interval.Days())
	require.EqualValues(t, 25, interval.Hours())
	require.EqualValues(t, 1501, interval.Minutes())
	require.EqualValues(t, 3671, interval.SecondsFraction())
	require.EqualValues(t, 3671500, interval.MillisecondsFraction())
	var elapsed data.IntervalDayTime
	require.NoError(t, elapsed.Scan(interval))
	require.Equal(t, interval, elapsed)
	var months data.IntervalYearMonth
	require.NoError(t, months.Scan(data.IntervalYearMonth(-13)))
	require.Equal(t, "-13 months", months.Sql())
}
