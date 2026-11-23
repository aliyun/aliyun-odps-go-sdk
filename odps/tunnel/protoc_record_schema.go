// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func wireValue(value data.Data) data.Data {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		if base, ok := rv.Elem().Interface().(data.Data); ok {
			value = base
		}
	}
	if _, isNull := value.(data.NullData); isNull {
		return nil
	}
	return value
}

func integerRange(t datatype.TypeID) (int64, int64, bool) {
	switch t {
	case datatype.TINYINT:
		return math.MinInt8, math.MaxInt8, true
	case datatype.SMALLINT:
		return math.MinInt16, math.MaxInt16, true
	case datatype.INT, datatype.IntervalYearMonth:
		return math.MinInt32, math.MaxInt32, true
	case datatype.BIGINT:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

func checkInteger(t datatype.TypeID, n int64) error {
	lo, hi, ok := integerRange(t)
	if !ok || t == datatype.IntervalYearMonth {
		return fmt.Errorf("expected %s, got an integer", t)
	}
	if n < lo || n > hi {
		return fmt.Errorf("integer out of range for %s", t)
	}
	return nil
}

func validateRecordValue(t datatype.DataType, value data.Data) error {
	if t == nil {
		return fmt.Errorf("missing data type in schema")
	}

	value = wireValue(value)
	switch v := value.(type) {
	case nil:
		return nil
	case data.BigInt:
		return checkInteger(t.ID(), int64(v))
	case data.Int:
		return checkInteger(t.ID(), int64(v))
	case data.SmallInt:
		return checkInteger(t.ID(), int64(v))
	case data.TinyInt:
		return checkInteger(t.ID(), int64(v))
	case data.Array:
		expected, ok := t.(datatype.ArrayType)
		if !ok {
			return fmt.Errorf("expected %s, got ARRAY", t.ID())
		}
		for i, item := range v.ToSlice() {
			if err := validateRecordValue(expected.ElementType, item); err != nil {
				return fmt.Errorf("array element %d: %w", i, err)
			}
		}
		return nil
	case data.Map:
		expected, ok := t.(datatype.MapType)
		if !ok {
			return fmt.Errorf("expected %s, got MAP", t.ID())
		}
		for key, value := range v.ToGoMap() {
			if err := validateRecordValue(expected.KeyType, key); err != nil {
				return fmt.Errorf("map key: %w", err)
			}
			if err := validateRecordValue(expected.ValueType, value); err != nil {
				return fmt.Errorf("map value: %w", err)
			}
		}
		return nil
	case data.Struct:
		expected, ok := t.(datatype.StructType)
		if !ok {
			return fmt.Errorf("expected %s, got STRUCT", t.ID())
		}
		for _, field := range v.Fields() {
			ft := expected.FieldType(field.Name)
			if ft == nil {
				return fmt.Errorf("unexpected struct field %q", field.Name)
			}
			if err := validateRecordValue(ft, field.Value); err != nil {
				return fmt.Errorf("struct field %q: %w", field.Name, err)
			}
		}
		return nil
	case data.Char:
		expected, ok := t.(datatype.CharType)
		if !ok {
			return fmt.Errorf("expected %s, got CHAR", t.ID())
		}
		return checkCharacterWidth(v.Data(), expected.Length)
	case data.VarChar:
		expected, ok := t.(datatype.VarcharType)
		if !ok {
			return fmt.Errorf("expected %s, got VARCHAR", t.ID())
		}
		return checkCharacterWidth(v.Data(), expected.Length)
	case data.Decimal:
		expected, ok := t.(datatype.DecimalType)
		if !ok {
			return fmt.Errorf("expected %s, got DECIMAL", t.ID())
		}
		return checkDecimalWidth(v.String(), expected)
	case data.Double, data.Float, data.Bool, data.String, data.Binary, data.Geography,
		data.DateTime, data.Date, data.IntervalDayTime, data.IntervalYearMonth, data.Timestamp,
		data.TimestampNtz, data.Json:
		if t.ID() != v.Type().ID() {
			return fmt.Errorf("expected %s, got %s", t.ID(), v.Type().ID())
		}
		return nil
	default:
		return fmt.Errorf("unsupported Protobuf value type %T", value)
	}
}

func checkCharacterWidth(s string, width int) error {
	if !utf8.ValidString(s) || width < 1 || utf8.RuneCountInString(s) > width {
		return fmt.Errorf("invalid UTF-8 or character width exceeds %d", width)
	}
	return nil
}

func checkDecimalWidth(value string, t datatype.DecimalType) error {
	unsigned := strings.TrimPrefix(strings.TrimPrefix(value, "-"), "+")
	whole, fraction, hasPoint := strings.Cut(unsigned, ".")
	for _, digit := range whole + fraction {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("expected fixed-point %s value", t)
		}
	}
	if whole == "" || hasPoint && fraction == "" {
		return fmt.Errorf("expected fixed-point %s value", t)
	}
	if len(strings.TrimLeft(whole, "0")) > int(t.Precision-t.Scale) || len(fraction) > int(t.Scale) {
		return fmt.Errorf("value exceeds %s precision or scale", t)
	}
	return nil
}
