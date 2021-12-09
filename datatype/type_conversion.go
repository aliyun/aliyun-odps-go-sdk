package datatype

import (
	"github.com/pkg/errors"
	"reflect"
	"strings"
)

var odpsDataType = reflect.TypeOf((*DataType)(nil)).Elem()

func TryConvertGoToOdpsType(i interface{}) (DataType, error) {
	t, ok := i.(reflect.Type)
	if !ok {
		tt := reflect.TypeOf(i)
		if tt.Implements(odpsDataType) {
			return i.(DataType), nil
		}

		return nil, errors.Errorf("cannot convert %s to odps type", tt.Name())
	}

	if t.Implements(odpsDataType) {
		switch strings.ToLower(t.Name()) {
		case "char", "varchar", "decimal", "array", "map", "struct":
			return nil, errors.Errorf("char, varchar, decimal need extra parameter to specific concrete type")
		}
	}

	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return BinaryType, nil
	}

	switch t.Kind() {
	case reflect.Bool:
		return BooleanType, nil
	case reflect.Int8:
		return TinyIntType, nil
	case reflect.Int32:
		return IntType, nil
	case reflect.Int16:
		return SmallIntType, nil
	case reflect.Int64:
		return BigIntType, nil
	case reflect.Float32:
		return FloatType, nil
	case reflect.Float64:
		return DoubleType, nil
	case reflect.String:
		return StringType, nil
	case reflect.Array, reflect.Slice:
		elemType, err := TryConvertGoToOdpsType(t.Elem())
		if err != nil {
			return nil, err
		}

		return NewArrayType(elemType), nil
	case reflect.Map:
		keyType, err := TryConvertGoToOdpsType(t.Key())
		if err != nil {
			return nil, err
		}

		valueType, err := TryConvertGoToOdpsType(t.Elem())
		if err != nil {
			return nil, err
		}

		return NewMapType(keyType, valueType), nil
	case reflect.Struct:
		n := t.NumField()
		fields := make([]StructFieldType, n)

		for i := 0; i < n; i++ {
			field := t.Field(i)

			fieldName := field.Tag.Get("odps")
			if fieldName == "" {
				fieldName = field.Name
			}

			fieldType, err := TryConvertGoToOdpsType(field.Type)
			if err != nil {
				return nil, err
			}

			fields[i] = NewStructFieldType(fieldName, fieldType)
		}

		return NewStructType(fields...), nil
	}

	return nil, errors.Errorf("Cannot convert %s to odps type", t.Name())
}
