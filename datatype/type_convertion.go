package datatype

import (
	"github.com/pkg/errors"
	"reflect"
)

var OdpsDataType = reflect.TypeOf((*DataType)(nil)).Elem()

func IsOdpsDataType(t reflect.Type) bool {
	return t.Implements(OdpsDataType) ||
		t.AssignableTo(OdpsDataType) ||
		t.ConvertibleTo(OdpsDataType)
}

var CannotConvertToOdpsDataTypeError = errors.New("type is not one of bool, int8, int32, int16, " +
	"int64, float32, float64, string, Data, or map, slice, struct that made up by these types")

func TryConvertToOdpsType(t reflect.Type) (DataType, error) {
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
	case reflect.Slice:
		return tryConvertToArrayType(t)
	case reflect.Map:
		return tryConvertToMapType(t)
	case reflect.Struct:
		return tryConvertToStructType(t)
	}

	return nil, CannotConvertToOdpsDataTypeError
}

func tryConvertToArrayType(t reflect.Type) (DataType, error) {
	elementType, err := TryConvertToOdpsType(t.Elem())
	if err != nil {
		return nil, err
	}

	return NewArrayType(elementType), nil
}

func tryConvertToMapType(t reflect.Type) (DataType, error) {
	keyType, err := TryConvertToOdpsType(t.Key())
	if err != nil {
		return nil, err
	}

	valueType, err := TryConvertToOdpsType(t.Elem())
	if err != nil {
		return nil, err
	}

	return NewMapType(keyType, valueType), nil
}

func tryConvertToStructType(t reflect.Type) (DataType, error) {
	structFields := make([]StructFieldType, t.NumField())

	for i, n := 0, t.NumField(); i < n; i++ {
		field := t.Field(i)
		fieldType, err := TryConvertToOdpsType(field.Type)

		fieldName := field.Tag.Get("odps")
		if fieldName == "" {
			fieldName = field.Name
		}

		if err != nil {
			return nil, err
		}

		structFields[i] = StructFieldType{Name: fieldName, Type: fieldType}
	}

	return NewStructType(structFields...), nil
}
