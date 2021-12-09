package data

import (
	"errors"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"reflect"
)

var OdpsDataType = reflect.TypeOf((*Data)(nil)).Elem()

func IsOdpsData(t reflect.Type) bool {
	return t.Implements(OdpsDataType) ||
		t.AssignableTo(OdpsDataType) ||
		t.ConvertibleTo(OdpsDataType)
}

var CannotConvertToOdpsDataError = errors.New("type is not one of bool, int8, int32, int16, " +
	"int64, float32, float64, string, Data, or map, slice, struct that made up by these types")

func TryConvertToOdps(i interface{}) (Data, error) {
	return tryConvertToOdps(reflect.ValueOf(i))
}

func tryConvertToOdps(v reflect.Value) (Data, error) {
	switch v.Kind() {
	case reflect.Bool:
		return Bool(v.Interface().(bool)), nil
	case reflect.Int8:
		return TinyInt(v.Interface().(int8)), nil
	case reflect.Int32:
		return Int(v.Interface().(int32)), nil
	case reflect.Int16:
		return SmallInt(v.Interface().(int16)), nil
	case reflect.Int64:
		return BigInt(v.Interface().(int64)), nil
	case reflect.Float32:
		return Float(v.Interface().(float32)), nil
	case reflect.Float64:
		return Double(v.Interface().(float64)), nil
	case reflect.String:
		s := String(v.Interface().(string))
		return &s, nil
	case reflect.Slice:
		return tryConvertToArray(v)
	case reflect.Map:
		return tryConvertToMap(v)
	case reflect.Struct:
		if IsOdpsData(v.Type()) {
			return v.Interface().(Data), nil
		}

		return tryConvertToStruct(v)
	case reflect.Interface:
		if IsOdpsData(v.Type()) {
			return v.Interface().(Data), nil
		}
	case reflect.Ptr:
		if IsOdpsData(v.Type()) {
			return v.Interface().(Data), nil
		}

		return tryConvertToOdps(v.Elem())
	}

	return nil, CannotConvertToOdpsDataError
}

func tryConvertToArray(v reflect.Value) (Data, error) {
	if v.Len() == 0 {
		return Null, nil
	}
	dataOfArray := make([]Data, v.Len())
	elem0, err := tryConvertToOdps(v.Index(0))
	if err != nil {
		return nil, err
	}
	dataOfArray[0] = elem0
	at := datatype.NewArrayType(elem0.Type())

	for i, n := 1, v.Len(); i < n; i++ {
		elem, err := tryConvertToOdps(v.Index(i))
		if err != nil {
			return nil, err
		}

		dataOfArray[i] = elem
	}

	arr := NewArray(at)
	err = arr.AddValue(dataOfArray...)
	if err != nil {
		return nil, err
	}

	return arr, nil
}

func tryConvertToMap(v reflect.Value) (Data, error) {
	if v.Len() == 0 {
		return Null, nil
	}

	key := v.MapKeys()[0]
	keyData, err := tryConvertToOdps(key)
	if err != nil {
		return nil, err
	}
	valueData, err := tryConvertToOdps(v.MapIndex(key))
	if err != nil {
		return nil, err
	}

	mt := datatype.NewMapType(keyData.Type(), valueData.Type())
	m := NewMap(mt)

	for i, n := 1, v.Len(); i < n; i++ {
		keyData, err := tryConvertToOdps(key)
		if err != nil {
			return nil, err
		}

		valueData, err := tryConvertToOdps(v.MapIndex(key))
		if err != nil {
			return nil, err
		}

		err = m.Set(keyData, valueData)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

func tryConvertToStruct(v reflect.Value) (Data, error) {
	fields := make([]datatype.StructFieldType, v.NumField())
	structData := Struct{
		data: make(map[string]Data, v.NumField()),
	}

	t := v.Type()

	if t.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i, n := 0, v.NumField(); i < n; i++ {
		fv := v.Field(i)
		f := t.Field(i)

		fieldName := f.Tag.Get("odps")
		if fieldName == "" {
			fieldName = f.Name
		}

		fieldData, err := tryConvertToOdps(fv)
		if err != nil {
			return nil, err
		}

		fields[i] = datatype.NewStructFieldType(fieldName, fieldData.Type())
		structData.data[fieldName] = fieldData
	}

	structData._type = datatype.NewStructType(fields...)
	return &structData, nil
}
