// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package data

import (
	"reflect"

	"github.com/pkg/errors"
)

var odpsDataType = reflect.TypeOf((*Data)(nil)).Elem()

// 主要用于将go的array|slice, map, struct, 转换为Array, Map, Struct类型

func TryConvertGoToOdpsData(i interface{}) (Data, error) {
	if i == nil {
		return nil, nil
	}

	it := reflect.TypeOf(i)

	if it.Implements(odpsDataType) {
		return i.(Data), nil
	}

	if it.Kind() == reflect.Slice && it.Elem().Kind() == reflect.Uint8 {
		return Binary(i.([]byte)), nil
	}

	iv := reflect.ValueOf(i)
	if iv.Kind() == reflect.Ptr {
		iv = iv.Elem()
	}

	return tryConvertGoToOdpsData(iv)
}

func tryConvertGoToOdpsData(v reflect.Value) (Data, error) {
	switch v.Kind() {
	case reflect.Bool:
		return Bool(v.Interface().(bool)), nil
	case reflect.Int8:
		return TinyInt(v.Interface().(int8)), nil
	case reflect.Int:
		return Int(v.Interface().(int)), nil
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
		return s, nil
	case reflect.Array, reflect.Slice:
		arr := NewArray()
		for i, n := 0, v.Len(); i < n; i++ {
			elem, err := TryConvertGoToOdpsData(v.Index(i).Interface())
			if err != nil {
				return nil, err
			}

			arr.Append(elem)
		}

		return arr, nil
	case reflect.Map:
		m := NewMap()
		for _, k := range v.MapKeys() {
			key, err := TryConvertGoToOdpsData(k.Interface())
			if err != nil {
				return nil, err
			}

			value, err := TryConvertGoToOdpsData(v.MapIndex(k).Interface())
			if err != nil {
				return nil, err
			}

			m.Set(key, value)
		}

		return m, nil
	case reflect.Struct:
		s := NewStruct()
		t := v.Type()
		for i, n := 0, v.NumField(); i < n; i++ {
			fieldValue, err := TryConvertGoToOdpsData(v.Field(i).Interface())
			if err != nil {
				return nil, err
			}

			field := t.Field(i)
			fieldName := field.Tag.Get("odps")
			if fieldName == "" {
				fieldName = field.Name
			}
			s.SetField(fieldName, fieldValue)
		}

		return s, nil
	}

	return nil, errors.Errorf("Cannot convert data of type %s to odps data", v.Type().Name())
}
