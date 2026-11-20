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

// 主要用于将go的array|slice, map, struct, 转换为Array, Map, Struct类型

func TryConvertGoToOdpsData(i interface{}) (Data, error) {
	if d, ok := i.(Data); ok {
		return d, nil
	}

	value := reflect.Indirect(reflect.ValueOf(i))
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
		return Binary(value.Bytes()), nil
	}

	return tryConvertGoToOdpsData(value)
}

func tryConvertGoToOdpsData(v reflect.Value) (Data, error) {
	switch v.Kind() {
	case reflect.Bool:
		return Bool(v.Bool()), nil
	case reflect.Int8:
		return TinyInt(v.Int()), nil
	case reflect.Int, reflect.Int32:
		return Int(v.Int()), nil
	case reflect.Int16:
		return SmallInt(v.Int()), nil
	case reflect.Int64:
		return BigInt(v.Int()), nil
	case reflect.Float32:
		return Float(v.Float()), nil
	case reflect.Float64:
		return Double(v.Float()), nil
	case reflect.String:
		return String(v.String()), nil
	case reflect.Array, reflect.Slice:
		arr := NewArray()
		for i := 0; i < v.Len(); i++ {
			elem, err := TryConvertGoToOdpsData(v.Index(i).Interface())
			if err != nil {
				return nil, err
			}

			arr.UnSafeAppend(elem)
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

			if err := m.set(key, value); err != nil {
				return nil, err
			}
		}

		return m, nil
	case reflect.Struct:
		s := NewStruct()
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			value := v.Field(i)
			if !value.CanInterface() {
				continue
			}
			fieldValue, err := TryConvertGoToOdpsData(value.Interface())
			if err != nil {
				return nil, err
			}

			field := t.Field(i)
			fieldName := field.Tag.Get("odps")
			if fieldName == "" {
				fieldName = field.Name
			}
			s.setField(fieldName, fieldValue)
		}

		return s, nil
	}

	return nil, errors.Errorf("Cannot convert data of type %s to odps data", v.Type().Name())
}
