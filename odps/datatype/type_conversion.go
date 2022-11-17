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
