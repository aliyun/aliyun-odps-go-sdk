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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
	"reflect"
	"strings"
)

type Map struct {
	typ  *datatype.MapType
	data map[Data]Data
}

func NewMap() *Map {
	return &Map{
		typ:  nil,
		data: make(map[Data]Data),
	}
}

func NewMapWithType(typ *datatype.MapType) *Map {
	return &Map{
		typ:  typ,
		data: make(map[Data]Data),
	}
}

func (m *Map) Type() datatype.DataType {
	return *m.typ
}

func (m *Map) String() string {
	i, n := 0, len(m.data)
	if n == 0 {
		return "map()"
	}

	sb := strings.Builder{}

	for key, value := range m.data {
		sb.WriteString(key.String())
		sb.WriteString(", ")
		sb.WriteString(value.String())

		i += 1
		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")
	return sb.String()
}

func (m *Map) Sql() string {
	i, n := 0, len(m.data)
	if n == 0 {
		return "map()"
	}

	sb := strings.Builder{}

	for key, value := range m.data {
		sb.WriteString(key.Sql())
		sb.WriteString(", ")
		sb.WriteString(value.Sql())

		i += 1
		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")
	return sb.String()
}

func (m *Map) Set(keyI interface{}, valueI interface{}) error {
	key, err := TryConvertGoToOdpsData(keyI)
	if err != nil {
		return errors.WithStack(err)
	}

	value, err := TryConvertGoToOdpsData(valueI)
	if err != nil {
		return errors.WithStack(err)
	}

	m.data[key] = value
	return nil
}

func (m *Map) SafeSet(keyI Data, valueI Data) error {
	if m.typ == nil {
		return errors.New("element type of Map has not be set")
	}

	key, err := TryConvertGoToOdpsData(keyI)
	if err != nil {
		return errors.WithStack(err)
	}

	value, err := TryConvertGoToOdpsData(valueI)
	if err != nil {
		return errors.WithStack(err)
	}

	if !datatype.IsTypeEqual(key.Type(), m.typ.KeyType) {
		return errors.Errorf("fail to set key of type %s to %s", key.Type(), *m.typ)
	}

	if !datatype.IsTypeEqual(value.Type(), m.typ.ValueType) {
		return errors.Errorf("fail to set key of type %s to %s", value.Type(), *m.typ)
	}

	m.data[key] = value
	return nil
}

func (m *Map) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, m))
}

func (m *Map) TypeInfer() (datatype.DataType, error) {
	if len(m.data) == 0 {
		return nil, errors.Errorf("cannot infer type for empty map")
	}

	i := 0
	var keyT, valueT datatype.DataType

	for key, value := range m.data {
		if i == 0 {
			keyT = key.Type()
			valueT = value.Type()
			continue
		}

		if !datatype.IsTypeEqual(keyT, key.Type()) {
			return nil, errors.Errorf("key type is not the same in array, find %s, %s types", keyT, key.Type())
		}

		if !datatype.IsTypeEqual(valueT, value.Type()) {
			return nil, errors.Errorf("value type is not the same in array, find %s, %s types", valueT, value.Type())
		}

		i += 1
	}

	return datatype.NewMapType(keyT, valueT), nil
}

func MapFromGoMap(m interface{}) (*Map, error) {
	mt := reflect.TypeOf(m)
	if mt.Kind() != reflect.Map {
		return nil, errors.Errorf("%s is not a map", mt.Name())
	}

	mm, err := TryConvertGoToOdpsData(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, _ := mm.(*Map)
	return ret, nil
}

func (m *Map) ToGoMap() map[Data]Data {
	return m.data
}
