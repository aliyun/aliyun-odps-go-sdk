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
	"maps"
	"math"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

type Map struct {
	typ   datatype.MapType
	data  map[Data]Data
	Valid bool
}

func NewMap() *Map {
	return &Map{
		typ:  datatype.MapType{},
		data: make(map[Data]Data),
	}
}

func NewMapWithType(typ datatype.MapType) *Map {
	return &Map{
		typ:   typ,
		data:  make(map[Data]Data),
		Valid: true,
	}
}

func (m Map) Type() datatype.DataType {
	return m.typ
}

func (m Map) String() string {
	return m.Sql()
}

func (m Map) Sql() string {
	i, n := 0, len(m.data)
	if n == 0 {
		return "map()"
	}

	sb := strings.Builder{}
	sb.WriteString("map(")

	for key, value := range m.data {
		if key != nil {
			sb.WriteString(key.Sql())
		} else {
			sb.WriteString("null")
		}
		sb.WriteString(", ")
		if value != nil {
			sb.WriteString(value.Sql())
		} else {
			sb.WriteString("null")
		}

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

	return m.set(key, value)
}

func (m *Map) SafeSet(key, value Data) error {
	if m.typ.KeyType == nil || m.typ.ValueType == nil {
		return errors.New("element type of Map has not be set")
	}

	// NULL keys and values are legal in the wire format and carry no type to
	// check against, so only non-null entries are type checked.
	if key != nil && !datatype.IsTypeEqual(key.Type(), m.typ.KeyType) {
		return errors.Errorf("fail to set key of type %s to %s", key.Type(), m.typ)
	}

	if value != nil && !datatype.IsTypeEqual(value.Type(), m.typ.ValueType) {
		return errors.Errorf("fail to set value of type %s to %s", value.Type(), m.typ)
	}

	return m.set(key, value)
}

func comparableMapKey(key Data) error {
	if key != nil && !reflect.ValueOf(key).Comparable() {
		return errors.Errorf("map key of type %s is not comparable", key.Type())
	}
	return nil
}

func (m *Map) set(key, value Data) error {
	if err := comparableMapKey(key); err != nil {
		return err
	}
	if bits := nanKeyBits(key); bits != 0 {
		m.removeNaNKey(bits)
	}
	m.data[key] = value
	return nil
}

// NaNs are not equal to themselves in a Go map. Identify scalar NaN keys
// by type and payload so that setting or deleting the same key works.
func nanKeyBits(key Data) uint64 {
	switch v := key.(type) {
	case Float:
		if math.IsNaN(float64(v)) {
			return uint64(math.Float32bits(float32(v)))
		}
	case Double:
		if math.IsNaN(float64(v)) {
			return math.Float64bits(float64(v))
		}
	}
	return 0
}

func (m *Map) removeNaNKey(bits uint64) {
	// delete cannot remove even a NaN obtained by ranging over the map.
	// Refill the same map so copies of this Map keep sharing its entries.
	for key := range m.data {
		if nanKeyBits(key) == bits {
			entries := maps.Clone(m.data)
			clear(m.data)
			for k, v := range entries {
				if nanKeyBits(k) != bits {
					m.data[k] = v
				}
			}
			return
		}
	}
}

// Delete removes the key using the same Go-to-ODPS conversion as Set.
// Deleting a missing key succeeds.
func (m *Map) Delete(key any) error {
	k, err := TryConvertGoToOdpsData(key)
	if err != nil {
		return err
	}
	if err := comparableMapKey(k); err != nil {
		return err
	}
	if bits := nanKeyBits(k); bits != 0 {
		m.removeNaNKey(bits)
	} else {
		delete(m.data, k)
	}
	return nil
}

func (m *Map) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, m))
}

func (m *Map) TypeInfer() (datatype.DataType, error) {
	if len(m.data) == 0 {
		return nil, errors.Errorf("cannot infer type for empty map")
	}

	// NULL keys and values carry no type information, the types are inferred
	// from the entries that do have a value.
	var keyT, valueT datatype.DataType

	for key, value := range m.data {
		if key != nil {
			if keyT == nil {
				keyT = key.Type()
			} else if !datatype.IsTypeEqual(keyT, key.Type()) {
				return nil, errors.Errorf("key type is not the same in array, find %s, %s types", keyT, key.Type())
			}
		}

		if value != nil {
			if valueT == nil {
				valueT = value.Type()
			} else if !datatype.IsTypeEqual(valueT, value.Type()) {
				return nil, errors.Errorf("value type is not the same in array, find %s, %s types", valueT, value.Type())
			}
		}
	}

	if keyT == nil || valueT == nil {
		return nil, errors.New("cannot infer type for a map whose keys or values are all null")
	}

	return datatype.NewMapType(keyT, valueT), nil
}

func MapFromGoMap(m interface{}) (*Map, error) {
	mt := reflect.TypeOf(m)
	if mt == nil || mt.Kind() != reflect.Map {
		return nil, errors.Errorf("%T is not a map", m)
	}

	mm, err := TryConvertGoToOdpsData(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, _ := mm.(*Map)
	return ret, nil
}

// ToGoMap returns a shallow copy of the entries.
func (m *Map) ToGoMap() map[Data]Data {
	return maps.Clone(m.data)
}
