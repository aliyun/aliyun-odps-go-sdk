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
	"strings"
)

type Array struct {
	typ   datatype.ArrayType
	data  []Data
	Valid bool
}

func NewArray() *Array {
	return &Array{
		typ:  datatype.ArrayType{},
		data: make([]Data, 0),
	}
}

func NewArrayWithType(typ datatype.ArrayType) *Array {
	return &Array{
		typ:   typ,
		data:  make([]Data, 0),
		Valid: true,
	}
}

func (a Array) Type() datatype.DataType {
	return a.typ
}

func (a Array) String() string {
	n := len(a.data)

	if n == 0 {
		return "array()"
	}

	sb := strings.Builder{}
	sb.WriteString("array(")

	for i, d := range a.data {
		sb.WriteString(d.String())

		if i+1 < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")

	return sb.String()
}

func (a Array) Sql() string {
	n := len(a.data)

	if n == 0 {
		return "array()"
	}

	sb := strings.Builder{}
	sb.WriteString("array(")

	for i, d := range a.data {
		sb.WriteString(d.Sql())

		if i+1 < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")

	return sb.String()
}

func (a *Array) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, a))
}

func (a *Array) SetType(typ datatype.ArrayType) {
	a.typ = typ
}

func (a *Array) UnSafeAppend(data ...Data) {
	a.data = append(a.data, data...)
}

func (a *Array) Append(data ...interface{}) error {
	for _, d := range data {
		o, err := TryConvertGoToOdpsData(d)
		if err != nil {
			return errors.WithStack(err)
		}

		a.data = append(a.data, o)
	}

	return nil
}

func (a *Array) SafeAppend(data ...interface{}) error {
	if a.typ.ElementType == nil {
		return errors.New("element type of Array has not be set")
	}

	for _, d := range data {
		o, err := TryConvertGoToOdpsData(d)
		if err != nil {
			return errors.WithStack(err)
		}

		if !datatype.IsTypeEqual(o.Type(), a.typ.ElementType) {
			return errors.Errorf("expect %s element type for array, but get %s", a.typ.ElementType, o.Type())
		}

		a.data = append(a.data, o)
	}

	return nil
}

func (a *Array) Len() int {
	return len(a.data)
}

func (a *Array) Index(i int) Data {
	return a.data[i]
}

func (a *Array) TypeInfer() (datatype.DataType, error) {
	if len(a.data) == 0 {
		return nil, errors.Errorf("cannot infer type for empty array")
	}

	et := a.data[0].Type()
	for _, e := range a.data[1:] {
		if !datatype.IsTypeEqual(e.Type(), et) {
			return nil, errors.Errorf("element type is not the same in array, find %s, %s types", et, e.Type())
		}
	}

	return datatype.NewArrayType(et), nil
}

func ArrayFromSlice(data ...interface{}) (*Array, error) {
	a := NewArray()
	err := a.Append(data...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return a, nil
}

func (a *Array) ToSlice() []Data {
	return a.data
}
