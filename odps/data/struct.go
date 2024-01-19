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
	"strings"
	"sync"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type StructField struct {
	Name  string
	Value Data
}

func NewStructField(name string, value Data) StructField {
	return StructField{
		Name:  name,
		Value: value,
	}
}

// Struct 这里用slice而不用map，是要保持Field顺序
type Struct struct {
	typ          datatype.StructType
	fields       []StructField
	fieldIndexes map[string]int
	Valid        bool
}

func NewStruct() *Struct {
	return &Struct{
		typ:          datatype.StructType{},
		fields:       make([]StructField, 0),
		fieldIndexes: make(map[string]int),
	}
}

func NewStructWithTyp(typ datatype.StructType) *Struct {
	return &Struct{
		typ:          typ,
		fields:       make([]StructField, 0),
		fieldIndexes: make(map[string]int),
		Valid:        true,
	}
}

func (s Struct) Type() datatype.DataType {
	return s.typ
}

func (s Struct) String() string {
	var sb strings.Builder
	sb.WriteString("struct<")
	n := len(s.fields) - 1
	for i, field := range s.fields {
		sb.WriteString(field.Name)
		sb.WriteString(":")
		sb.WriteString(field.Value.String())

		if i < n {
			sb.WriteString(",")
		}
	}

	sb.WriteString(">")

	return sb.String()
}

func (s Struct) Sql() string {
	var sb strings.Builder
	sb.WriteString("named_struct(")
	n := len(s.fields) - 1

	for i, field := range s.fields {
		sb.WriteString("'")
		sb.WriteString(field.Name)
		sb.WriteString("'")
		sb.WriteString(", ")
		sb.WriteString(field.Value.Sql())

		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")

	return sb.String()
}

func (s *Struct) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, s))
}

func (s *Struct) Fields() []StructField {
	return s.fields
}

func (s *Struct) GetField(fieldName string) Data {
	i, ok := s.fieldIndexes[fieldName]
	if !ok {
		return nil
	}

	return s.fields[i].Value
}

func (s *Struct) SetField(fieldName string, a interface{}) error {
	d, err := TryConvertGoToOdpsData(a)
	if err != nil {
		return errors.WithStack(err)
	}

	i, ok := s.fieldIndexes[fieldName]

	if !ok {
		m := sync.Mutex{}
		m.Lock()
		s.fields = append(s.fields, NewStructField(fieldName, d))
		s.fieldIndexes[fieldName] = len(s.fields) - 1
		m.Unlock()
	} else {
		s.fields[i] = NewStructField(fieldName, d)
	}

	return nil
}

func (s *Struct) SafeSetField(fieldName string, i interface{}) error {
	if s.typ.Fields == nil {
		return errors.New("type of Struct has not be set")
	}

	d, err := TryConvertGoToOdpsData(i)
	if err != nil {
		return errors.WithStack(err)
	}

	var fieldType datatype.DataType
	for _, f := range s.typ.Fields {
		if f.Name == fieldName {
			fieldType = f.Type
			break
		}
	}

	if fieldType == nil {
		return errors.Errorf("cannot set %s to %s", fieldName, s.typ)
	}

	if !datatype.IsTypeEqual(fieldType, d.Type()) {
		return errors.Errorf("cannot set type %s to %s of %s", d.Type(), fieldName, s.typ)
	}

	_ = s.SetField(fieldName, d)

	return nil
}

func (s *Struct) TypeInfer() (datatype.DataType, error) {
	if len(s.fields) == 0 {
		return nil, errors.New("cannot infer type for empty struct")
	}

	fieldTypes := make([]datatype.StructFieldType, len(s.fields))
	for i, field := range s.fields {
		fieldTypes[i] = datatype.NewStructFieldType(field.Name, field.Value.Type())
	}

	return datatype.NewStructType(fieldTypes...), nil
}

func StructFromGoStruct(i interface{}) (*Struct, error) {
	it := reflect.TypeOf(i)
	if it.Kind() != reflect.Struct {
		return nil, errors.Errorf("%s is not a struct", it.Name())
	}

	s, err := TryConvertGoToOdpsData(i)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, _ := s.(*Struct)
	return ret, nil
}

func (s *Struct) FillGoStruct(i interface{}) error {
	it := reflect.TypeOf(i)
	if it.Kind() != reflect.Ptr {
		return errors.Errorf("%s is not a pointer", it.Name())
	}

	it = it.Elem()

	if it.Kind() != reflect.Struct {
		return errors.Errorf("%s is not a struct", it.Name())
	}

	iv := reflect.ValueOf(i).Elem()

	for j, n := 0, it.NumField(); j < n; j++ {
		field := it.Field(j)
		fieldName := field.Tag.Get("odps")

		if fieldName == "" {
			fieldName = field.Name
		}

		data := s.GetField(fieldName)
		if data == nil {
			continue
		}

		fv := iv.Field(j)

		var goData interface{}
		switch dt := data.(type) {
		case *Struct:
			return dt.FillGoStruct(fv)
		case *Array:
			goData = dt.ToSlice()
		case *Map:
			goData = dt.ToGoMap()
		case *String:
			goData = *(*string)(dt)
		default:
			goData = dt
		}

		goDataT := reflect.TypeOf(goData)

		if goDataT.AssignableTo(field.Type) {
			fv.Set(reflect.ValueOf(goData))
		}

		if goDataT.ConvertibleTo(field.Type) {
			fv.Set(reflect.ValueOf(goData).Convert(field.Type))
		}
	}

	return nil
}
