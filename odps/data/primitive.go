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
	"encoding/json"
	"fmt"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/pkg/errors"
)

type Bool bool
type TinyInt int8
type Int int32
type SmallInt int16
type BigInt int64
type Float float32
type Double float64
type String string
type Object map[string]interface{}
type Slice []interface{}

func (b Bool) Type() datatype.DataType {
	return datatype.BooleanType
}

func (b Bool) String() string {
	return fmt.Sprintf("%t", bool(b))
}

func (b Bool) Sql() string {
	return fmt.Sprintf("%t", bool(b))
}

func (b *Bool) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, b))
}

func (t TinyInt) Type() datatype.DataType {
	return datatype.TinyIntType
}

func (t TinyInt) String() string {
	return fmt.Sprintf("%d", t)
}

func (t TinyInt) Sql() string {
	return fmt.Sprintf("%dY", t)
}

func (t *TinyInt) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, t))
}

func (s SmallInt) Type() datatype.DataType {
	return datatype.SmallIntType
}

func (s SmallInt) String() string {
	return fmt.Sprintf("%d", s)
}

func (s SmallInt) Sql() string {
	return fmt.Sprintf("%ds", s)
}

func (i Int) Type() datatype.DataType {
	return datatype.IntType
}

func (i Int) String() string {
	return fmt.Sprintf("%d", i)
}

func (i Int) Sql() string {
	return fmt.Sprintf("%d", i)
}

func (i *Int) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, i))
}

func (b BigInt) Type() datatype.DataType {
	return datatype.BigIntType
}

func (b BigInt) String() string {
	return fmt.Sprintf("%d", b)
}

func (b BigInt) Sql() string {
	return fmt.Sprintf("%dL", b)
}

func (b *BigInt) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, b))
}

func (f Float) Type() datatype.DataType {
	return datatype.FloatType
}

func (f Float) String() string {
	return fmt.Sprintf("%E", float32(f))
}

func (f Float) Sql() string {
	return fmt.Sprintf("cast(%E as float)", float32(f))
}

func (f *Float) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, f))
}

func (d Double) Type() datatype.DataType {
	return datatype.DoubleType
}

func (d Double) String() string {
	return fmt.Sprintf("%E", float64(d))
}

func (d Double) Sql() string {
	return fmt.Sprintf("%E", float64(d))
}

func (d *Double) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, d))
}

func (s String) Type() datatype.DataType {
	return datatype.StringType
}

func (s String) String() string {
	return string(s)
}

func (s String) Sql() string {
	return fmt.Sprintf("'%s'", s)
}

func (s *String) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, s))
}

func (o Object) Type() datatype.DataType {
	return datatype.ObjectType
}

func (o Object) String() string {
	str, err := json.Marshal(o)
	if err != nil {
		return ""
	}
	return string(str)
}

func (o Object) Sql() string {
	return o.String()
}

func (o *Object) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, o))
}

func (s Slice) Type() datatype.DataType {
	return datatype.SliceType
}

func (s Slice) String() string {
	str, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(str)
}

func (s Slice) Sql() string {
	return s.String()
}

func (s *Slice) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, s))
}
