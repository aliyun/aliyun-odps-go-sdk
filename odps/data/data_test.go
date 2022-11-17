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

//
//import (
//	"fmt"
//	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
//	"reflect"
//	"testing"
//)
//
//
//// []Decimal
//// []Struct
//
//func TestTryConvertToOdps(t *testing.T) {
//	type ComplexData struct {
//		Int int32
//		Slice []int64
//		Dec *Decimal
//		Str string
//		M map[int16]string
//	}
//
//	de, _ := DecimalFromStr("123.23")
//	m := make(map[int16]string)
//	m[int16(10)] = "10"
//
//	c := ComplexData{
//		Int: 100,
//		Slice: []int64{1, 2, 3},
//		Dec: de,
//		Str: "hello",
//		M: m,
//	}
//
//	d, err := TryConvertToOdps(c)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//
//	fmt.Println(d.Type().Name())
//
//	c1 := ComplexData{}
//	s, ok := d.(*Struct)
//	if !ok {
//		t.Fatal("fail to convert struct to Struct")
//	}
//
//	err = s.Decode(&c1)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	fmt.Printf("%v\n", c1)
//}
//
//func TestStruct(t *testing.T)  {
//	st := datatype.NewStructType(
//		datatype.NewStructFieldType("X", datatype.IntType),
//		datatype.NewStructFieldType("Y", datatype.IntType),
//		datatype.NewStructFieldType(
//			"Z",
//			datatype.NewStructType(
//				datatype.NewStructFieldType("A", datatype.StringType),
//			),
//		),
//	)
//
//	s := NewStruct(st)
//	type XY struct {
//		X int32
//		Y int32
//		Z struct{
//			A string
//		}
//	}
//
//	xy := XY{
//		X: 1,
//		Y: 2,
//		Z: struct {
//			A string
//		} {
//			A: "hello world",
//		},
//	}
//
//	err := s.Encode(xy)
//	if err != nil {
//		t.Error(err)
//	}
//
//	fmt.Printf("X=%v, Y=%v, Z=%v\n", s.data["X"], s.data["Y"], s.data["Z"])
//}
//
//func TestTemp(_ *testing.T)  {
//	t := reflect.TypeOf(datatype.IntType)
//	println(t.Name())
//	println(t.PkgPath())
//}
