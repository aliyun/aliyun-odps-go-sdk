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
	"testing"
)

func TestTryConvertGoToOdpsData(t *testing.T) {
	c, _ := NewChar(5, "hello")

	type SimpleStruct struct {
		Name string
		Age  int32
	}
	odpsStruct := NewStruct()
	_ = odpsStruct.SetField("Name", "tom")
	_ = odpsStruct.SetField("Age", int32(10))

	odpsArray := NewArray()
	_ = odpsArray.Append("a", "b")

	//m := make(map[string]int32)
	//m["hello"] = 32
	//odpsMap := NewMap()
	//_ = odpsMap.Set("hello", int32(32))

	testData := []struct {
		input interface{}
		want  Data
	}{
		{int32(10), Int(10)},
		{c, c},
		{SimpleStruct{Name: "tom", Age: 10}, odpsStruct},
		{[]string{"a", "b"}, odpsArray},
		//{m, odpsMap},
	}

	for _, d := range testData {
		got, err := TryConvertGoToOdpsData(d.input)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if !IsDataEqual(got, d.want) {
			t.Fatalf("expect %s, but get %s", d.want, got)
		}
	}
}

func TestStruct_FillGoStruct(t *testing.T) {
	type SimpleStruct struct {
		Name string
		Age  int32
	}
	odpsStruct := NewStruct()
	_ = odpsStruct.SetField("Name", "tom")
	_ = odpsStruct.SetField("Age", int32(10))

	var simpleStruct SimpleStruct
	expected := SimpleStruct{
		Name: "tom",
		Age:  10,
	}

	err := odpsStruct.FillGoStruct(&simpleStruct)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if !reflect.DeepEqual(expected, simpleStruct) {
		t.Fatalf("expect %+v, but get %+v", expected, simpleStruct)
	}
}
