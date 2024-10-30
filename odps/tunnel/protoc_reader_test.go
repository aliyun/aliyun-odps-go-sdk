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

package tunnel

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

var structTypeProtocData = []byte{
	0x0a, 0x00, 0x16, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0xc8, 0x01, 0x00, 0x00,
	0x80, 0xc0, 0xff, 0x7f, 0xa0, 0x92, 0x83, 0xc9, 0x05, 0xf0, 0xff, 0xff, 0x7f, 0x02, 0xf8, 0xff,
	0xff, 0x7f, 0xf0, 0xcc, 0xb3, 0xc0, 0x01,
}

var simpleTypeProtocData = []byte{
	0x08, 0x02, 0x10, 0xfe, 0xff, 0x03, 0x18, 0xc8, 0x01, 0x20, 0x80, 0xa0, 0xb7, 0x87, 0xe9, 0x05,
	0x2a, 0x10, 0xfa, 0x34, 0xe1, 0x02, 0x93, 0xcb, 0x42, 0x84, 0x85, 0x73, 0xa4, 0xe3, 0x99, 0x37,
	0xf4, 0x79, 0x35, 0x3b, 0xaf, 0xef, 0x4b, 0x39, 0x9a, 0x99, 0x99, 0x61, 0xe7, 0xf5, 0x7d, 0x41,
	0x42, 0x03, 0x33, 0x2e, 0x35, 0x4a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x52, 0x64, 0x77, 0x6f,
	0x72, 0x6c, 0x64, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
	0x20, 0x20, 0x5a, 0x07, 0x61, 0x6c, 0x69, 0x62, 0x61, 0x62, 0x61, 0x60, 0x92, 0x91, 0x02, 0x68,
	0x80, 0xa0, 0xc6, 0xea, 0xf4, 0x57, 0x72, 0x80, 0xb4, 0xae, 0xa0, 0x0b, 0xaa, 0xb4, 0xde, 0x75,
	0x78, 0x01, 0x80, 0xc0, 0xff, 0x7f, 0xd9, 0x9e, 0x9d, 0xde, 0x0f, 0xf0, 0xff, 0xff, 0x7f, 0x02,
	0xf8, 0xff, 0xff, 0x7f, 0xc0, 0x8f, 0xfa, 0xf1, 0x0b,
}

func TestProtocReadStructType(t *testing.T) {
	br := bytes.NewReader(structTypeProtocData)

	typeName := "struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>"
	dt, _ := datatype.ParseDataType(typeName)
	st := dt.(datatype.StructType)
	columns := []tableschema.Column{
		{
			Name: "struct_field",
			Type: st,
		},
	}

	rp := RecordProtocReader{
		httpRes:      nil,
		protocReader: NewProtocStreamReader(br),
		columns:      columns,
		recordCrc:    NewCrc32CheckSum(),
		crcOfCrc:     NewCrc32CheckSum(),
	}

	record, err := rp.Read()
	if err != nil {
		t.Fatal(err)
	}

	if record.Len() != 1 {
		t.Fatalf("record has one column, but get %d", record.Len())
	}

	s := record[0].(*data.Struct)
	structStr := "struct<x:11,y:hello,z:struct<a:100,b:1970-01-01>>"

	if s.String() != structStr {
		t.Fatalf("expected %s, but get %s", structStr, s.String())
	}

	expected := []struct {
		name  string
		value string
		typ   datatype.DataType
	}{
		{"x", "11", datatype.IntType},
		{"y", "hello", datatype.NewVarcharType(256)},
		{"z", "struct<a:100,b:1970-01-01>", st.FieldType("z")},
		{"z.a", "100", datatype.TinyIntType},
		{"z.b", "1970-01-01", datatype.DateType},
	}

	x := s.GetField("x")
	y := s.GetField("y")
	z := s.GetField("z").(*data.Struct)
	a := z.GetField("a")
	b := z.GetField("b")

	fields := []data.Data{x, y, z, a, b}
	for i, n := 0, len(expected); i < n; i++ {
		fv := fields[i].String()
		ft := fields[i].Type()

		if expected[i].value != fv {
			t.Fatalf(
				"expect value is %s, but get %s for %s",
				expected[i].value, fv, expected[i].name,
			)
		}

		if !datatype.IsTypeEqual(expected[i].typ, ft) {
			t.Fatalf(
				"expect type is %s, but get %s for %s",
				expected[i].typ, ft, expected[i].name,
			)
		}
	}
}

func TestProtocReadSimpleType(t *testing.T) {
	br := bytes.NewReader(simpleTypeProtocData)

	columns := []tableschema.Column{
		{
			Name: "ti",
			Type: datatype.TinyIntType,
		},
		{
			Name: "si",
			Type: datatype.SmallIntType,
		},
		{
			Name: "i",
			Type: datatype.IntType,
		},
		{
			Name: "bi",
			Type: datatype.BigIntType,
		},
		{
			Name: "b",
			Type: datatype.BinaryType,
		},
		{
			Name: "f",
			Type: datatype.FloatType,
		},
		{
			Name: "d",
			Type: datatype.DoubleType,
		},
		{
			Name: "dc",
			Type: datatype.NewDecimalType(38, 18),
		},
		{
			Name: "vc",
			Type: datatype.NewVarcharType(1000),
		},
		{
			Name: "c",
			Type: datatype.NewCharType(100),
		},
		{
			Name: "s",
			Type: datatype.StringType,
		},
		{
			Name: "da",
			Type: datatype.DateType,
		},
		{
			Name: "dat",
			Type: datatype.DateTimeType,
		},
		{
			Name: "t",
			Type: datatype.TimestampType,
		},
		{
			Name: "bl",
			Type: datatype.BooleanType,
		},
	}

	rp := RecordProtocReader{
		httpRes:      nil,
		protocReader: NewProtocStreamReader(br),
		columns:      columns,
		recordCrc:    NewCrc32CheckSum(),
		crcOfCrc:     NewCrc32CheckSum(),
	}

	expected := []struct {
		value string
		typ   datatype.DataType
	}{
		{"1", datatype.TinyIntType},                                        // 1
		{"32767", datatype.SmallIntType},                                   // 2
		{"100", datatype.IntType},                                          // 3
		{"100000000000", datatype.BigIntType},                              // 4
		{"unhex('FA34E10293CB42848573A4E39937F479')", datatype.BinaryType}, // 5
		{"3.1415926E7", datatype.FloatType},                                // 6
		{"3.14159261E7", datatype.DoubleType},                              // 7
		{"3.5", datatype.NewDecimalType(38, 18)},                           // 8
		{"hello", datatype.NewVarcharType(1000)},                           // 9
		{"world", datatype.NewCharType(100)},                               // 10
		{"alibaba", datatype.StringType},                                   // 11
		{"2017-11-11", datatype.DateType},                                  // 12
		{"2017-11-11 00:00:00", datatype.DateTimeType},                     // 13
		{"2017-11-11 00:00:00.123", datatype.TimestampType},                // 14
		{"true", datatype.BooleanType},                                     // 15
	}

	record, err := rp.Read()

	if err != nil {
		t.Fatal(err)
	} else {
		if record.Len() != len(columns) {
			t.Fatalf("expected %d columns, but get %d", len(columns), record.Len())
		}

		for i, n := 0, record.Len(); i < n; i++ {
			got := record[i]
			gotStr := got.String()
			expectedStr := expected[i].value

			if i != 5 && i != 6 && strings.TrimSpace(got.String()) != expected[i].value {
				t.Fatalf(
					"%dth column should be %s, but get %s",
					i+1, expectedStr, gotStr,
				)
			}

			if !datatype.IsTypeEqual(got.Type(), expected[i].typ) {
				t.Fatalf(
					"%dth column's type should be %s, but get %s",
					i+1, expected[i].typ, got.Type(),
				)

			}
		}
	}
}
