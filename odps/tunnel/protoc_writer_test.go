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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"reflect"
	"testing"
)

var simpleTypeProtocEncodedData = []byte{
	0x08, 0x02, // 1 tinyint 1
	0x10, 0xfe, 0xff, 0x03, // 2 smallint 32678
	0x18, 0xc8, 0x01, // 3 int 100
	0x20, 0x80, 0xa0, 0xb7, 0x87, 0xe9, 0x05, // 4 bigint 100000000000
	0x2a, 0x06, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, // 5 binary binary
	0x35, 0xc3, 0xf5, 0x48, 0x40, // 6 float 3.14
	0x39, 0x4a, 0xd8, 0x12, 0x4d, 0xfb, 0x21, 0x09, 0x40, // 7 double 3.1415926
	0x42, 0x09, 0x33, 0x2e, 0x31, 0x34, 0x31, 0x35, 0x39, 0x32, 0x36, // 8 decimal 3.1415926
	0x4a, 0x07, 0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72, // 9 varchar varchar
	0x52, 0x04, 0x63, 0x68, 0x61, 0x72, // 10 char char
	0x5a, 0x07, 0x61, 0x6c, 0x69, 0x62, 0x61, 0x62, 0x61, // 11 string alibaba
	0x60, 0xa8, 0xad, 0x02, // 12 date 2022-10-19
	0x68, 0x80, 0xca, 0xc4, 0xf7, 0xfd, 0x60, // 13 datetime  2022-10-19 17:00:00
	0x72, 0xa0, 0xf2, 0xfd, 0xb4, 0x0c, 0x00, // 14 timestamp 2022-10-19 17:00:00.000
	0x78, 0x01, // 15 bool true
	0x82, 0x01, 0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62, 0x00, 0x03, 0x74, 0x6f, 0x6d, // 16 struct type
	0x80, 0xc0, 0xff, 0x7f, // end of record
	0xcc, 0x90, 0xbd, 0xa1, // crc of record
	0x01, 0xf0, 0xff, 0xff, 0x7f, 0x02, 0xf8, 0xff, 0xff, 0x7f, 0xa6, 0x83,
	0x95, 0xf5, 0x0d, // record count, total crc

}

var complexTypeProtocEncodedData = []byte{
	0x0a, 0x07, 0x61, 0x6c, 0x69, 0x62, 0x61, 0x62, 0x61, 0x10, 0x80, 0xa0, 0xb7, 0x87, 0xe9, 0x05,
	0x18, 0x80, 0xca, 0xc4, 0xf7, 0xfd, 0x60, 0x22, 0x00, 0x02, 0x00, 0x0b, 0x63, 0x6c, 0x6f, 0x75,
	0x64, 0x5f, 0x73, 0x69, 0x6c, 0x6c, 0x79, 0x00, 0x03, 0x65, 0x66, 0x63, 0x00, 0x0b, 0x70, 0x72,
	0x6f, 0x67, 0x72, 0x61, 0x6d, 0x6d, 0x69, 0x6e, 0x67, 0x80, 0xc0, 0xff, 0x7f, 0xb8, 0xb6, 0x94,
	0x8d, 0x07, 0xf0, 0xff, 0xff, 0x7f, 0x02, 0xf8, 0xff, 0xff, 0x7f, 0xf5, 0xf3, 0xb0, 0xc1, 0x07,
}

func TestEncodeProtocSimpleType(t *testing.T) {
	bw := &bufWriter{bytes.NewBuffer(nil)}
	stTypeName := "STRUCT<arr:ARRAY<STRING>, name:STRING> "
	stColumnType_, _ := datatype.ParseDataType(stTypeName)
	stColumnType := stColumnType_.(datatype.StructType)

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
		{
			Name: "st",
			Type: stColumnType,
		},
	}

	pw := newRecordProtocWriter(bw, columns, false)
	varchar, _ := data.NewVarChar(1000, "varchar")
	char, _ := data.NewVarChar(100, "char")
	s := data.String("alibaba")
	date, _ := data.NewDate("2022-10-19")
	datetime, _ := data.NewDateTime("2022-10-19 17:00:00")
	timestamp, _ := data.NewTimestamp("2022-10-19 17:00:00.000")

	structData := data.NewStructWithTyp(stColumnType)
	arrayType := stColumnType.FieldType("arr").(datatype.ArrayType)
	arr := data.NewArrayWithType(arrayType)
	_ = arr.Append("a")
	_ = arr.Append("b")
	_ = structData.SetField("arr", arr)
	_ = structData.SetField("name", "tom")

	record := []data.Data{
		data.TinyInt(1),
		data.SmallInt(32767),
		data.Int(100),
		data.BigInt(100000000000),
		data.Binary("binary"),
		data.Float(3.14),
		data.Double(3.1415926),
		data.NewDecimal(38, 18, "3.1415926"),
		varchar,
		char,
		&s,
		date,
		datetime,
		timestamp,
		data.Bool(true),
		structData,
	}

	err := pw.Write(record)
	if err != nil {
		t.Fatal(err)
	}

	err = pw.Close()
	if err != nil {
		t.Fatal(err)
	}

	bytesGot := bw.buf.Bytes()

	if !reflect.DeepEqual(bytesGot, simpleTypeProtocEncodedData) {
		t.Fatal("protoc writer cannot serialize right data")
	}
}

func TestEncodeProtocComplexType(t *testing.T) {
	bw := &bufWriter{bytes.NewBuffer(nil)}
	extraColumnTypeName := "struct<address:array<string>,hobby:string>"
	extraColumnType_, _ := datatype.ParseDataType(extraColumnTypeName)
	extraColumnType := extraColumnType_.(datatype.StructType)

	columns := []tableschema.Column{
		{
			Name: "name",
			Type: datatype.StringType,
		},
		{
			Name: "score",
			Type: datatype.BigIntType,
		},
		{
			Name: "birthday",
			Type: datatype.DateTimeType,
		},
		{
			Name: "extra",
			Type: extraColumnType,
		},
	}

	pw := newRecordProtocWriter(bw, columns, false)
	name := data.String("alibaba")
	birthday, _ := data.NewDateTime("2022-10-19 17:00:00")
	extra := data.NewStructWithTyp(extraColumnType)
	arrayType := extraColumnType.FieldType("address").(datatype.ArrayType)
	address := data.NewArrayWithType(arrayType)
	_ = address.Append("cloud_silly")
	_ = address.Append("efc")
	_ = extra.SetField("address", address)
	_ = extra.SetField("hobby", "programming")

	record := []data.Data{
		&name,
		data.BigInt(100000000000),
		birthday,
		extra,
	}

	err := pw.Write(record)
	if err != nil {
		t.Fatal(err)
	}

	err = pw.Close()
	if err != nil {
		t.Fatal(err)
	}

	bytesGot := bw.buf.Bytes()
	if !reflect.DeepEqual(bytesGot, complexTypeProtocEncodedData) {
		t.Fatal("protoc writer cannot serialize right data")
	}
}
