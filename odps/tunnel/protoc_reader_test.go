package tunnel

import (
	"bytes"
	data2 "github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"strings"
	"testing"
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
	dt, _ := datatype2.ParseDataType(typeName)
	st := dt.(datatype2.StructType)
	columns := []tableschema.Column{
		{
			Name: "struct_field",
			Type: st,
		},
	}

	rp := RecordProtocReader{
		httpRes:     nil,
		protoReader: NewProtocStreamReader(br),
		columns:     columns,
		recordCrc:   NewCrc32CheckSum(),
		crcOfCrc:    NewCrc32CheckSum(),
	}

	record, err := rp.Read()
	if err != nil {
		t.Fatal(err)
	}

	if record.Len() != 1 {
		t.Fatalf("record has one column, but get %d", record.Len())
	}

	s := record[0].(*data2.Struct)
	structStr := "struct<x:11,y:hello,z:struct<a:100,b:1970-01-01>>"

	if s.String() != structStr {
		t.Fatalf("expected %s, but get %s", structStr, s.String())
	}

	expected := []struct {
		name  string
		value string
		typ   datatype2.DataType
	}{
		{"x", "11", datatype2.IntType},
		{"y", "hello", datatype2.NewVarcharType(256)},
		{"z", "struct<a:100,b:1970-01-01>", st.FieldType("z")},
		{"z.a", "100", datatype2.TinyIntType},
		{"z.b", "1970-01-01", datatype2.DateType},
	}

	x := s.GetField("x")
	y := s.GetField("y")
	z := s.GetField("z").(*data2.Struct)
	a := z.GetField("a")
	b := z.GetField("b")

	fields := []data2.Data{x, y, z, a, b}
	for i, n := 0, len(expected); i < n; i++ {
		fv := fields[i].String()
		ft := fields[i].Type()

		if expected[i].value != fv {
			t.Fatalf(
				"expect value is %s, but get %s for %s",
				expected[i].value, fv, expected[i].name,
			)
		}

		if !datatype2.IsTypeEqual(expected[i].typ, ft) {
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
			Type: datatype2.TinyIntType,
		},
		{
			Name: "si",
			Type: datatype2.SmallIntType,
		},
		{
			Name: "i",
			Type: datatype2.IntType,
		},
		{
			Name: "bi",
			Type: datatype2.BigIntType,
		},
		{
			Name: "b",
			Type: datatype2.BinaryType,
		},
		{
			Name: "f",
			Type: datatype2.FloatType,
		},
		{
			Name: "d",
			Type: datatype2.DoubleType,
		},
		{
			Name: "dc",
			Type: datatype2.NewDecimalType(38, 18),
		},
		{
			Name: "vc",
			Type: datatype2.NewVarcharType(1000),
		},
		{
			Name: "c",
			Type: datatype2.NewCharType(100),
		},
		{
			Name: "s",
			Type: datatype2.StringType,
		},
		{
			Name: "da",
			Type: datatype2.DateType,
		},
		{
			Name: "dat",
			Type: datatype2.DateTimeType,
		},
		{
			Name: "t",
			Type: datatype2.TimestampType,
		},
		{
			Name: "bl",
			Type: datatype2.BooleanType,
		},
	}

	rp := RecordProtocReader{
		httpRes:     nil,
		protoReader: NewProtocStreamReader(br),
		columns:     columns,
		recordCrc:   NewCrc32CheckSum(),
		crcOfCrc:    NewCrc32CheckSum(),
	}

	expected := []struct {
		value string
		typ   datatype2.DataType
	}{
		{"1", datatype2.TinyIntType},                                        // 1
		{"32767", datatype2.SmallIntType},                                   // 2
		{"100", datatype2.IntType},                                          // 3
		{"100000000000", datatype2.BigIntType},                              // 4
		{"unhex('FA34E10293CB42848573A4E39937F479')", datatype2.BinaryType}, // 5
		{"3.1415926E7", datatype2.FloatType},                                // 6
		{"3.14159261E7", datatype2.DoubleType},                              // 7
		{"3.5", datatype2.NewDecimalType(38, 18)},                           // 8
		{"hello", datatype2.NewVarcharType(1000)},                           // 9
		{"world", datatype2.NewCharType(100)},                               // 10
		{"alibaba", datatype2.StringType},                                   // 11
		{"2017-11-11", datatype2.DateType},                                  // 12
		{"2017-11-11 00:00:00", datatype2.DateTimeType},                     // 13
		{"2017-11-11 00:00:00.123", datatype2.TimestampType},                // 14
		{"true", datatype2.BooleanType},                                     // 15
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

			if !datatype2.IsTypeEqual(got.Type(), expected[i].typ) {
				t.Fatalf(
					"%dth column's type should be %s, but get %s",
					i+1, expected[i].typ, got.Type(),
				)

			}
		}
	}
}
