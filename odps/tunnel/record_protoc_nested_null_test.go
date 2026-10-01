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
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// Everything in this file runs in memory: no tunnel endpoint, no credentials,
// no server round trip. It covers NULL values *inside* ARRAY / MAP / STRUCT,
// the shapes reported in
// https://github.com/aliyun/aliyun-odps-go-sdk/issues/27.

func mustParseDataType(t *testing.T, typeText string) datatype.DataType {
	t.Helper()

	dt, err := datatype.ParseDataType(typeText)
	if err != nil {
		t.Fatalf("parse data type %q: %v", typeText, err)
	}

	return dt
}

// reportPanic turns a panic into a test failure carrying the stack, so that one
// run gives a verdict for every type shape instead of aborting the binary on
// the first panic.
func reportPanic(t *testing.T, label string) {
	if r := recover(); r != nil {
		t.Errorf("PANIC in %s: %v\n%s", label, r, debug.Stack())
	}
}

// mustNotPanic runs f and fails the test if f panics.
func mustNotPanic(t *testing.T, label string, f func()) {
	t.Helper()

	defer reportPanic(t, label)
	f()
}

// canonicalValue renders a value deterministically, with NULL written as the
// literal "null" at every nesting level. Written and read-back values are
// compared through the same function, so primitive rendering details
// (for example the "L" suffix of BIGINT literals) cancel out.
func canonicalValue(v data.Data) string {
	if v == nil {
		return "null"
	}

	switch typed := v.(type) {
	case *data.Array:
		if typed == nil {
			return "null"
		}
		return canonicalArray(typed)
	case *data.Map:
		if typed == nil {
			return "null"
		}
		return canonicalMap(typed)
	case *data.Struct:
		if typed == nil {
			return "null"
		}
		return canonicalStruct(typed)
	default:
		return v.Sql()
	}
}

func canonicalArray(a *data.Array) string {
	elems := make([]string, 0, a.Len())
	for i := 0; i < a.Len(); i++ {
		elems = append(elems, canonicalValue(a.Index(i)))
	}

	return "[" + strings.Join(elems, ",") + "]"
}

// canonicalMap sorts the pairs because Go map iteration order is random.
func canonicalMap(m *data.Map) string {
	pairs := make([]string, 0, len(m.ToGoMap()))
	for key, value := range m.ToGoMap() {
		pairs = append(pairs, canonicalValue(key)+"=>"+canonicalValue(value))
	}
	sort.Strings(pairs)

	return "{" + strings.Join(pairs, ",") + "}"
}

func canonicalStruct(s *data.Struct) string {
	pairs := make([]string, 0, len(s.Fields()))
	for _, field := range s.Fields() {
		pairs = append(pairs, field.Name+"="+canonicalValue(field.Value))
	}

	return "<" + strings.Join(pairs, ",") + ">"
}

// protocRoundTrip serializes records with RecordProtocWriter and reads them
// back with RecordProtocReader out of the same in-memory buffer.
func protocRoundTrip(t *testing.T, columns []tableschema.Column, records []data.Record) []data.Record {
	t.Helper()

	bw := &bufWriter{buf: bytes.NewBuffer(nil)}
	pw := newRecordProtocWriter(bw, columns, false)

	for i, record := range records {
		if err := pw.Write(record); err != nil {
			t.Fatalf("write record %d: %v", i, err)
		}
	}

	if err := pw.Close(); err != nil {
		t.Fatalf("close protoc writer: %v", err)
	}

	pr := RecordProtocReader{
		protocReader: NewProtocStreamReader(bytes.NewReader(bw.buf.Bytes())),
		columns:      columns,
		recordCrc:    NewCrc32CheckSum(),
		crcOfCrc:     NewCrc32CheckSum(),
	}

	read := make([]data.Record, 0, len(records))
	for {
		record, err := pr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read record %d: %v", len(read), err)
		}

		read = append(read, record)
	}

	if len(read) != len(records) {
		t.Fatalf("expected to read back %d records, got %d", len(records), len(read))
	}

	return read
}

// assertRoundTripPreservesValues compares canonical renderings column by column.
func assertRoundTripPreservesValues(t *testing.T, written, read data.Record) {
	t.Helper()

	if written.Len() != read.Len() {
		t.Fatalf("column count changed: wrote %d, read %d", written.Len(), read.Len())
	}

	for i := 0; i < written.Len(); i++ {
		want, got := canonicalValue(written[i]), canonicalValue(read[i])
		if want != got {
			t.Errorf("column %d changed through the round trip: wrote %s, read %s", i, want, got)
		}
	}
}

func TestProtocRoundTripArrayWithNullElements(t *testing.T) {
	arrayType := mustParseDataType(t, "array<bigint>").(datatype.ArrayType)
	columns := []tableschema.Column{{Name: "a", Type: arrayType}}

	arr := data.NewArrayWithType(arrayType)
	if err := arr.Append(int64(1)); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := arr.Append(nil); err != nil {
		t.Fatalf("append null: %v", err)
	}
	if err := arr.Append(int64(3)); err != nil {
		t.Fatalf("append 3: %v", err)
	}

	record := data.Record{arr}
	read := protocRoundTrip(t, columns, []data.Record{record})
	assertRoundTripPreservesValues(t, record, read[0])

	got, ok := read[0][0].(*data.Array)
	if !ok {
		t.Fatalf("expected *data.Array, got %T", read[0][0])
	}
	if got.Len() != 3 {
		t.Fatalf("expected 3 elements, got %d", got.Len())
	}
	if got.Index(1) != nil {
		t.Fatalf("expected a null element at index 1, got %v", got.Index(1))
	}
	if want := "[1L,null,3L]"; canonicalValue(got) != want {
		t.Fatalf("array round trip: want %s, got %s", want, canonicalValue(got))
	}
}

func TestProtocRoundTripMapWithNullValues(t *testing.T) {
	mapType := mustParseDataType(t, "map<string,string>").(datatype.MapType)
	columns := []tableschema.Column{{Name: "m", Type: mapType}}

	m := data.NewMapWithType(mapType)
	if err := m.Set("hello", "a"); err != nil {
		t.Fatalf("set hello: %v", err)
	}
	if err := m.Set("world", nil); err != nil {
		t.Fatalf("set world null: %v", err)
	}

	record := data.Record{m}
	read := protocRoundTrip(t, columns, []data.Record{record})
	assertRoundTripPreservesValues(t, record, read[0])

	got, ok := read[0][0].(*data.Map)
	if !ok {
		t.Fatalf("expected *data.Map, got %T", read[0][0])
	}
	if want := "{'hello'=>'a','world'=>null}"; canonicalValue(got) != want {
		t.Fatalf("map round trip: want %s, got %s", want, canonicalValue(got))
	}

	nullValues := 0
	for key, value := range got.ToGoMap() {
		if value == nil {
			nullValues++
			if key.Sql() != "'world'" {
				t.Errorf("unexpected null value for key %s", key.Sql())
			}
		}
	}
	if nullValues != 1 {
		t.Fatalf("expected exactly one null map value, got %d", nullValues)
	}
}

func TestProtocRoundTripStructWithNullFields(t *testing.T) {
	structType := mustParseDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)
	columns := []tableschema.Column{{Name: "s", Type: structType}}

	s := data.NewStructWithTyp(structType)
	if err := s.SetField("x", int64(1)); err != nil {
		t.Fatalf("set x: %v", err)
	}
	if err := s.SetField("y", nil); err != nil {
		t.Fatalf("set y null: %v", err)
	}

	record := data.Record{s}
	read := protocRoundTrip(t, columns, []data.Record{record})
	assertRoundTripPreservesValues(t, record, read[0])

	got, ok := read[0][0].(*data.Struct)
	if !ok {
		t.Fatalf("expected *data.Struct, got %T", read[0][0])
	}
	if got.GetField("y") != nil {
		t.Fatalf("expected a null field y, got %v", got.GetField("y"))
	}
	if want := "<x=1L,y=null>"; canonicalValue(got) != want {
		t.Fatalf("struct round trip: want %s, got %s", want, canonicalValue(got))
	}
}

func TestProtocRoundTripNestedNullInsideComplexTypes(t *testing.T) {
	arrayOfStructType := mustParseDataType(t, "array<struct<x:bigint,y:bigint>>").(datatype.ArrayType)
	mapOfArrayType := mustParseDataType(t, "map<bigint,array<string>>").(datatype.MapType)
	structWithComplexType := mustParseDataType(t, "struct<arr:array<string>,m:map<string,string>>").(datatype.StructType)

	columns := []tableschema.Column{
		{Name: "a_of_s", Type: arrayOfStructType},
		{Name: "m_of_a", Type: mapOfArrayType},
		{Name: "s_with_cmplx", Type: structWithComplexType},
	}

	// [ struct<x:1,y:null>, null ]
	innerStructType := mustParseDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)
	elem := data.NewStructWithTyp(innerStructType)
	if err := elem.SetField("x", int64(1)); err != nil {
		t.Fatalf("set inner x: %v", err)
	}
	if err := elem.SetField("y", nil); err != nil {
		t.Fatalf("set inner y null: %v", err)
	}
	outerArray := data.NewArrayWithType(arrayOfStructType)
	if err := outerArray.Append(elem); err != nil {
		t.Fatalf("append struct: %v", err)
	}
	if err := outerArray.Append(nil); err != nil {
		t.Fatalf("append null struct: %v", err)
	}

	// { 1: ["a", null], 2: null }
	strArrayType := mustParseDataType(t, "array<string>").(datatype.ArrayType)
	innerArray := data.NewArrayWithType(strArrayType)
	if err := innerArray.Append("a"); err != nil {
		t.Fatalf("append 'a': %v", err)
	}
	if err := innerArray.Append(nil); err != nil {
		t.Fatalf("append null string: %v", err)
	}
	complexMap := data.NewMapWithType(mapOfArrayType)
	if err := complexMap.Set(int64(1), innerArray); err != nil {
		t.Fatalf("set key 1: %v", err)
	}
	if err := complexMap.Set(int64(2), nil); err != nil {
		t.Fatalf("set key 2 null: %v", err)
	}

	// struct<arr:["a",null], m:null>
	complexStruct := data.NewStructWithTyp(structWithComplexType)
	if err := complexStruct.SetField("arr", innerArray); err != nil {
		t.Fatalf("set field arr: %v", err)
	}
	if err := complexStruct.SetField("m", nil); err != nil {
		t.Fatalf("set field m null: %v", err)
	}

	record := data.Record{outerArray, complexMap, complexStruct}
	read := protocRoundTrip(t, columns, []data.Record{record})
	assertRoundTripPreservesValues(t, record, read[0])

	wantColumns := []string{
		"[<x=1L,y=null>,null]",
		"{1L=>['a',null],2L=>null}",
		"<arr=['a',null],m=null>",
	}
	for i, want := range wantColumns {
		if got := canonicalValue(read[0][i]); got != want {
			t.Errorf("column %d round trip: want %s, got %s", i, want, got)
		}
	}
}

// TestDownloadedRecordWithNullsIsPrintable covers the read side the issue
// reports: after a download, user code and the SDK examples print records,
// which walks every nested value through String().
func TestDownloadedRecordWithNullsIsPrintable(t *testing.T) {
	arrayType := mustParseDataType(t, "array<bigint>").(datatype.ArrayType)
	mapType := mustParseDataType(t, "map<string,string>").(datatype.MapType)
	structType := mustParseDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)
	columns := []tableschema.Column{
		{Name: "a", Type: arrayType},
		{Name: "m", Type: mapType},
		{Name: "s", Type: structType},
	}

	arr := data.NewArrayWithType(arrayType)
	if err := arr.Append(int64(1)); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := arr.Append(nil); err != nil {
		t.Fatalf("append null: %v", err)
	}

	m := data.NewMapWithType(mapType)
	if err := m.Set("hello", "a"); err != nil {
		t.Fatalf("set hello: %v", err)
	}
	if err := m.Set("world", nil); err != nil {
		t.Fatalf("set world null: %v", err)
	}

	s := data.NewStructWithTyp(structType)
	if err := s.SetField("x", int64(1)); err != nil {
		t.Fatalf("set x: %v", err)
	}
	if err := s.SetField("y", nil); err != nil {
		t.Fatalf("set y null: %v", err)
	}

	record := data.Record{arr, m, s}
	read := protocRoundTrip(t, columns, []data.Record{record})
	got := read[0]

	// A column that is NULL itself is also legal in a downloaded record.
	withNullColumn := data.Record{nil, arr, m, s}

	mustNotPanic(t, "Array.String() with null element", func() { _ = got[0].String() })
	mustNotPanic(t, "Map.String() with null value", func() { _ = got[1].String() })
	mustNotPanic(t, "Struct.String() with null field", func() { _ = got[2].String() })
	mustNotPanic(t, "Record.String() with nested nulls", func() { _ = got.String() })
	mustNotPanic(t, "Record.String() with a null column", func() { _ = withNullColumn.String() })
	mustNotPanic(t, "Array.Sql() with null element", func() { _ = got[0].Sql() })
	mustNotPanic(t, "Map.Sql() with null value", func() { _ = got[1].Sql() })
	mustNotPanic(t, "Struct.Sql() with null field", func() { _ = got[2].Sql() })
	mustNotPanic(t, "fmt.Println(record)", func() {
		_, _ = fmt.Fprintln(io.Discard, got[0], got[1], got[2], got)
	})
}

// TestNullSafeTypedApiSurface pins the typed ("Safe") constructors and the
// type-inference helpers, which are the other way a user builds ARRAY/MAP/STRUCT
// values containing NULL before an upload.
func TestNullSafeTypedApiSurface(t *testing.T) {
	arrayType := mustParseDataType(t, "array<bigint>").(datatype.ArrayType)
	mapType := mustParseDataType(t, "map<string,string>").(datatype.MapType)
	structType := mustParseDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)

	t.Run("Array.SafeAppendWithNull", func(t *testing.T) {
		defer reportPanic(t, "Array.SafeAppend(nil)")

		a := data.NewArrayWithType(arrayType)
		if err := a.SafeAppend(int64(1)); err != nil {
			t.Fatalf("SafeAppend(1): %v", err)
		}
		if err := a.SafeAppend(nil); err != nil {
			t.Fatalf("SafeAppend(nil) returned an error: %v", err)
		}
		if a.Len() != 2 || a.Index(1) != nil {
			t.Fatalf("expected [1L, null], got %s", canonicalArray(a))
		}
	})

	t.Run("Map.SafeSetWithNullValue", func(t *testing.T) {
		defer reportPanic(t, "Map.SafeSet(key, nil)")

		m := data.NewMapWithType(mapType)
		if err := m.SafeSet(data.String("hello"), data.String("a")); err != nil {
			t.Fatalf("SafeSet(hello): %v", err)
		}
		if err := m.SafeSet(data.String("world"), nil); err != nil {
			t.Fatalf("SafeSet(world, nil) returned an error: %v", err)
		}
		if want := "{'hello'=>'a','world'=>null}"; canonicalMap(m) != want {
			t.Fatalf("want %s, got %s", want, canonicalMap(m))
		}
	})

	t.Run("Struct.SafeSetFieldWithNull", func(t *testing.T) {
		defer reportPanic(t, "Struct.SafeSetField(y, nil)")

		s := data.NewStructWithTyp(structType)
		if err := s.SafeSetField("x", int64(1)); err != nil {
			t.Fatalf("SafeSetField(x): %v", err)
		}
		if err := s.SafeSetField("y", nil); err != nil {
			t.Fatalf("SafeSetField(y, nil) returned an error: %v", err)
		}
		if s.GetField("y") != nil {
			t.Fatalf("expected a null field y, got %v", s.GetField("y"))
		}
	})

	t.Run("TypeInferWithNulls", func(t *testing.T) {
		a := data.NewArrayWithType(arrayType)
		if err := a.Append(int64(1)); err != nil {
			t.Fatalf("append 1: %v", err)
		}
		if err := a.Append(nil); err != nil {
			t.Fatalf("append null: %v", err)
		}

		m := data.NewMapWithType(mapType)
		if err := m.Set("hello", "a"); err != nil {
			t.Fatalf("set hello: %v", err)
		}
		if err := m.Set("world", nil); err != nil {
			t.Fatalf("set world null: %v", err)
		}

		s := data.NewStructWithTyp(structType)
		if err := s.SetField("x", int64(1)); err != nil {
			t.Fatalf("set x: %v", err)
		}
		if err := s.SetField("y", nil); err != nil {
			t.Fatalf("set y null: %v", err)
		}

		// NULL elements carry no type: inference must either return the type of
		// the non-null values or an error, but never panic.
		mustNotPanic(t, "Array.TypeInfer()", func() {
			dt, err := a.TypeInfer()
			t.Logf("Array.TypeInfer() = %v, err=%v", dt, err)
		})
		mustNotPanic(t, "Map.TypeInfer()", func() {
			dt, err := m.TypeInfer()
			t.Logf("Map.TypeInfer() = %v, err=%v", dt, err)
		})
		mustNotPanic(t, "Struct.TypeInfer()", func() {
			dt, err := s.TypeInfer()
			t.Logf("Struct.TypeInfer() = %v, err=%v", dt, err)
		})

		// An array whose elements are all NULL has no inferable element type.
		allNull := data.NewArray()
		if err := allNull.Append(nil); err != nil {
			t.Fatalf("append null: %v", err)
		}
		mustNotPanic(t, "Array.TypeInfer() on all-null array", func() {
			dt, err := allNull.TypeInfer()
			t.Logf("all-null Array.TypeInfer() = %v, err=%v", dt, err)
		})
	})
}
