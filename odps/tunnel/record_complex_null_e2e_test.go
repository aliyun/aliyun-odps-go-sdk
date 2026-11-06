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

package tunnel_test

import (
	"errors"
	"io"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

// This test really talks to a tunnel endpoint: it uploads records, commits
// them and downloads them back. It skips when no credentials are present, so
// it stays out of the way locally and runs in CI, which has them.
//
// The in-memory round trip in record_protoc_nested_null_test.go only proves the
// writer and the reader agree with each other. This proves the service agrees
// too: a NULL inside an ARRAY / MAP / STRUCT survives a real upload and
// download, and the record that comes back can still be printed - which is what
// https://github.com/aliyun/aliyun-odps-go-sdk/issues/27 reported as a panic.

const complexNullE2ETable = "tunnel_complex_null_e2e"

func mustParseE2EDataType(t *testing.T, typeText string) datatype.DataType {
	t.Helper()

	dt, err := datatype.ParseDataType(typeText)
	if err != nil {
		t.Fatalf("parse data type %q: %v", typeText, err)
	}

	return dt
}

// complexNullE2ERender is local on purpose: the assertions compare against it,
// and it must not depend on the String()/Sql() rendering that the change under
// test is about.
func complexNullE2ERender(v data.Data) string {
	if v == nil {
		return "null"
	}

	switch typed := v.(type) {
	case *data.Array:
		elems := make([]string, 0, typed.Len())
		for i := 0; i < typed.Len(); i++ {
			elems = append(elems, complexNullE2ERender(typed.Index(i)))
		}

		return "[" + strings.Join(elems, ",") + "]"
	case *data.Map:
		pairs := make([]string, 0, len(typed.ToGoMap()))
		for key, value := range typed.ToGoMap() {
			pairs = append(pairs, complexNullE2ERender(key)+"=>"+complexNullE2ERender(value))
		}
		sort.Strings(pairs)

		return "{" + strings.Join(pairs, ",") + "}"
	case *data.Struct:
		pairs := make([]string, 0, len(typed.Fields()))
		for _, field := range typed.Fields() {
			pairs = append(pairs, field.Name+"="+complexNullE2ERender(field.Value))
		}

		return "<" + strings.Join(pairs, ",") + ">"
	default:
		return v.Sql()
	}
}

func newNullStruct(t *testing.T, structType datatype.StructType, x int64) *data.Struct {
	t.Helper()

	s := data.NewStructWithTyp(structType)
	if err := s.SetField("x", x); err != nil {
		t.Fatalf("set x=%d: %v", x, err)
	}
	if err := s.SetField("y", nil); err != nil {
		t.Fatalf("set y null: %v", err)
	}

	return s
}

// complexNullE2ERecords returns the rows to upload and the rendering each row
// must come back with.
func complexNullE2ERecords(t *testing.T) ([]data.Record, []string) {
	t.Helper()

	arrayType := mustParseE2EDataType(t, "array<bigint>").(datatype.ArrayType)
	mapType := mustParseE2EDataType(t, "map<string,string>").(datatype.MapType)
	structType := mustParseE2EDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)
	arrayOfStructType := mustParseE2EDataType(t, "array<struct<x:bigint,y:bigint>>").(datatype.ArrayType)

	// Row 1: a NULL in every nested position the issue lists, plus a string
	// whose text is literally "NULL" so a null cannot be confused with it.
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

	nested := data.NewArrayWithType(arrayOfStructType)
	if err := nested.Append(newNullStruct(t, structType, 2)); err != nil {
		t.Fatalf("append struct with null field: %v", err)
	}
	if err := nested.Append(nil); err != nil {
		t.Fatalf("append null struct: %v", err)
	}

	withNulls := data.Record{arr, m, s, nested, data.String("NULL")}
	withNullsWant := "[1L,null,3L],{'hello'=>'a','world'=>null},<x=1L,y=null>,[<x=2L,y=null>,null],'NULL'"

	// Row 2: no NULL anywhere, so a rendering or serialization bug cannot hide
	// behind the first row.
	fullArr := data.NewArrayWithType(arrayType)
	if err := fullArr.Append(int64(7)); err != nil {
		t.Fatalf("append 7: %v", err)
	}

	fullMap := data.NewMapWithType(mapType)
	if err := fullMap.Set("k", "v"); err != nil {
		t.Fatalf("set k: %v", err)
	}

	fullStruct := data.NewStructWithTyp(structType)
	if err := fullStruct.SetField("x", int64(8)); err != nil {
		t.Fatalf("set x=8: %v", err)
	}
	if err := fullStruct.SetField("y", int64(9)); err != nil {
		t.Fatalf("set y=9: %v", err)
	}

	fullNestedElem := data.NewStructWithTyp(structType)
	if err := fullNestedElem.SetField("x", int64(10)); err != nil {
		t.Fatalf("set nested x=10: %v", err)
	}
	if err := fullNestedElem.SetField("y", int64(11)); err != nil {
		t.Fatalf("set nested y=11: %v", err)
	}

	fullNested := data.NewArrayWithType(arrayOfStructType)
	if err := fullNested.Append(fullNestedElem); err != nil {
		t.Fatalf("append nested elem: %v", err)
	}

	full := data.Record{fullArr, fullMap, fullStruct, fullNested, data.String("plain")}
	fullWant := "[7L],{'k'=>'v'},<x=8L,y=9L>,[<x=10L,y=11L>],'plain'"

	return []data.Record{withNulls, full}, []string{withNullsWant, fullWant}
}

func TestEndToEndComplexTypeWithNulls(t *testing.T) {
	if os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET") == "" {
		t.Skip("no MaxCompute credentials in the environment; this test needs a real project and tunnel endpoint")
	}

	arrayType := mustParseE2EDataType(t, "array<bigint>").(datatype.ArrayType)
	mapType := mustParseE2EDataType(t, "map<string,string>").(datatype.MapType)
	structType := mustParseE2EDataType(t, "struct<x:bigint,y:bigint>").(datatype.StructType)
	arrayOfStructType := mustParseE2EDataType(t, "array<struct<x:bigint,y:bigint>>").(datatype.ArrayType)

	builder := tableschema.NewSchemaBuilder()
	builder.Name(complexNullE2ETable).
		Column(tableschema.Column{Name: "a", Type: arrayType}).
		Column(tableschema.Column{Name: "m", Type: mapType}).
		Column(tableschema.Column{Name: "s", Type: structType}).
		Column(tableschema.Column{Name: "ns", Type: arrayOfStructType}).
		Column(tableschema.Column{Name: "txt", Type: datatype.StringType})

	if err := odpsIns.Tables().Create(builder.Build(), true, nil, nil); err != nil {
		t.Fatalf("create table %s: %v", complexNullE2ETable, err)
	}

	records, wants := complexNullE2ERecords(t)

	// Overwrite, so a rerun compares against exactly these rows.
	uploadSession, err := tunnelIns.CreateUploadSession(ProjectName, complexNullE2ETable, tunnel.SessionCfg.Overwrite())
	if err != nil {
		t.Fatalf("create upload session: %v", err)
	}

	writer, err := uploadSession.OpenRecordWriter(0)
	if err != nil {
		t.Fatalf("open record writer: %v", err)
	}

	for i, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("write record %d: %v", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close record writer: %v", err)
	}

	if err := uploadSession.Commit([]int{0}); err != nil {
		t.Fatalf("commit upload: %v", err)
	}

	downloadSession, err := tunnelIns.CreateDownloadSession(ProjectName, complexNullE2ETable)
	if err != nil {
		t.Fatalf("create download session: %v", err)
	}

	if got := downloadSession.RecordCount(); got != len(records) {
		t.Fatalf("expected %d rows in the table, got %d", len(records), got)
	}

	reader, err := downloadSession.OpenRecordReader(0, downloadSession.RecordCount(), nil)
	if err != nil {
		t.Fatalf("open record reader: %v", err)
	}

	downloaded := make([]data.Record, 0, len(records))
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read record %d: %v", len(downloaded), err)
		}

		downloaded = append(downloaded, record)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("close record reader: %v", err)
	}

	if len(downloaded) != len(records) {
		t.Fatalf("expected %d downloaded records, got %d", len(records), len(downloaded))
	}

	// A tunnel download does not guarantee row order, so compare the set of
	// renderings rather than positions.
	gotRenders := make([]string, 0, len(downloaded))
	for _, record := range downloaded {
		parts := make([]string, 0, record.Len())
		for i := 0; i < record.Len(); i++ {
			parts = append(parts, complexNullE2ERender(record[i]))
		}

		gotRenders = append(gotRenders, strings.Join(parts, ","))
	}
	sort.Strings(gotRenders)
	sort.Strings(wants)

	for i := range wants {
		if gotRenders[i] != wants[i] {
			t.Errorf("row %d changed through the real tunnel: want %s, got %s", i, wants[i], gotRenders[i])
		}
	}

	// The reported failure mode: printing what came back. A NULL nested in a
	// complex value used to take the process down here.
	texts := make([]string, 0, len(downloaded))
	for i, record := range downloaded {
		i, record := i, record
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("row %d panicked when printed: %v\n%s", i, r, debug.Stack())
				}
			}()

			texts = append(texts, record.String())
		}()
	}

	if len(texts) != len(downloaded) {
		t.Fatalf("only %d of %d rows could be printed", len(texts), len(downloaded))
	}

	// A null renders as the bare marker NULL, and a string whose text happens to
	// be "NULL" renders quoted. Exactly one row here has both, and it must not
	// be possible to read one as the other.
	withBoth := 0
	for i, text := range texts {
		quoted := strings.Contains(text, "'NULL'")
		bare := strings.Contains(strings.ReplaceAll(text, "'NULL'", ""), "NULL")
		t.Logf("row %d: quoted=%v bare=%v %s", i, quoted, bare, text)

		if quoted != bare {
			t.Errorf("row %d renders NULL and the text \"NULL\" indistinguishably (quoted=%v bare=%v): %s", i, quoted, bare, text)
		}

		if quoted && bare {
			withBoth++
		}
	}

	if withBoth != 1 {
		t.Errorf("expected exactly one downloaded row to contain both a null and the text \"NULL\", got %d", withBoth)
	}
}
