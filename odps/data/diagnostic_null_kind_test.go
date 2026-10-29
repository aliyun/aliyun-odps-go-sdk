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
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// There are two ways to carry "no value" in this package: a nil Data interface, and the
// explicit data.Null (a non-nil NullData{} whose String() is "NULL"). Both must keep the
// bare NULL marker in the diagnostic rendering; only a value that happens to *print* as
// the marker text - data.String("NULL") - gets quoted. Quoting data.Null would make an
// explicit null indistinguishable from that string, which is the opposite of what the
// quoting rule is for.

func TestDiagnosticNullKindsAcrossContainers(t *testing.T) {
	stringArray := datatype.NewArrayType(datatype.StringType)

	arr := NewArrayWithType(stringArray)
	if err := arr.Append(nil); err != nil {
		t.Fatalf("append nil: %v", err)
	}
	if err := arr.Append(Null); err != nil {
		t.Fatalf("append data.Null: %v", err)
	}
	if err := arr.Append(String("NULL")); err != nil {
		t.Fatalf("append the string NULL: %v", err)
	}

	st := NewStruct()
	if err := st.SetField("a", nil); err != nil {
		t.Fatalf("set a nil: %v", err)
	}
	if err := st.SetField("b", Null); err != nil {
		t.Fatalf("set b data.Null: %v", err)
	}
	if err := st.SetField("c", String("NULL")); err != nil {
		t.Fatalf("set c the string NULL: %v", err)
	}

	rec := Record{nil, Null, String("NULL")}

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"array", arr.String(), "array(NULL, NULL, 'NULL')"},
		{"struct", st.String(), "struct<a:NULL,b:NULL,c:'NULL'>"},
		{"record", rec.String(), "[NULL, NULL, 'NULL']"},
	}

	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s renders nulls wrong: want %s, got %s", c.name, c.want, c.got)
		}
	}
}

// The counting form of the same contract, so a change that makes both null kinds render
// as quoted text cannot pass by accident: two bare markers, one quoted literal.
func TestDiagnosticCountsOfNullMarkers(t *testing.T) {
	stringArray := datatype.NewArrayType(datatype.StringType)

	arr := NewArrayWithType(stringArray)
	if err := arr.Append(nil); err != nil {
		t.Fatalf("append nil: %v", err)
	}
	if err := arr.Append(Null); err != nil {
		t.Fatalf("append data.Null: %v", err)
	}
	if err := arr.Append(String("NULL")); err != nil {
		t.Fatalf("append the string NULL: %v", err)
	}
	if err := arr.Append(String("plain")); err != nil {
		t.Fatalf("append plain: %v", err)
	}

	text := arr.String()
	quoted := strings.Count(text, "'"+NULLDiagnostic+"'")
	bare := strings.Count(strings.ReplaceAll(text, "'"+NULLDiagnostic+"'", ""), NULLDiagnostic)
	t.Logf("array rendering %q: quoted=%d bare=%d", text, quoted, bare)

	if quoted != 1 {
		t.Errorf("want exactly one quoted %s (the literal string), got %d in %s", NULLDiagnostic, quoted, text)
	}
	if bare != 2 {
		t.Errorf("want exactly two bare %s (nil and data.Null), got %d in %s", NULLDiagnostic, bare, text)
	}
	if !strings.Contains(text, "plain") {
		t.Errorf("the unrelated string value disappeared from %s", text)
	}
}

// Sql() is the other rendering, and it predates the diagnostic rule. This pins that a
// null and the literal text "NULL" cannot be confused there either. Note what it also
// records: Sql() renders a nil element as lowercase "null" while data.Null.Sql() is
// uppercase "NULL" - two spellings for the same idea inside one function. That is
// pre-existing behaviour, not something this change introduces or fixes, so the
// assertion below is written against what the code does today and the inconsistency is
// raised for review separately rather than silently normalised here.
func TestDiagnosticSqlKeepsNullKindsDistinct(t *testing.T) {
	stringArray := datatype.NewArrayType(datatype.StringType)

	arr := NewArrayWithType(stringArray)
	if err := arr.Append(nil); err != nil {
		t.Fatalf("append nil: %v", err)
	}
	if err := arr.Append(Null); err != nil {
		t.Fatalf("append data.Null: %v", err)
	}
	if err := arr.Append(String("NULL")); err != nil {
		t.Fatalf("append the string NULL: %v", err)
	}

	text := arr.Sql()
	t.Logf("array Sql() = %s", text)

	if strings.Count(text, "'NULL'") != 1 {
		t.Errorf("the literal string must stay quoted in Sql(), got %s", text)
	}
	// Both null kinds must be recognisable as nulls, whatever their spelling.
	if got := strings.Count(text, "null") + strings.Count(strings.ReplaceAll(text, "'NULL'", ""), "NULL"); got != 2 {
		t.Errorf("want two null spellings for the two null kinds in Sql(), got %d in %s", got, text)
	}
}
