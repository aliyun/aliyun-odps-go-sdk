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
	"bytes"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestGeographyType(t *testing.T) {
	g := Geography{0x01, 0x02, 0x03}
	if g.Type().ID() != datatype.GEOGRAPHY {
		t.Fatalf("Geography.Type().ID() = %v, want GEOGRAPHY", g.Type().ID())
	}
}

func TestGeographyAsBinary(t *testing.T) {
	wkb := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	g := Geography(wkb)
	if !bytes.Equal(g.AsBinary(), wkb) {
		t.Fatalf("AsBinary() = %v, want %v", g.AsBinary(), wkb)
	}
}

func TestGeographyString(t *testing.T) {
	g := Geography{0xAB, 0xCD}
	got := g.String()
	if got != "unhex('ABCD')" {
		t.Fatalf("String() = %q, want \"unhex('ABCD')\"", got)
	}
}

func TestGeographySql(t *testing.T) {
	g := Geography{0xAB, 0xCD}
	got := g.Sql()
	want := "ST_GEOGFROMWKB(unhex('ABCD'))"
	if got != want {
		t.Fatalf("Sql() = %q, want %q", got, want)
	}
	if !strings.Contains(got, "ST_GEOGFROMWKB") {
		t.Fatal("Sql() must reference ST_GEOGFROMWKB")
	}
}

func TestGeographyScanFromGeography(t *testing.T) {
	src := Geography{1, 2, 3}
	var dst Geography
	if err := dst.Scan(src); err != nil {
		t.Fatalf("Scan err = %v", err)
	}
	if !bytes.Equal(dst, src) {
		t.Fatalf("after Scan: dst = %v, want %v", dst, src)
	}
}

func TestGeographyScanFromBytes(t *testing.T) {
	// []byte is unnamed and assignable to Geography (named []byte) via reflect.AssignableTo.
	raw := []byte{4, 5, 6}
	var dst Geography
	if err := dst.Scan(raw); err != nil {
		t.Fatalf("Scan from []byte err = %v", err)
	}
	if !bytes.Equal(dst, raw) {
		t.Fatalf("after Scan: dst = %v, want %v", dst, raw)
	}
}
