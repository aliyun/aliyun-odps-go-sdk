# Geography Type Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add MaxCompute `GEOGRAPHY` type support to the Go SDK (mirroring Java SDK's `RawGeographyObject`): a raw-WKB-bytes wrapper, parser/type-system entries, and protobuf tunnel read/write paths.

**Architecture:** Three layers, each isolated and testable: (1) `odps/datatype` — register the new TypeID and primitive singleton; (2) `odps/data` — `Geography` type wrapping `[]byte` (WKB), implementing `Data`; (3) `odps/tunnel` — protobuf reader/writer cases that mirror existing `Binary`/`Json` handling. No third-party geometry library, no Arrow path, no SQL driver changes.

**Tech Stack:** Go (existing toolchain in repo), no new dependencies. Existing test infra: `go test ./...`. Reference spec: `docs/superpowers/specs/2026-05-22-geography-type-support-design.md`.

---

## Pre-flight

- Repo root: `/Users/dingxin/GolandProjects/odps-sdk-go`
- All `go test` / `go build` commands run from repo root unless noted
- Reference Java implementation: `/Users/dingxin/IdeaProjects/odps/odps-sdk-java/odps-sdk/odps-sdk-commons/src/main/java/com/aliyun/odps/data/RawGeographyObject.java` and `ProtobufRecordStreamReader.java:418-420`
- The repo currently has **uncommitted changes** unrelated to this plan (see `git status` at session start: `arrow/array/numeric.gen.go*`, untracked `analysis.md`, etc.). Do NOT stage them. Each commit must explicitly add only the files this plan touches.

---

## File Structure

| File | Role |
|---|---|
| `odps/datatype/data_type.go` | (modify) Add `GEOGRAPHY` to TypeID iota, str↔TypeID maps, and `GeographyType` singleton |
| `odps/datatype/data_type_test.go` | (modify) Add test for `TypeCodeFromStr("GEOGRAPHY")` and `String()` round-trip |
| `odps/datatype/data_type_parser_test.go` | (modify or create) Test `ParseDataType("GEOGRAPHY")` |
| `odps/data/geography.go` | (create) `type Geography []byte` + `Type()` / `String()` / `Sql()` / `Scan()` / `AsBinary()` |
| `odps/data/data_scan_test.go` | (modify) Add `Geography` round-trip case |
| `odps/data/geography_test.go` | (create) Unit tests for Geography methods |
| `odps/tunnel/record_protoc_reader.go` | (modify, ~line 242 area) Add `case datatype.GEOGRAPHY:` |
| `odps/tunnel/record_protoc_writer.go` | (modify, ~line 140 area + writeField switch) Add GEOGRAPHY to BytesType list and `case data.Geography:` |
| `odps/tunnel/protoc_writer_test.go` | (modify or new test func) Geography round-trip via `newRecordProtocWriter` |

No changes to `data_conversion.go` or `util.go` — see spec section "类型转换" for rationale.

---

## Task 1: Register GEOGRAPHY in datatype package

**Files:**
- Modify: `odps/datatype/data_type.go`
- Test: `odps/datatype/data_type_test.go`

This task adds the type identifier and the `GeographyType` primitive singleton. Pure additions — no behavior change for existing types. Verifying with tests catches typos in the str↔ID maps.

- [ ] **Step 1: Write the failing test**

Open `odps/datatype/data_type_test.go` and append:

```go
func TestGeographyTypeIDRoundTrip(t *testing.T) {
	got := TypeCodeFromStr("GEOGRAPHY")
	if got != GEOGRAPHY {
		t.Fatalf("TypeCodeFromStr(\"GEOGRAPHY\") = %v, want GEOGRAPHY", got)
	}
	if got.String() != "GEOGRAPHY" {
		t.Fatalf("GEOGRAPHY.String() = %q, want \"GEOGRAPHY\"", got.String())
	}
	if TypeCodeFromStr("geography") != GEOGRAPHY {
		t.Fatal("TypeCodeFromStr should be case-insensitive for GEOGRAPHY")
	}
}

func TestGeographyTypeSingleton(t *testing.T) {
	if GeographyType.ID() != GEOGRAPHY {
		t.Fatalf("GeographyType.ID() = %v, want GEOGRAPHY", GeographyType.ID())
	}
	if GeographyType.Name() != "GEOGRAPHY" {
		t.Fatalf("GeographyType.Name() = %q, want \"GEOGRAPHY\"", GeographyType.Name())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./odps/datatype/ -run TestGeography -v`
Expected: FAIL with `undefined: GEOGRAPHY` and `undefined: GeographyType`.

- [ ] **Step 3: Add the TypeID constant**

In `odps/datatype/data_type.go`, modify the iota block (currently ends with `JSON / TypeUnknown`). Insert `GEOGRAPHY` **between `JSON` and `TypeUnknown`** so any existing references to `TypeUnknown`'s numeric value (none expected, but defensive) shift only at the very end:

```go
const (
	NULL TypeID = iota
	BIGINT
	DOUBLE
	BOOLEAN
	DATETIME
	STRING
	DECIMAL
	MAP
	ARRAY
	VOID
	TINYINT
	SMALLINT
	INT
	FLOAT
	CHAR
	VARCHAR
	DATE
	TIMESTAMP
	TIMESTAMP_NTZ
	BINARY
	IntervalDayTime
	IntervalYearMonth
	STRUCT
	JSON
	GEOGRAPHY
	TypeUnknown
)
```

- [ ] **Step 4: Add string mappings**

In the same file, add a case to `TypeCodeFromStr` (between the `JSON` case and `default`):

```go
	case "GEOGRAPHY":
		return GEOGRAPHY
```

And to `String()` (between the `JSON` case and `default`):

```go
	case GEOGRAPHY:
		return "GEOGRAPHY"
```

- [ ] **Step 5: Add the GeographyType singleton**

In the `var ( ... )` block at the bottom of `data_type.go` (currently ends with `NullType`), add:

```go
	GeographyType         = PrimitiveType{GEOGRAPHY}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./odps/datatype/ -run TestGeography -v`
Expected: PASS for both `TestGeographyTypeIDRoundTrip` and `TestGeographyTypeSingleton`.

- [ ] **Step 7: Run the full datatype suite to catch regressions**

Run: `go test ./odps/datatype/...`
Expected: All existing tests still pass.

- [ ] **Step 8: Commit**

```bash
git add odps/datatype/data_type.go odps/datatype/data_type_test.go
git commit -m "feat(datatype): register GEOGRAPHY TypeID and GeographyType singleton"
```

---

## Task 2: Verify ParseDataType("GEOGRAPHY") works

**Files:**
- Test: `odps/datatype/data_type_test.go` (or `data_type_parser_test.go` if it exists — check first)

The parser's `default` branch falls through to `newPrimitive`, so this should already work after Task 1. We verify with a test rather than guessing.

- [ ] **Step 1: Check whether `data_type_parser_test.go` exists**

Run: `ls odps/datatype/data_type_parser_test.go 2>&1`
- If it exists, append the test there.
- If not, append to `data_type_test.go`.

- [ ] **Step 2: Write the test**

```go
func TestParseGeography(t *testing.T) {
	dt, err := ParseDataType("GEOGRAPHY")
	if err != nil {
		t.Fatalf("ParseDataType(\"GEOGRAPHY\") err = %v", err)
	}
	if dt.ID() != GEOGRAPHY {
		t.Fatalf("ParseDataType(\"GEOGRAPHY\").ID() = %v, want GEOGRAPHY", dt.ID())
	}
	// Case-insensitive variant
	if _, err := ParseDataType("geography"); err != nil {
		t.Fatalf("ParseDataType(\"geography\") err = %v", err)
	}
}
```

- [ ] **Step 3: Run the test**

Run: `go test ./odps/datatype/ -run TestParseGeography -v`
Expected: PASS (no implementation change needed — parser delegates to `newPrimitive` for primitives without parameters).

- [ ] **Step 4: If it fails**, only then modify `parse()` in `data_type_parser.go`. Most likely: PASS as-is.

- [ ] **Step 5: Commit**

```bash
git add odps/datatype/  # only the test file changed; verify with git status
git commit -m "test(datatype): cover ParseDataType for GEOGRAPHY"
```

---

## Task 3: Implement data.Geography type

**Files:**
- Create: `odps/data/geography.go`
- Create: `odps/data/geography_test.go`

Models WKB-bearing Geography column data, paralleling `data/binary.go` and `data/json.go`.

- [ ] **Step 1: Write the failing test**

Create `odps/data/geography_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./odps/data/ -run TestGeography -v`
Expected: compilation error `undefined: Geography`.

- [ ] **Step 3: Implement Geography**

Create `odps/data/geography.go`:

```go
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
	"fmt"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// Geography 承载 MaxCompute GEOGRAPHY 列的 WKB (Well-Known Binary) 字节。
// 不内置 WKT/WKB 互转；如需 WKT 请自行使用 paulmach/orb 或 twpayne/go-geom 解析。
type Geography []byte

func (g Geography) Type() datatype.DataType {
	return datatype.GeographyType
}

// AsBinary 返回 WKB 字节。
func (g Geography) AsBinary() []byte {
	return []byte(g)
}

func (g Geography) String() string {
	return fmt.Sprintf("unhex('%X')", []byte(g))
}

// Sql 返回可直接拼入 SQL 字面量的表达式。
func (g Geography) Sql() string {
	return fmt.Sprintf("ST_GEOGFROMWKB(unhex('%X'))", []byte(g))
}

func (g *Geography) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, g))
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./odps/data/ -run TestGeography -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run the full data package suite to catch regressions**

Run: `go test ./odps/data/...`
Expected: existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add odps/data/geography.go odps/data/geography_test.go
git commit -m "feat(data): add Geography type for GEOGRAPHY columns"
```

---

## Task 4: Add Geography to data_scan_test round-trip

**Files:**
- Modify: `odps/data/data_scan_test.go:39-50` (the `values` slice)

Existing test exercises the generic `tryConvertType` path for every supported type. Adding Geography to the same table proves it integrates with the same machinery (and catches future regressions if someone refactors `tryConvertType`).

- [ ] **Step 1: Add a Geography case to the `values` slice**

In `odps/data/data_scan_test.go`, locate the `values` slice (around line 39-50). Append:

```go
		{Geography([]byte{0xAB, 0xCD}), new(Geography)},
```

- [ ] **Step 2: Run the test**

Run: `go test ./odps/data/ -run TestDataScan -v`
Expected: PASS (the loop calls `src.String() != dst.String()`, which compares `unhex('ABCD')` to itself).

- [ ] **Step 3: Commit**

```bash
git add odps/data/data_scan_test.go
git commit -m "test(data): cover Geography in tryConvertType round-trip"
```

---

## Task 5: protobuf tunnel reader supports GEOGRAPHY

**Files:**
- Modify: `odps/tunnel/record_protoc_reader.go` (around line 242, the BINARY case is the closest sibling)

Mirror Java SDK's `ProtobufRecordStreamReader.java:418-420`: read raw bytes, update CRC, wrap in `data.Geography`.

- [ ] **Step 1: Locate the switch statement**

Run: `grep -n "case datatype.BINARY" odps/tunnel/record_protoc_reader.go`
Expected: a single line around 242.

- [ ] **Step 2: Add the GEOGRAPHY case**

Insert directly after the `case datatype.JSON:` block (around lines 338-349). Order in the file is unimportant for behavior; the BINARY-style block reads ~6 lines:

```go
	case datatype.GEOGRAPHY:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		fieldValue = data.Geography(v)
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./odps/tunnel/...`
Expected: success.

- [ ] **Step 4: Run the existing tunnel tests to catch regressions**

Run: `go test ./odps/tunnel/... -count=1`
Expected: all existing tests still pass (no Geography test yet — that's Task 7).

- [ ] **Step 5: Commit**

```bash
git add odps/tunnel/record_protoc_reader.go
git commit -m "feat(tunnel): protobuf reader handles GEOGRAPHY columns"
```

---

## Task 6: protobuf tunnel writer supports GEOGRAPHY

**Files:**
- Modify: `odps/tunnel/record_protoc_writer.go` (~line 140 wireType list, and the `writeField` type switch)

Two edits: declare GEOGRAPHY's wireType (BytesType, same as Binary/Json) and add a `data.Geography` case to `writeField`.

- [ ] **Step 1: Add GEOGRAPHY to BytesType wireType list**

Locate the `case datatype.IntervalDayTime, datatype.TIMESTAMP, ... datatype.JSON:` block ending around line 145. Insert `datatype.GEOGRAPHY,` into that comma list (place it next to `datatype.JSON` for readability):

```go
	case datatype.IntervalDayTime,
		datatype.TIMESTAMP,
		datatype.TIMESTAMP_NTZ,
		datatype.STRING,
		datatype.CHAR,
		datatype.VARCHAR,
		datatype.BINARY,
		datatype.DECIMAL,
		datatype.ARRAY,
		datatype.MAP,
		datatype.STRUCT,
		datatype.JSON,
		datatype.GEOGRAPHY:
		wireType = protowire.BytesType
```

- [ ] **Step 2: Add data.Geography case to writeField**

In `writeField`, find the `case data.Binary:` block (around line 209-211) and add immediately after it:

```go
	case data.Geography:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteBytes(val))
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./odps/tunnel/...`
Expected: success.

- [ ] **Step 4: Run existing tunnel tests**

Run: `go test ./odps/tunnel/... -count=1`
Expected: all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add odps/tunnel/record_protoc_writer.go
git commit -m "feat(tunnel): protobuf writer handles GEOGRAPHY columns"
```

---

## Task 7: tunnel round-trip test for Geography

**Files:**
- Modify: `odps/tunnel/protoc_writer_test.go` (add a new top-level test function)

End-to-end test: write a record with a Geography column through the protoc writer, then read it back through the protoc reader. Validates the wireType, byte-level encoding, and the writer↔reader contract together. We do NOT pin exact bytes (unlike the existing fixture-based tests) because the round-trip equality is the contract we care about.

**Pre-verified facts about the API** (so you don't have to discover them):
- Writer constructor: `newRecordProtocWriter(bw io.Writer, columns []tableschema.Column, shouldTransformDate bool) RecordProtocWriter` — same as existing tests use.
- Reader constructor: `newRecordProtocReader(httpRes *http.Response, columns []tableschema.Column, shouldTransformDate bool) RecordProtocReader` (`odps/tunnel/record_protoc_reader.go:41`). It reads from `httpRes.Body`. **There is no `io.Reader`-based constructor and no test file for the reader in this package.** Real callers (e.g. `odps/tunnel/download_session.go:252`) pass a real `*http.Response`. For the test, wrap a buffer in a fake `*http.Response`.
- The reader's `Read()` returns `(data.Record, error)`. `data.Record.Get(i)` returns `data.Data` (interface).
- The reader writes **values** (not pointers) for primitive types — e.g. STRING column → `data.String` (value), BINARY column → `data.Binary` (value). Type-assert to value, not pointer.

- [ ] **Step 1: Write the round-trip test**

Append to `odps/tunnel/protoc_writer_test.go`. Note the new imports `io`, `net/http`:

```go
func TestProtocGeographyRoundTrip(t *testing.T) {
	bw := &bufWriter{bytes.NewBuffer(nil)}

	columns := []tableschema.Column{
		{Name: "desc", Type: datatype.StringType},
		{Name: "geo", Type: datatype.GeographyType},
	}

	pw := newRecordProtocWriter(bw, columns, false)

	desc := data.String("Point")
	// POINT(10 20) in little-endian WKB. Treated here as opaque bytes —
	// the test does not need a real geometry parser.
	wkb := []byte{
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
	}
	geo := data.Geography(wkb)

	record := []data.Data{&desc, geo}

	if err := pw.Write(record); err != nil {
		t.Fatalf("Write err = %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}

	// newRecordProtocReader takes *http.Response; fabricate one wrapping the
	// writer's output buffer.
	fakeRes := &http.Response{
		Body: io.NopCloser(bytes.NewReader(bw.buf.Bytes())),
	}
	pr := newRecordProtocReader(fakeRes, columns, false)

	gotRecord, err := pr.Read()
	if err != nil {
		t.Fatalf("Read err = %v", err)
	}

	gotDesc, ok := gotRecord.Get(0).(data.String)
	if !ok {
		t.Fatalf("desc column type = %T, want data.String", gotRecord.Get(0))
	}
	if string(gotDesc) != "Point" {
		t.Fatalf("desc round-trip: got %q, want \"Point\"", string(gotDesc))
	}

	gotGeo, ok := gotRecord.Get(1).(data.Geography)
	if !ok {
		t.Fatalf("geo column type = %T, want data.Geography", gotRecord.Get(1))
	}
	if !bytes.Equal(gotGeo, wkb) {
		t.Fatalf("geo bytes mismatch: got %x, want %x", []byte(gotGeo), wkb)
	}
}
```

Add the new imports to the file's import block:

```go
import (
	"bytes"
	"fmt"
	"io"        // new
	"net/http" // new
	"reflect"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)
```

- [ ] **Step 2: Run the test**

Run: `go test ./odps/tunnel/ -run TestProtocGeographyRoundTrip -v`
Expected: PASS.

If you get a `nil pointer` panic from inside `Read()`, it most likely means a CRC mismatch path triggered. Re-check that the writer's `Close()` was called before reading and that no extra trailing bytes were lost.

If a type assertion fails, `t.Fatalf` will print the actual type — match the assertion to it. Both observed types in the codebase are values; a pointer would be a surprise.

- [ ] **Step 3: Run the full tunnel test suite**

Run: `go test ./odps/tunnel/... -count=1`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add odps/tunnel/protoc_writer_test.go
git commit -m "test(tunnel): protobuf round-trip for GEOGRAPHY columns"
```

---

## Task 8: Final verification across the repo

**Files:** none (verification only)

- [ ] **Step 1: Build the whole module**

Run: `go build ./...`
Expected: success, no errors.

- [ ] **Step 2: Test the whole module**

Run: `go test ./odps/... -count=1`
Expected: all tests pass. (We deliberately scope to `./odps/...` because `arrow/` is unrelated and has uncommitted modifications from before this session per `git status`.)

- [ ] **Step 3: Verify git history is tidy**

Run: `git log --oneline -8`
Expected: 6-7 small commits, each scoped to a single layer (datatype, data, tunnel reader, tunnel writer, tests).

- [ ] **Step 4: Skim git diff against master to confirm no stray changes**

Run: `git diff master --stat`
Expected: only files listed in the "File Structure" table above.

If extra files appear (e.g. anything under `arrow/`, root-level `*.md`), that's the pre-existing uncommitted state from session start — leave it alone, do NOT include in any commit.

---

## Done criteria

- [ ] All 8 tasks completed and committed
- [ ] `go test ./odps/...` passes
- [ ] `git diff master --stat` shows only files in the File Structure table
- [ ] No new third-party dependencies introduced (run `git diff master -- go.mod go.sum` → empty)
- [ ] Spec accurately predicts the final code (cross-check `docs/superpowers/specs/2026-05-22-geography-type-support-design.md`)
