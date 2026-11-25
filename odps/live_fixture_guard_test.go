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

package odps_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
)

// This test runs offline: it reads source files, never a service. It exists
// because a fixture name that is spelled out in a test body cannot be given a
// run namespace, and the next person to add a live case will not remember
// which names are already taken.

// livePathFiles are the test files that reach a real MaxCompute service,
// either through the live gate or through the package-level client that the
// live setup configures. A file belongs here when turning MAXCOMPUTE_LIVE_TESTS
// on makes its code send requests; that includes examples without an
// "// Output:" comment, which go test compiles but does not run today.
var livePathFiles = []string{
	"odps/example_column_test.go",
	"odps/example_instance_test.go",
	"odps/example_odps_test.go",
	"odps/example_partition_test.go",
	"odps/example_project_test.go",
	"odps/example_table_test.go",
	"odps/example_tables_test.go",
	"odps/example_test.go",
	"odps/schemas_test.go",
	"odps/live_fixture_wire_test.go",
	"odps/tables_test.go",
	"odps/tenant_test.go",
	"odps/security/example_manager_test.go",
	"odps/tunnel/example_instance_result_download_test.go",
	"odps/tunnel/example_test.go",
	"odps/tunnel/example_tunnel_test.go",
	"odps/tunnel/geography_ft_test.go",
	"odps/tunnel/record_complex_null_e2e_test.go",
	"odps/tunnel/stream_upload_session_test.go",
	"odps/tunnel/upload_session_test.go",
	"sqldriver/context_real_service_test.go",
	"sqldriver/example_test.go",
	"sqldriver/retry_credential_real_service_test.go",
}

// instrumentationFiles are the two files that describe the naming rule instead
// of using it: the guard below and the wire probe that lists the fixture names
// as data. They are exempt from the literal scan for that reason, and a name
// added to the rule has to appear in them on purpose.
var instrumentationFiles = []string{
	"odps/live_fixture_guard_test.go",
	"odps/live_fixture_wire_test.go",
}

// liveGateMarkers identify the files that opt in to a real service. A new file
// using one of them has to be added to livePathFiles, so the list cannot rot.
var liveGateMarkers = []string{
	"requireLiveService(",
	"requireLiveTunnel(",
	"liveServiceEnabled(",
	"realServiceConfig(",
	"realServiceSetup(",
	`"MAXCOMPUTE_LIVE_TESTS"`,
}

// sourceOfCall strips the calls that ask internal/livetest for the name, so the
// guard only reports a literal that reaches the service without going through it.
var sourceOfCall = regexp.MustCompile(`livetest\.(Fixture|RunID)\(\s*"[^"]*"\s*\)`)

// sqlShaped matches the string literals that carry a name into the service as
// text rather than as an argument: a bare "user" inside
// "select * from user;" is the same hazard as Table("user").
var sqlShaped = regexp.MustCompile(`(?i)\b(create|drop|alter|truncate)\b.*\btable\b|\b(insert|update|delete|select)\b`)

func TestLiveFixtureNamesComeFromTheSingleSource(t *testing.T) {
	root := moduleRoot(t)
	bases := livetest.Bases()
	violations := []string{}

	for _, rel := range livePathFiles {
		if contains(instrumentationFiles, rel) {
			continue
		}
		path := filepath.Join(root, rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("declared live-path file %s: %v", rel, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			code := strings.SplitN(line, "//", 2)[0]
			// A name handed to the single source is the rule being followed, not
			// a spelling that escaped it.
			code = sourceOfCall.ReplaceAllString(code, "")
			for _, literal := range stringLiterals(code) {
				for _, base := range bases {
					exact := literal == base
					inSQL := sqlShaped.MatchString(literal) &&
						regexp.MustCompile(`(^|[^A-Za-z0-9_])`+regexp.QuoteMeta(base)+`($|[^A-Za-z0-9_])`).MatchString(literal)
					if exact || inSQL {
						violations = append(violations, rel+":"+itoa(i+1)+` spells "`+literal+`" instead of livetest.Fixture(...)`)
					}
				}
			}
		}
	}

	undeclared := []string{}
	for _, rel := range testFiles(t, root) {
		if contains(instrumentationFiles, rel) || contains(livePathFiles, rel) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, marker := range liveGateMarkers {
			if strings.Contains(string(data), marker) {
				undeclared = append(undeclared, rel+" (uses "+marker+")")
				break
			}
		}
	}

	for _, v := range violations {
		t.Error(v)
	}
	for _, u := range undeclared {
		t.Error("live-path file missing from livePathFiles: " + u)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not walk up to the module root from the test's working directory")
	return ""
}

func testFiles(t *testing.T, root string) []string {
	t.Helper()

	found := []string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "arrow", "cdata", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(found) < 50 {
		t.Fatalf("walk found %d test files, which is too few to trust", len(found))
	}
	return found
}

// stringLiterals returns the contents of the double-quoted literals on one
// source line, in order. Back quotes are handled by the caller's regexes
// rather than here because no fixture name is written that way.
func stringLiterals(code string) []string {
	out := []string{}
	for i := 0; i < len(code); {
		if code[i] != '"' {
			i++
			continue
		}
		j := i + 1
		for j < len(code) {
			if code[j] == '\\' {
				j += 2
				continue
			}
			if code[j] == '"' {
				break
			}
			j++
		}
		if j >= len(code) {
			break
		}
		out = append(out, unquoteEscapes(code[i+1:j]))
		i = j + 1
	}
	return out
}

func unquoteEscapes(s string) string {
	r := strings.NewReplacer(`\"`, `"`, `\\`, `\`, `\n`, " ", `\t`, " ")
	return r.Replace(s)
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}
