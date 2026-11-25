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

// Package livetest holds the naming rules shared by the tests that talk to a
// real MaxCompute service. It has no effect unless the live opt-in is set, and
// every function here is pure: it reads environment values and formats names,
// so the rules can be tested offline.
package livetest

import (
	"os"
	"strings"
)

// EnvRunID names the namespace for the fixtures of one test run. When it is
// set, every object the live tests create themselves is prefixed with it, so
// two runs against the same project cannot see or delete each other's objects.
// When it is unset the names stay exactly as they have always been, which
// keeps a run working against fixtures a human created earlier.
const EnvRunID = "MAXCOMPUTE_LIVE_RUN_ID"

// EnvKeepFixtures, set to "1", leaves run-scoped fixtures in place after the
// live tests finish. It exists for debugging a failed run; nothing reads it in
// this package, the live test binaries do.
const EnvKeepFixtures = "MAXCOMPUTE_LIVE_KEEP_FIXTURES"

// maxPrefixLength bounds the namespace part of a name. MaxCompute allows 128
// characters in an identifier and the longest base below is 44, so the sum
// always fits.
const maxPrefixLength = 40

// Bases lists every object name that the live tests create themselves, spelled
// exactly as the tests spell it, because that is how the guard test finds it.
// Objects that only exist because someone made them in the regression project
// (the "mv" and "tunnel" schemas, the "test" and "testtable" tables read by the
// examples) are deliberately absent: renaming those would break the tests that
// read them, so they stay bare and are documented where they are used. The list
// does carry not_exists, which nothing creates: it is the object a test asserts
// is absent, and only a run-scoped name makes that assertion mean what it says.
//
// internal/livetest/fixtures_test.go pins this list against the sites that use
// it, and odps/live_fixture_guard_test.go fails if any of these names appears
// as a literal in a live-path file.
func Bases() []string {
	return []string{
		"data_type_demo",
		"ExampleTunnel_Preview",
		"exist_schema",
		"has_struct",
		"new_schema",
		"not_exists",
		"odps_sdk_geography_test_go",
		"overwrite_test",
		"sale_detail",
		"simple_struct",
		"table1",
		"table2",
		"to_delete_schema",
		"TestStreamUploadSessionTest_SchemaEvolution",
		"tunnel_complex_null_e2e",
		"upload_create_partition_test",
		"upload_sample_arrow",
		"user",
		"user_temp",
	}
}

// RunID returns the namespace for this run, or "" when the run is configured
// to use the shared legacy names. RunIDFrom documents where the value comes
// from.
func RunID() string {
	return RunIDFrom(os.LookupEnv)
}

// Fixture returns the name this run uses for the fixture identified by base.
// With no namespace configured the name is returned unchanged.
func Fixture(base string) string {
	return FixtureFrom(os.LookupEnv, base)
}

// RunIDFrom reports the fixture namespace held by lookup, in this order:
//
//  1. MAXCOMPUTE_LIVE_RUN_ID, for a local run or any runner the SDK does not
//     know about;
//  2. GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT and GITHUB_JOB joined, because matrix
//     jobs inside one workflow run share the first two and would otherwise
//     collide with each other;
//  3. empty, meaning the shared legacy names.
//
// Each candidate is sanitized (see FixtureFrom) and a candidate that sanitizes
// to nothing is treated as unset.
func RunIDFrom(lookup func(key string) (string, bool)) string {
	if v, ok := lookup(EnvRunID); ok {
		if s := sanitizePrefix(v); s != "" {
			return s
		}
	}

	runID, ok := lookup("GITHUB_RUN_ID")
	if !ok {
		return ""
	}
	attempt, _ := lookup("GITHUB_RUN_ATTEMPT")
	job, _ := lookup("GITHUB_JOB")
	return sanitizePrefix(strings.Join([]string{runID, attempt, job}, "-"))
}

// FixtureFrom formats base with the namespace held by lookup. A name is built
// as "<namespace>_<base>", which stays a valid MaxCompute identifier because
// the namespace is sanitized to lower case letters, digits and underscores and
// is capped at maxPrefixLength characters.
//
// With no namespace, base is returned byte for byte: that is what keeps the
// live tests pointed at fixtures a human created before namespaces existed.
func FixtureFrom(lookup func(key string) (string, bool), base string) string {
	prefix := RunIDFrom(lookup)
	if prefix == "" {
		return base
	}
	return prefix + "_" + base
}

// sanitizePrefix maps arbitrary runner identifiers onto the character set
// MaxCompute accepts in an identifier. Anything that is not a letter, a digit
// or an underscore becomes an underscore, runs of underscores collapse, and
// leading or trailing ones are dropped. A result that starts with a digit gets
// an "r" in front of it, and an empty result means "no namespace".
func sanitizePrefix(raw string) string {
	var b strings.Builder
	lastUnderscore := false

	for _, r := range strings.ToLower(raw) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}

	out := strings.Trim(b.String(), "_")
	if out == "" {
		return ""
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "r" + out
	}
	if len(out) > maxPrefixLength {
		out = strings.Trim(out[:maxPrefixLength], "_")
	}
	return out
}
