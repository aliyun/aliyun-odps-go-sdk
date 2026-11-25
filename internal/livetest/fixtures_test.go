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

package livetest

import (
	"regexp"
	"strings"
	"testing"
)

func env(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

// TestUnsetNamespaceKeepsTheLegacyNames is the compatibility half of the rule:
// with no namespace the names must not change by one byte, because live runs
// against fixtures that were created before namespacing existed have to keep
// working.
func TestUnsetNamespaceKeepsTheLegacyNames(t *testing.T) {
	for _, lookup := range []func(string) (string, bool){env(map[string]string{}), env(map[string]string{EnvRunID: "   "})} {
		if got := RunIDFrom(lookup); got != "" {
			t.Fatalf("RunIDFrom() = %q, want empty", got)
		}
		for _, base := range Bases() {
			if got := FixtureFrom(lookup, base); got != base {
				t.Errorf("FixtureFrom(_, %q) = %q, want the name unchanged", base, got)
			}
		}
	}
}

func TestExplicitRunIDNamespacesEveryFixture(t *testing.T) {
	lookup := env(map[string]string{EnvRunID: "nightly-7"})

	if got := RunIDFrom(lookup); got != "nightly_7" {
		t.Fatalf("RunIDFrom() = %q, want nightly_7", got)
	}
	for _, base := range Bases() {
		want := "nightly_7_" + base
		if got := FixtureFrom(lookup, base); got != want {
			t.Errorf("FixtureFrom(_, %q) = %q, want %q", base, got, want)
		}
	}
}

// TestGitHubIdentitySeparatesRunsAndJobs covers why the Actions fallback folds
// in the attempt and the job: two runs of the same workflow must not share a
// namespace, and neither must two jobs of one run, because a workflow_dispatch
// live run and a push run can overlap.
func TestGitHubIdentitySeparatesRunsAndJobs(t *testing.T) {
	cases := []struct {
		name   string
		env    map[string]string
		prefix string
	}{
		{"run and job", map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_JOB": "live"}, "r1234_live"},
		{"attempt separates a rerun", map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_JOB": "live"}, "r1234_2_live"},
		{"job separates matrix legs", map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_JOB": "live-tunnel"}, "r1234_live_tunnel"},
		{"run id alone still namespaces", map[string]string{"GITHUB_RUN_ID": "1234"}, "r1234"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RunIDFrom(env(c.env)); got != c.prefix {
				t.Errorf("RunIDFrom() = %q, want %q", got, c.prefix)
			}
		})
	}

	first := env(map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_JOB": "live"})
	second := env(map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_JOB": "live"})
	if RunIDFrom(first) == RunIDFrom(second) {
		t.Error("a rerun must not reuse the previous attempt's namespace")
	}
}

func TestExplicitRunIDWinsOverGitHub(t *testing.T) {
	lookup := env(map[string]string{
		EnvRunID:        "mine",
		"GITHUB_RUN_ID": "1234",
		"GITHUB_JOB":    "live",
	})

	if got := RunIDFrom(lookup); got != "mine" {
		t.Fatalf("RunIDFrom() = %q, want mine", got)
	}
}

func TestSanitizingTheNamespace(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"CI-42", "ci_42"},
		{"release/v1.2", "release_v1_2"},
		{"a  b", "a_b"},
		{"__leading", "leading"},
		{"trailing__", "trailing"},
		{"0726xmas", "r0726xmas"},
		{"***", ""},
		{"", ""},
		{"用户", ""},
		{strings.Repeat("v", 60), strings.Repeat("v", maxPrefixLength)},
	}

	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			if got := RunIDFrom(env(map[string]string{EnvRunID: c.raw})); got != c.want {
				t.Errorf("RunIDFrom() with %q = %q, want %q", c.raw, got, c.want)
			}
		})
	}
}

// TestNamesStayValidIdentifiers is the constraint that makes the rule usable:
// a namespaced fixture still has to be a name MaxCompute accepts.
func TestNamesStayValidIdentifiers(t *testing.T) {
	identifier := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	lookup := env(map[string]string{EnvRunID: "run-99-attempt-3-job-name-that-is-quite-long"})

	for _, base := range Bases() {
		got := FixtureFrom(lookup, base)
		if !identifier.MatchString(got) {
			t.Errorf("FixtureFrom(_, %q) = %q, not a valid identifier", base, got)
		}
		if len(got) > 128 {
			t.Errorf("FixtureFrom(_, %q) = %q, %d characters, want at most 128", base, got, len(got))
		}
	}
}

func TestBasesAreUsable(t *testing.T) {
	seen := map[string]bool{}

	for _, base := range Bases() {
		if seen[base] {
			t.Errorf("Bases() lists %q twice", base)
		}
		seen[base] = true
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(base) {
			t.Errorf("Bases() holds %q, which is not an identifier", base)
		}
		if len(base) > 128-maxPrefixLength {
			t.Errorf("Bases() holds %q, too long to namespace", base)
		}
	}
}
