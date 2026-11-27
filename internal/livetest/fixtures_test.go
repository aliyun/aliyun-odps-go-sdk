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
	"fmt"
	"regexp"
	"strings"
	"testing"
)

var validPrefix = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

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
	lookup := env(map[string]string{EnvRunID: "nightly7"})

	if got := RunIDFrom(lookup); got != "nightly7" {
		t.Fatalf("RunIDFrom() = %q, want nightly7", got)
	}
	for _, base := range Bases() {
		want := "nightly7_" + base
		if got := FixtureFrom(lookup, base); got != want {
			t.Errorf("FixtureFrom(_, %q) = %q, want %q", base, got, want)
		}
	}
}

// TestLossyIdentityKeepsItsOwnDigest covers the alias problem: two spellings of
// one readable prefix must not become one namespace.
func TestLossyIdentityKeepsItsOwnDigest(t *testing.T) {
	pairs := [][2]string{
		{"ci-a", "ci_a"},
		{"CI-42", "ci-42"},
		{"run.7", "run-7"},
	}
	for _, pair := range pairs {
		first, second := Sanitize(pair[0]), Sanitize(pair[1])
		if first == "" || second == "" {
			t.Fatalf("Sanitize(%q)=%q, Sanitize(%q)=%q, both must be usable", pair[0], first, pair[1], second)
		}
		if first == second {
			t.Errorf("%q and %q collapse to the namespace %q", pair[0], pair[1], first)
		}
		if strings.Split(first, "_")[0] != strings.Split(second, "_")[0] {
			t.Errorf("%q and %q should share a readable prefix, got %q and %q", pair[0], pair[1], first, second)
		}
	}

	// Truncation is the other way a readable prefix loses information.
	long := strings.Repeat("identity", 12)
	if Sanitize(long) == Sanitize(long[:len(long)-20]) {
		t.Error("a truncated identity must not reuse the shorter identity's namespace")
	}
	if Sanitize(long) != Sanitize(long) {
		t.Error("Sanitize must be deterministic, or a run cannot clean up its own fixtures")
	}
}

// TestGitHubIdentitySeparatesRunsAttemptsAndJobs pins what the Actions fallback
// really separates: a different run, a different attempt, or a different job id.
// It does NOT separate matrix legs - GITHUB_JOB is the job_id and every leg of
// that job reports the same one - which is what the leg cases below document.
func TestGitHubIdentitySeparatesRunsAttemptsAndJobs(t *testing.T) {
	identities := []map[string]string{
		{"GITHUB_RUN_ID": "1234", "GITHUB_JOB": "live"},
		{"GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_JOB": "live"},
		{"GITHUB_RUN_ID": "1235", "GITHUB_JOB": "live"},
		{"GITHUB_RUN_ID": "1234", "GITHUB_JOB": "live-tunnel"},
		{"GITHUB_RUN_ID": "1234"},
	}

	seen := map[string]string{}
	for _, values := range identities {
		got := RunIDFrom(env(values))
		if got == "" {
			t.Errorf("RunIDFrom(%v) = empty, a hosted run must be namespaced", values)
			continue
		}
		if !validPrefix.MatchString(got) || len(got) > maxPrefixLength {
			t.Errorf("RunIDFrom(%v) = %q, not a usable prefix", values, got)
		}
		key := fmt.Sprint(values)
		if before, dup := seen[got]; dup {
			t.Errorf("identities %s and %s share the namespace %q", before, key, got)
		}
		seen[got] = key
	}
}

// TestMatrixLegsOfOneJobNeedTheIndex covers the gap that the fallback cannot
// close on its own: two legs of one job have the same run id, attempt and job
// id, so without a leg discriminator they land in one namespace and their
// setup and cleanup target the same names. Passing strategy.job-index through
// MAXCOMPUTE_LIVE_RUN_INDEX separates them, and an explicit run id wins.
func TestMatrixLegsOfOneJobNeedTheIndex(t *testing.T) {
	leg := func(index string) map[string]string {
		values := map[string]string{"GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_JOB": "live"}
		if index != "" {
			values[EnvRunIndex] = index
		}
		return values
	}

	if a, b := RunIDFrom(env(leg(""))), RunIDFrom(env(leg(""))); a != b {
		t.Errorf("two legs without a discriminator should share a namespace, got %q and %q", a, b)
	}

	namespaces := map[string]bool{}
	for _, index := range []string{"0", "1", "2"} {
		got := RunIDFrom(env(leg(index)))
		if namespaces[got] {
			t.Errorf("leg index %s reused namespace %q", index, got)
		}
		namespaces[got] = true
	}

	override := env(map[string]string{EnvRunID: "leg2", EnvRunIndex: "9", "GITHUB_RUN_ID": "1234", "GITHUB_JOB": "live"})
	if got := RunIDFrom(override); got != "leg2" {
		t.Errorf("RunIDFrom() = %q, want the explicit run id to win over the fallback", got)
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

// TestSanitizingTheNamespace pins the character-set rules of the readable part.
// Identities that survive normalization unchanged keep their shape; the rest get
// a digest, which TestLossyIdentityKeepsItsOwnDigest covers.
func TestReadablePrefixRules(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"nightly7", "nightly7"},
		{"a_b", "a_b"},
		{"CI-42", "ci_42"},
		{"release/v1.2", "release_v1_2"},
		{"a  b", "a_b"},
		{"__leading", "leading"},
		{"trailing__", "trailing"},
		{"0726xmas", "r0726xmas"},
		{"***", ""},
		{"", ""},
		{"\u7528\u6237", ""},
	}

	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			if got := readablePrefix(c.raw); got != c.want {
				t.Errorf("readablePrefix(%q) = %q, want %q", c.raw, got, c.want)
			}
		})
	}

	for _, raw := range []string{"CI-42", "release/v1.2", "0726xmas"} {
		got := Sanitize(raw)
		if !strings.HasPrefix(got, readablePrefix(raw)) || got == readablePrefix(raw) {
			t.Errorf("Sanitize(%q) = %q, want the readable prefix %q plus a digest", raw, got, readablePrefix(raw))
		}
	}
	if got := Sanitize("nightly7"); got != "nightly7" {
		t.Errorf("Sanitize of an already valid prefix = %q, want it left alone", got)
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
