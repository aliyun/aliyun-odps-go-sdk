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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
)

// This is the offline half of "two runs in one project do not collide". A real
// service is not reachable from CI (the OIDC credential step is broken and the
// live opt-in is off), so the suite proves the property on the wire instead: a
// child process boots this package with the live gate turned on against a
// recording endpoint, and what it asks for is read back out of the log.
//
// What that does and does not establish:
//
//   - it shows which names leave the process, for the live setup, the live
//     tests' own objects and the cleanup, with no dependency on a real project;
//   - it does not show that MaxCompute accepts those names or that two real runs
//     are isolated end to end. That reading needs the live switch, which is the
//     repository owner's action.

// createdFixtures are the objects this package's live setup makes in init, the
// ones a second run used to overwrite out from under the first.
var createdFixtures = []string{
	"user",
	"user_temp",
	"has_struct",
	"sale_detail",
	"exist_schema",
	"table1",
	"table2",
}

type wireLog struct {
	mu       sync.Mutex
	requests []string
}

func (w *wireLog) record(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.requests = append(w.requests, line)
}

func (w *wireLog) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.requests...)
}

// startRecordingService answers the few calls a table or schema DDL needs -
// create the instance, report it terminated, report the task successful, accept
// a schema POST - and keeps a line per request so the caller can assert on the
// names that went out.
func startRecordingService(t *testing.T) (*httptest.Server, *wireLog) {
	t.Helper()

	log := &wireLog{}
	taskNames := &sync.Map{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		body := ""
		if r.Body != nil {
			raw := make([]byte, 0, 4096)
			buf := make([]byte, 4096)
			for len(raw) < 64*1024 {
				n, err := r.Body.Read(buf)
				raw = append(raw, buf[:n]...)
				if err != nil {
					break
				}
			}
			body = string(raw)
		}
		log.record(r.Method + " " + r.URL.String() + " body=" + strings.Join(strings.Fields(body), " "))

		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/instances"):
			id := fmt.Sprintf("wire%d", time.Now().UnixNano())
			for _, m := range rangeMatches(body, `<Name>([^<]+)</Name>`) {
				taskNames.Store(id, m)
			}
			w.Header().Set("Location", serverURLFor(r, "/projects/any/instances/"+id))
			w.WriteHeader(http.StatusCreated)
		case r.URL.Query().Has("taskstatus"):
			name, _ := taskNames.Load("unknown")
			_ = name
			_, _ = fmt.Fprintf(w, `<Instance><Tasks><Task Type="SQL"><Name>%s</Name><Status>Success</Status></Task></Tasks></Instance>`,
				firstTaskName(body, r.URL.Path))
		case strings.Contains(r.URL.Path, "/instances/"):
			// The SDK asks for the instance itself before it asks for the task
			// status, so both shapes have to look like an Instance element.
			_, _ = fmt.Fprint(w, `<Instance><Status>Terminated</Status></Instance>`)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `<Result/>`)
		}
	}))
	t.Cleanup(server.Close)
	return server, log
}

// firstTaskName is only needed because the SDK matches a task by name when it
// checks for success; the value echoed here is whatever the create request
// asked for, so the child's DDL completes instead of timing out.
func firstTaskName(body, path string) string {
	for _, m := range rangeMatches(body, `<Name>([^<]+)</Name>`) {
		return m
	}
	return "SQLCreateTableTask"
}

func rangeMatches(s, pattern string) []string {
	re := regexp.MustCompile(pattern)
	out := []string{}
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}

func serverURLFor(r *http.Request, path string) string {
	return "http://" + r.Host + path
}

// runLiveChild boots this test binary with the live gate configured to point at
// a recording endpoint. namespace="" means "no namespace at all", which is how a
// run behaved before names existed.
func runLiveChild(t *testing.T, endpoint, namespace string) ([]byte, error) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary to re-exec: %v", err)
	}

	env := []string{}
	for _, pair := range os.Environ() {
		key, _, _ := strings.Cut(pair, "=")
		// GITHUB_RUN_ID and friends are set on every Actions job. A control run
		// has to mean "no namespace", so the fallback sources go too.
		switch key {
		case "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB", livetest.EnvRunID:
			continue
		}
		env = append(env, pair)
	}
	env = append(env,
		"MAXCOMPUTE_LIVE_TESTS=1",
		"odps_endpoint="+endpoint,
		"ALIBABA_CLOUD_ACCESS_KEY_ID=wire-probe-id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET=wire-probe-key",
		"MAXCOMPUTE_LIVE_KEEP_FIXTURES=0",
	)
	if namespace != "" {
		env = append(env, livetest.EnvRunID+"="+namespace)
	}

	cmd := exec.Command(exe, "-test.run=^ZzzLiveFixtureNamespaceChild$", "-test.timeout=90s")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return out, err
}

// ZzzLiveFixtureNamespaceChild does nothing. Its purpose is to be the one test
// the child selects, so the child's value is entirely in what package start-up
// and teardown put on the wire.
func ZzzLiveFixtureNamespaceChild(t *testing.T) {
	if !liveServiceEnabled() {
		t.Fatal("the child was started without the live gate satisfied")
	}
}

func TestLiveFixtureNamesOnTheWire(t *testing.T) {
	server, log := startRecordingService(t)
	out, err := runLiveChild(t, server.URL, "wirescoped")
	if err != nil {
		t.Fatalf("live child exited with %v:\n%s", err, out)
	}

	lines := log.all()
	if len(lines) == 0 {
		t.Fatal("the recording service saw no request, so this test would pass on nothing")
	}
	for _, line := range lines {
		t.Logf("wire: %s", line)
	}
	tokens := identifiersIn(lines)

	for _, base := range createdFixtures {
		if !tokens[livetest.FixtureFrom(envWith("wirescoped"), base)] {
			t.Errorf("no request carried the namespaced name %s",
				livetest.FixtureFrom(envWith("wirescoped"), base))
		}
		if tokens[base] {
			t.Errorf("a request carried the bare name %q alongside the namespace", base)
		}
	}

	drops := 0
	for _, line := range lines {
		lowered := strings.ToLower(line)
		if strings.Contains(line, "wirescoped_") &&
			(strings.Contains(lowered, "drop table") || strings.Contains(lowered, "delete /projects")) {
			drops++
		}
	}
	if drops == 0 {
		t.Error("cleanup issued no drop, so the namespaced fixtures would leak")
	}
	t.Logf("namespaced run: %d requests, %d drops", len(lines), drops)
}

// TestLegacyFixtureNamesStayUntouchedWhenNoNamespaceIsSet is the compatibility
// control: with nothing to namespace by, the same code path must ask for exactly
// the historical names. It is also the positive control that the counter above
// is not dead.
func TestLegacyFixtureNamesStayUntouchedWhenNoNamespaceIsSet(t *testing.T) {
	server, log := startRecordingService(t)
	out, err := runLiveChild(t, server.URL, "")
	if err != nil {
		t.Fatalf("live child exited with %v:\n%s", err, out)
	}

	lines := log.all()
	for _, line := range lines {
		t.Logf("wire: %s", line)
	}
	tokens := identifiersIn(lines)
	for _, base := range createdFixtures {
		if !tokens[base] {
			t.Errorf("the unscoped run no longer asks for %q; the names are not byte-for-byte what they were", base)
		}
	}
	for _, line := range lines {
		lowered := strings.ToLower(line)
		if strings.Contains(lowered, "drop table") || strings.Contains(lowered, "delete /projects") {
			t.Errorf("the unscoped run must not drop shared fixtures, saw %s", line)
			break
		}
	}
	t.Logf("legacy run: %d requests", len(lines))
}

// TestTwoNamespacedRunsDoNotNameEachOthersObjects is the property the switch
// needs before anyone can open it: two runs against the same project, started at
// the same moment, with nothing but the namespace between them. Each child gets
// its own recording endpoint, so a name from the other namespace in either log
// means the two runs are reaching for the same object.
func TestTwoNamespacedRunsDoNotNameEachOthersObjects(t *testing.T) {
	type run struct {
		namespace string
		log       *wireLog
	}

	type result struct {
		namespace string
		out       []byte
		err       error
	}

	runs := []*run{{namespace: "wirealpha"}, {namespace: "wirebeta"}}
	results := make(chan result, len(runs))
	for _, r := range runs {
		server, log := startRecordingService(t)
		r.log = log
		go func(server *httptest.Server, r *run) {
			out, err := runLiveChild(t, server.URL, r.namespace)
			results <- result{namespace: r.namespace, out: out, err: err}
		}(server, r)
	}

	// Both children have to be done before any log is read: reporting from a
	// goroutine after the test returns is a framework error, not a finding.
	for range runs {
		select {
		case res := <-results:
			if res.err != nil {
				t.Errorf("child %s exited with %v:\n%s", res.namespace, res.err, res.out)
			}
		case <-time.After(180 * time.Second):
			t.Fatal("a live child did not finish")
		}
	}

	for _, r := range runs {
		lines := r.log.all()
		if len(lines) == 0 {
			t.Errorf("child %s recorded nothing", r.namespace)
			continue
		}
		tokens := identifiersIn(lines)
		other := "wirebeta"
		if r.namespace == "wirebeta" {
			other = "wirealpha"
		}
		for _, base := range createdFixtures {
			want := r.namespace + "_" + base
			if !tokens[want] {
				t.Errorf("child %s never named %s", r.namespace, want)
			}
			if tokens[base] {
				t.Errorf("child %s used the bare name %q", r.namespace, base)
			}
			if tokens[other+"_"+base] {
				t.Errorf("child %s reached for %s, which belongs to the other run", r.namespace, other+"_"+base)
			}
		}
		t.Logf("child %s: %d requests, no %s_ name", r.namespace, len(lines), other)
	}
}

func envWith(namespace string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		if key == livetest.EnvRunID {
			return namespace, true
		}
		return "", false
	}
}

// xmlEntities unescapes what the SDK puts inside an XML element: the DDL travels
// as text, so its quotes and angle brackets arrive escaped and have to be put
// back before the line can be read as SQL.
var xmlEntities = strings.NewReplacer(
	"&#39;", "'", "&apos;", "'", "&quot;", `"`, "&lt;", "<", "&gt;", ">", "&amp;", "&",
)

// quotedSQL strips the string literals of a SQL statement: a fixture name and
// the words inside a column comment are not the same kind of occurrence, and
// only the first one can collide with another run. "comment 'name of user'" is
// not a request for the table called user.
var quotedSQL = regexp.MustCompile(`'[^']*'`)

// xmlText keeps the elements of a request that name an object rather than the
// ones that describe it, i.e. drops the query text of a DDL task from the URL
// half of a recorded line.
var tokenSplit = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// identifiersIn returns every bare word that could be an object name in the
// recorded requests, after stripping SQL string literals.
func identifiersIn(lines []string) map[string]bool {
	out := map[string]bool{}
	for _, line := range lines {
		stripped := quotedSQL.ReplaceAllString(xmlEntities.Replace(line), " ")
		for _, tok := range tokenSplit.Split(stripped, -1) {
			if tok != "" {
				out[tok] = true
			}
		}
	}
	return out
}
