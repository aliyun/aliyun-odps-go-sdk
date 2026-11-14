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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// These tests need no cloud credentials: they point the SDK at a local
// counting server and check what a *separate* test process does with it.
//
// The counting has to happen in a subprocess because the thing under test is
// process startup - the fixtures used to be created from a package init(), so
// running them inside this process would either skip the code path or pollute
// the parent's environment. `go test -run '^$'` is the sharpest case: no test
// body runs at all, so any request means setup that nobody asked for.

// countingServer answers every request with 404 and a body the SDK treats as
// an error, so an unwanted live call fails fast instead of hanging, and counts
// how many it saw.
type countingServer struct {
	srv      *httptest.Server
	requests int64
	mu       sync.Mutex
	seen     []string
}

// requested returns a copy of the request lines seen so far; the tests assert on
// the project name inside them, so the slice needs to be read under a lock.
func (c *countingServer) requested() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.seen))
	copy(out, c.seen)
	return out
}

func newCountingServer(t *testing.T) *countingServer {
	c := &countingServer{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&c.requests, 1)
		c.mu.Lock()
		c.seen = append(c.seen, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		c.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("<Error><Code>NoSuchObject</Code><Message>counting server</Message></Error>"))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *countingServer) count() int64 { return atomic.LoadInt64(&c.requests) }

// goTest runs `go test` for this package in a child process whose environment
// is exactly env. It returns the child's exit code and combined output.
func goTest(t *testing.T, pkg, runFlag string, env []string) (int, string) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("no go binary on PATH, cannot exercise process startup in a child process: %v", err)
	}
	root := filepath.Join("..") // this test lives in odps/, the module root is above it
	cmd := exec.Command(goBin, "test", "-count=1", "-timeout", "180s", "-v", "-run", runFlag, pkg)
	cmd.Dir = root
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	}
	if err != nil {
		return -1, err.Error() + "\n" + string(out)
	}
	return 0, string(out)
}

// childEnv builds a child environment: this process's environment, plus set,
// minus every name in unset. Setting a value to "" is deliberately different
// from unsetting it - "exported but empty" is one of the shapes
// account.AccountFromEnv used to accept, so the tests need to produce it.
// unset wins over set, and it also removes names the parent does not have (a
// child must not inherit a value through the set map by accident).
func childEnv(set map[string]string, unset []string) []string {
	merged := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.Index(kv, "="); i > 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	for k, v := range set {
		merged[k] = v
	}
	for _, k := range unset {
		delete(merged, k)
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

func TestLiveSetupStaysAsleepWithoutOptIn(t *testing.T) {
	srv := newCountingServer(t)

	// Credentials and an endpoint that are complete except for the opt-in: this
	// is exactly the developer machine the reviewer described.
	env := childEnv(map[string]string{
		"ALIBABA_CLOUD_ACCESS_KEY_ID":     "fake-id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "fake-secret",
		"odps_endpoint":                   srv.srv.URL,
	}, []string{"ODPS_RUN_LIVE_TESTS", "ALIBABA_CLOUD_SECURITY_TOKEN", "ODPS_LIVE_TEST_PROJECT"})

	t.Run("selecting no test still creates nothing", func(t *testing.T) {
		code, out := goTest(t, "./odps", "^$", env)
		if code != 0 {
			t.Fatalf("child exited %d with no live case selected:\n%s", code, out)
		}
		if got := srv.count(); got != 0 {
			t.Fatalf("%d requests reached the service although nothing was opted in (paths %v)", got, srv.requested())
		}
	})

	t.Run("a live case is skipped, not run", func(t *testing.T) {
		code, out := goTest(t, "./odps", "^TestSchemas_List$", env)
		if code != 0 {
			t.Fatalf("child exited %d; a guarded live case must skip, not fail:\n%s", code, out)
		}
		if !strings.Contains(out, "--- SKIP: TestSchemas_List") {
			t.Errorf("child output does not show the live case skipping:\n%s", out)
		}
		if got := srv.count(); got != 0 {
			t.Fatalf("%d requests reached the service without opt-in (paths %v)", got, srv.requested())
		}
	})
}

func TestLiveSetupRejectsIncompleteConfigurationEvenWithOptIn(t *testing.T) {
	srv := newCountingServer(t)

	cases := []struct {
		name  string
		set   map[string]string
		unset []string
	}{
		{"endpoint unset", map[string]string{}, []string{"odps_endpoint"}},
		{"endpoint empty", map[string]string{"odps_endpoint": ""}, nil},
		{"endpoint has no scheme", map[string]string{"odps_endpoint": "service.cn-shanghai.maxcompute.aliyun.com/api"}, nil},
		{"secret unset", map[string]string{}, []string{"ALIBABA_CLOUD_ACCESS_KEY_SECRET"}},
		{"secret exported but empty", map[string]string{"ALIBABA_CLOUD_ACCESS_KEY_SECRET": ""}, nil},
		{"only a secret exported", map[string]string{}, []string{"ALIBABA_CLOUD_ACCESS_KEY_ID"}},
		{"id exported but empty", map[string]string{"ALIBABA_CLOUD_ACCESS_KEY_ID": ""}, nil},
		{"token exported but empty", map[string]string{"ALIBABA_CLOUD_SECURITY_TOKEN": ""}, nil},
		{"project exported but empty", map[string]string{"ODPS_LIVE_TEST_PROJECT": ""}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			set := map[string]string{
				"ODPS_RUN_LIVE_TESTS":             "1",
				"ALIBABA_CLOUD_ACCESS_KEY_ID":     "fake-id",
				"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "fake-secret",
				"odps_endpoint":                   srv.srv.URL,
			}
			unset := []string{"ALIBABA_CLOUD_SECURITY_TOKEN", "ODPS_LIVE_TEST_PROJECT"}
			for k, v := range c.set {
				set[k] = v
				// A case that sets a name wins over the base list that clears
				// it - "exported but empty" is the whole point of those rows.
				for i := 0; i < len(unset); i++ {
					if unset[i] == k {
						unset = append(unset[:i], unset[i+1:]...)
						i--
					}
				}
			}
			env := childEnv(set, append(unset, c.unset...))
			code, out := goTest(t, "./odps", "^TestSchemas_List$", env)
			if code != 0 {
				t.Fatalf("child exited %d on an incomplete configuration; it must skip:\n%s", code, out)
			}
			if got := srv.count(); got != 0 {
				t.Fatalf("%d requests reached the service with an incomplete configuration (paths %v)", got, srv.requested())
			}
		})
	}
}

// TestOptInWithCompleteConfigurationActuallyCalls is the control that keeps the
// two tests above from passing because of a broken counting server or a child
// that never started: with the opt-in set and a complete configuration, the
// live case must reach the service. It reaches the local server, not a cloud
// project, and the fixture creation then fails - loudly, which is the point.
func TestOptInWithCompleteConfigurationActuallyCalls(t *testing.T) {
	srv := newCountingServer(t)
	env := childEnv(map[string]string{
		"ODPS_RUN_LIVE_TESTS":             "1",
		"ODPS_LIVE_TEST_PROJECT":          "livetest_gate_project",
		"ALIBABA_CLOUD_ACCESS_KEY_ID":     "fake-id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "fake-secret",
		"odps_endpoint":                   srv.srv.URL,
	}, []string{"ALIBABA_CLOUD_SECURITY_TOKEN"})

	// Even opted in, selecting no test must not create anything: setup is tied
	// to the guard, not to process startup.
	if code, out := goTest(t, "./odps", "^$", env); code != 0 {
		t.Fatalf("opted-in child exited %d with no case selected:\n%s", code, out)
	}
	if got := srv.count(); got != 0 {
		t.Fatalf("%d requests with no live case selected (paths %v)", got, srv.requested())
	}

	code, out := goTest(t, "./odps", "^TestSchemas_List$", env)
	if srv.count() == 0 {
		t.Fatalf("no request reached the server although the live case was opted in and configured; child exit %d:\n%s", code, out)
	}
	if code == 0 {
		t.Errorf("the opted-in live case passed against a server that answers 404 - the fixture setup is not being attempted (output below)\n%s", out)
	}
}

// TestLiveProjectIsNotHardcodedAnywhere keeps creators and readers on one
// source: the project comes from internal/livetest, so setting
// ODPS_LIVE_TEST_PROJECT for the live job moves every case together. A literal
// left in a test file would silently keep pointing at the old project.
func TestLiveProjectIsNotHardcodedAnywhere(t *testing.T) {
	// The first version read only this package's directory and skipped
	// subdirectories, which is exactly how a hard-coded project survived in
	// odps/tunnel. Walk the whole module instead: the point of this check is that
	// the project name has one source, and that claim is only true across every
	// package that talks to a live project.
	const literal = "go_sdk_regression_testing"
	var files []string
	err := filepath.Walk("..", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cannot walk the module: %v", err)
	}
	if len(files) < 20 {
		t.Fatalf("the walk found only %d test files; the scan is not covering the module", len(files))
	}
	var hits []string
	for _, path := range files {
		if filepath.Base(path) == "live_setup_guard_test.go" {
			continue // this file names the literal in order to look for it
		}
		if filepath.Base(path) == "livetest.go" {
			continue // DefaultProject, the one place the value is allowed to sit
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			t.Fatalf("cannot read %s: %v", path, err)
		}
		if strings.Contains(string(data), literal) {
			hits = append(hits, path)
		}
	}
	if len(hits) > 0 {
		t.Errorf("%d test file(s) still hardcode the live project %q: %v", len(hits), literal, hits)
	}
	t.Logf("scanned %d *_test.go files across the module, %d hard-coded project literals", len(files), len(hits))
}

// TestTunnelPackageStaysAsleepWithoutOptIn is the cell that was missing when the
// reviewer found that TestEndToEndComplexTypeWithNulls - which arrives from
// master, not from this branch - guarded itself with "is a secret exported" and
// therefore created a table on any machine holding credentials. Every live case in
// every package has to go through the shared gate, and a head-only audit of the
// branch cannot prove that, so the check runs the integrated tree's tunnel package.
func TestTunnelPackageStaysAsleepWithoutOptIn(t *testing.T) {
	srv := newCountingServer(t)
	env := childEnv(map[string]string{
		"ALIBABA_CLOUD_ACCESS_KEY_ID":     "fake-id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "fake-secret",
		"odps_endpoint":                   srv.srv.URL,
		"tunnel_endpoint":                 srv.srv.URL,
	}, []string{"ODPS_RUN_LIVE_TESTS", "ALIBABA_CLOUD_SECURITY_TOKEN", "ODPS_LIVE_TEST_PROJECT"})

	code, out := goTest(t, "./odps/tunnel", "^TestEndToEndComplexTypeWithNulls$", env)
	if code != 0 {
		t.Fatalf("child exited %d; a live case must skip when nothing was opted in:\n%s", code, out)
	}
	if !strings.Contains(out, "--- SKIP: TestEndToEndComplexTypeWithNulls") {
		t.Errorf("the tunnel live case did not report SKIP:\n%s", out)
	}
	if got := srv.count(); got != 0 {
		t.Fatalf("%d requests reached the service from the tunnel package without opt-in (paths %v)", got, srv.requested())
	}
}

// TestProjectOverrideReachesTheWire is the "assert the requested project at a
// fake endpoint" half of the review: setting ODPS_LIVE_TEST_PROJECT has to show up
// in what the SDK actually asks for, otherwise "the project is explicit" is only a
// statement about a variable.
func TestProjectOverrideReachesTheWire(t *testing.T) {
	srv := newCountingServer(t)
	env := childEnv(map[string]string{
		"ODPS_RUN_LIVE_TESTS":             "1",
		"ODPS_LIVE_TEST_PROJECT":          "gate_probe_project",
		"ALIBABA_CLOUD_ACCESS_KEY_ID":     "fake-id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "fake-secret",
		"odps_endpoint":                   srv.srv.URL,
	}, []string{"ALIBABA_CLOUD_SECURITY_TOKEN"})

	code, out := goTest(t, "./odps", "^TestSchemas_List$", env)
	if srv.count() == 0 {
		t.Fatalf("opted-in live case made no request at all, so the project cannot be checked; child exit %d:\n%s", code, out)
	}
	found := false
	for _, path := range srv.requested() {
		if strings.Contains(path, "gate_probe_project") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no request carried the configured project; paths seen: %v", srv.requested())
	}
	if code == 0 {
		t.Errorf("the opted-in case passed against a server that answers 404 - the live path is not really being exercised:\n%s", out)
	}
}
