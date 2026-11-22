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

// Package livetest decides whether a test may talk to a real MaxCompute
// project. It exists because "the environment happens to have credentials" is
// not a request to use them: a developer machine, an IDE or any CI job can
// export ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET and
// odps_endpoint for entirely unrelated reasons, and a test that then creates
// tables in a live project has done something nobody asked for.
//
// So live cases need two things, and both are checked here:
//
//  1. an explicit opt-in (ODPS_RUN_LIVE_TESTS=1), set only by the job that is
//     meant to run against a service;
//  2. a complete configuration - non-empty AccessKey ID and secret, a usable
//     endpoint, and a non-empty token when a token is supplied at all.
//
// Anything else reports false with a reason, and the caller skips. Disabled or
// incomplete configurations must make zero service requests, which is why no
// setup work belongs in a package init(): see Ready.
package livetest

import (
	"net/url"
	"os"
	"strings"
	"sync"
)

// Environment variables read by this package.
const (
	// EnvOptIn must equal "1" before any test talks to a live service.
	EnvOptIn = "ODPS_RUN_LIVE_TESTS"
	// EnvProject names the project live cases run against.
	EnvProject = "ODPS_LIVE_TEST_PROJECT"
	// LegacyProjectEnv is the project variable the sqldriver real-service helpers
	// have always required. It is kept working on purpose rather than renamed out
	// from under them - see ProjectExplicit for which one wins.
	LegacyProjectEnv = "MAXCOMPUTE_PROJECT"
	// DefaultProject is the regression project the live cases have always used.
	// It stays the default so that turning this gate on changes nothing for an
	// existing live run; the live job is expected to set EnvProject explicitly.
	DefaultProject = "go_sdk_regression_testing"
)

// EnvFunc reads one environment variable. os.LookupEnv satisfies it; tests
// pass their own map so the rules can be exercised without mutating the
// process environment.
type EnvFunc func(key string) (string, bool)

// Config is a validated live-service configuration, built from the environment
// and not from account.AccountFromEnv. Two separate reasons, kept separate on
// purpose: AccountFromEnv is public API and answers "did the caller configure
// credentials" (since #95 it returns nil unless ID and secret are both usable),
// while this package additionally answers "was a live run asked for, and is the
// endpoint usable" - opt-in, endpoint shape and a non-empty token when one is
// supplied. Feeding the live gate off the exported function would tie test
// isolation to a public behaviour that exists for a different question.
type Config struct {
	AccessID  string
	AccessKey string
	Token     string
	STS       bool
	Endpoint  string
	Project   string
}

// Inspect validates env and returns the first reason it is unusable ("" when
// it is usable). It reads nothing but env, so it is safe to call from tests
// with a fake lookup.
func Inspect(env EnvFunc) (Config, string) {
	cfg := Config{Project: DefaultProject}

	lookup := func(key string) (string, bool) {
		v, ok := env(key)
		return strings.TrimSpace(v), ok
	}

	if optIn, _ := lookup(EnvOptIn); optIn != "1" {
		return cfg, EnvOptIn + " is not set to 1 (a live run has to be asked for)"
	}

	id, idPresent := lookup("ALIBABA_CLOUD_ACCESS_KEY_ID")
	if !idPresent || id == "" {
		return cfg, "ALIBABA_CLOUD_ACCESS_KEY_ID is missing or empty"
	}
	secret, secretPresent := lookup("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	if !secretPresent || secret == "" {
		return cfg, "ALIBABA_CLOUD_ACCESS_KEY_SECRET is missing or empty"
	}
	cfg.AccessID, cfg.AccessKey = id, secret

	// A token that is present but empty used to select the STS account with an
	// empty SecurityToken; here it makes the configuration invalid instead.
	if token, present := lookup("ALIBABA_CLOUD_SECURITY_TOKEN"); present {
		if token == "" {
			return cfg, "ALIBABA_CLOUD_SECURITY_TOKEN is set but empty"
		}
		cfg.Token, cfg.STS = token, true
	}

	endpoint, present := lookup("odps_endpoint")
	if !present || endpoint == "" {
		return cfg, "odps_endpoint is missing or empty"
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return cfg, "odps_endpoint is not a valid URL: " + err.Error()
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return cfg, "odps_endpoint must be an absolute http(s) URL with a host, got " + endpoint
	}
	cfg.Endpoint = endpoint

	if project, present := lookup(EnvProject); present {
		if project == "" {
			return cfg, EnvProject + " is set but empty"
		}
		cfg.Project = project
	}

	return cfg, ""
}

var (
	stateOnce sync.Once
	current   Config
	problem   string
)

func state() (Config, string) {
	stateOnce.Do(func() { current, problem = Inspect(os.LookupEnv) })
	return current, problem
}

var setupOnce sync.Once

// Ready reports whether live cases may run in this process. When the
// configuration is complete it runs setup once - the shared fixtures live
// cases depend on. This is where that work belongs; a package init() would do
// it before any test is even selected, so `go test -run '^$'` would still
// touch the service.
func Ready(setup ...func()) bool {
	cfg, problem := state()
	if problem != "" {
		return false
	}
	if len(setup) > 0 && setup[0] != nil {
		setupOnce.Do(func() {
			_ = cfg
			setup[0]()
		})
	}
	return true
}

// Enabled reports whether the live configuration is complete, without running
// any setup.
func Enabled() bool {
	_, problem := state()
	return problem == ""
}

// Reason explains why Enabled is false, for a skip message. It is empty when
// the configuration is complete.
func Reason() string {
	_, problem := state()
	return problem
}

// Project returns the project live cases run against; empty when not
// configured.
func Project() string {
	cfg, problem := state()
	if problem != "" {
		return ""
	}
	return cfg.Project
}

// ProjectExplicit returns the project an individual live helper should use,
// honouring ODPS_LIVE_TEST_PROJECT first and falling back to the older
// MAXCOMPUTE_PROJECT, with no implicit default: helpers that have always
// required an explicit project keep skipping when neither is set.
//
// Two variables is not elegant and this is the honest reason it stays that way
// for now: ODPS_LIVE_TEST_PROJECT controls every case that goes through this
// package, while the two sqldriver real-service helpers predate it and name a
// project of their own choosing. Collapsing them into one knob would silently
// repoint those helpers at DefaultProject, so the reconciliation is "prefer the
// new name, keep the old one working" here plus a line in the PR, not a claim
// that one variable rules every live case.
func ProjectExplicit() string { return ProjectExplicitFrom(os.LookupEnv) }

// ProjectExplicitFrom is the same rule with the environment injected, so the
// precedence table can be exercised without touching the process environment.
// The exported helper is what the live cases actually call; the matrix below is
// paired with a subprocess cell that proves the sqldriver helpers really go
// through it, because a pure-function test on its own does not prove that.
func ProjectExplicitFrom(env EnvFunc) string {
	if v, ok := env(EnvProject); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := env(LegacyProjectEnv); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

// ProjectOrDefault returns the validated project when a live run is
// configured, and otherwise the historical default. Test packages build their
// project name with this so that opting in and setting ODPS_LIVE_TEST_PROJECT
// moves every reader and creator together: a prefix or a project that only the
// creator honours would fail silently, so there is deliberately no such knob
// until all fixture names go through one place.
func ProjectOrDefault() string {
	cfg, problem := state()
	if problem != "" {
		return DefaultProject
	}
	return cfg.Project
}
