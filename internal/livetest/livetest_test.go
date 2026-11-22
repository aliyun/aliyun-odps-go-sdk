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
	"strings"
	"testing"
)

// env makes a lookup out of a map, so no test in this file has to touch the
// real process environment.
func env(values map[string]string) EnvFunc {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

func complete() map[string]string {
	return map[string]string{
		EnvOptIn:                          "1",
		"ALIBABA_CLOUD_ACCESS_KEY_ID":     "id",
		"ALIBABA_CLOUD_ACCESS_KEY_SECRET": "secret",
		"odps_endpoint":                   "http://service.example.com/api",
	}
}

func TestInspectAcceptsACompleteConfiguration(t *testing.T) {
	cfg, problem := Inspect(env(complete()))
	if problem != "" {
		t.Fatalf("complete configuration rejected: %s", problem)
	}
	if cfg.AccessID != "id" || cfg.AccessKey != "secret" {
		t.Errorf("credentials not carried through: %+v", cfg)
	}
	if cfg.STS {
		t.Error("STS selected without a token")
	}
	if cfg.Project != DefaultProject {
		t.Errorf("project = %q, want the default %q", cfg.Project, DefaultProject)
	}
}

func TestInspectRejectsIncompleteConfigurations(t *testing.T) {
	cases := []struct {
		name     string
		drop     []string
		set      map[string]string
		contains string
	}{
		{
			name:     "no opt-in at all",
			drop:     []string{EnvOptIn},
			contains: EnvOptIn,
		},
		{
			name:     "opt-in says something else",
			set:      map[string]string{EnvOptIn: "yes"},
			contains: EnvOptIn,
		},
		{
			name:     "missing access key id",
			drop:     []string{"ALIBABA_CLOUD_ACCESS_KEY_ID"},
			contains: "ALIBABA_CLOUD_ACCESS_KEY_ID",
		},
		{
			name:     "empty access key id",
			set:      map[string]string{"ALIBABA_CLOUD_ACCESS_KEY_ID": ""},
			contains: "ALIBABA_CLOUD_ACCESS_KEY_ID is missing or empty",
		},
		{
			name:     "whitespace access key id",
			set:      map[string]string{"ALIBABA_CLOUD_ACCESS_KEY_ID": "   "},
			contains: "ALIBABA_CLOUD_ACCESS_KEY_ID is missing or empty",
		},
		{
			name:     "missing secret",
			drop:     []string{"ALIBABA_CLOUD_ACCESS_KEY_SECRET"},
			contains: "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
		},
		{
			name:     "empty secret",
			set:      map[string]string{"ALIBABA_CLOUD_ACCESS_KEY_SECRET": ""},
			contains: "ALIBABA_CLOUD_ACCESS_KEY_SECRET is missing or empty",
		},
		{
			name:     "missing endpoint",
			drop:     []string{"odps_endpoint"},
			contains: "odps_endpoint",
		},
		{
			name:     "endpoint without a scheme",
			set:      map[string]string{"odps_endpoint": "service.example.com/api"},
			contains: "absolute http(s) URL",
		},
		{
			name:     "endpoint with a scheme but no host",
			set:      map[string]string{"odps_endpoint": "http://"},
			contains: "absolute http(s) URL",
		},
		{
			name:     "endpoint is not a URL",
			set:      map[string]string{"odps_endpoint": "ht tp://%"},
			contains: "not a valid URL",
		},
		{
			name:     "token present but empty",
			set:      map[string]string{"ALIBABA_CLOUD_SECURITY_TOKEN": ""},
			contains: "ALIBABA_CLOUD_SECURITY_TOKEN is set but empty",
		},
		{
			name:     "project set but empty",
			set:      map[string]string{EnvProject: ""},
			contains: EnvProject + " is set but empty",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			values := complete()
			for _, k := range c.drop {
				delete(values, k)
			}
			for k, v := range c.set {
				values[k] = v
			}
			cfg, problem := Inspect(env(values))
			if problem == "" {
				t.Fatalf("incomplete configuration accepted: %+v", cfg)
			}
			if !strings.Contains(problem, c.contains) {
				t.Errorf("reason %q does not mention %q", problem, c.contains)
			}
		})
	}
}

func TestInspectAcceptsSTSTokenWhenNotEmpty(t *testing.T) {
	values := complete()
	values["ALIBABA_CLOUD_SECURITY_TOKEN"] = "token"
	cfg, problem := Inspect(env(values))
	if problem != "" {
		t.Fatalf("rejected a complete STS configuration: %s", problem)
	}
	if !cfg.STS || cfg.Token != "token" {
		t.Errorf("STS not recorded: %+v", cfg)
	}
}

func TestInspectUsesExplicitProject(t *testing.T) {
	values := complete()
	values[EnvProject] = "my_project"
	cfg, problem := Inspect(env(values))
	if problem != "" {
		t.Fatalf("rejected: %s", problem)
	}
	if cfg.Project != "my_project" {
		t.Errorf("project = %q", cfg.Project)
	}
}

// TestInspectOrderIsIdAndSecretFirst pins the check order the reviewer asked
// about: an environment with only a secret - the shape
// account.AccountFromEnv accepted until #95 fixed it - has to be rejected on
// the ID here too, because this gate also has to catch a missing opt-in and an
// unusable endpoint rather than leaning on the exported function.
func TestInspectRejectsSecretOnlyEnvironment(t *testing.T) {
	values := complete()
	delete(values, "ALIBABA_CLOUD_ACCESS_KEY_ID")
	delete(values, "odps_endpoint")
	cfg, problem := Inspect(env(values))
	if problem == "" {
		t.Fatalf("secret-only environment accepted: %+v", cfg)
	}
	if !strings.Contains(problem, "ALIBABA_CLOUD_ACCESS_KEY_ID") {
		t.Errorf("reason = %q, want the missing ID to be the reported problem", problem)
	}
}

func TestProjectExplicitPrecedenceMatrix(t *testing.T) {
	cases := []struct {
		name  string
		set   map[string]string
		unset []string
		want  string
	}{
		{"new variable only", map[string]string{EnvProject: "new_proj"}, nil, "new_proj"},
		{"legacy variable only", map[string]string{LegacyProjectEnv: "legacy_proj"}, []string{EnvProject}, "legacy_proj"},
		{"both set, new wins", map[string]string{EnvProject: "new_proj", LegacyProjectEnv: "legacy_proj"}, nil, "new_proj"},
		{"neither set", nil, []string{EnvProject, LegacyProjectEnv}, ""},
		{"new exported but blank, legacy usable", map[string]string{EnvProject: "   ", LegacyProjectEnv: "legacy_proj"}, nil, "legacy_proj"},
		{"both exported but blank", map[string]string{EnvProject: "", LegacyProjectEnv: "\t "}, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			values := map[string]string{}
			for k, v := range c.set {
				values[k] = v
			}
			for _, k := range c.unset {
				delete(values, k)
			}
			if got := ProjectExplicitFrom(env(values)); got != c.want {
				t.Errorf("ProjectExplicit = %q, want %q (env %v)", got, c.want, values)
			}
		})
	}
}
