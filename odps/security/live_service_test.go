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

package security_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
)

// liveServiceConfigured is this package's gate for cases that need a real
// MaxCompute project. It is not "the environment happens to have credentials":
// see internal/livetest - a live run has to be asked for with
// ODPS_RUN_LIVE_TESTS=1 and the configuration has to be complete (non-empty
// ID and secret, an absolute http(s) endpoint, a non-empty token when one is
// supplied at all). Ordinary offline tests therefore stay offline on a machine
// that has credentials exported for something else.
//
// The cases use it in two shapes:
//
//   - a test calls skipWithoutLiveService(t) and shows up as SKIP;
//   - an example has no *testing.T to skip with, so it returns before touching
//     the service. Its "// Output:" block expects no output, which still matches.
//
// When it does return true, nothing about the case changes - that is what the
// live-service CI job sets the opt-in for.
func liveServiceConfigured() bool {
	return livetest.Ready()
}

func skipWithoutLiveService(t *testing.T) {
	t.Helper()
	if !liveServiceConfigured() {
		t.Skip("not running against a live MaxCompute project: " + livetest.Reason())
	}
}
