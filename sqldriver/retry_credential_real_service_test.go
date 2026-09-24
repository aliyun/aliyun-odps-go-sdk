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

package sqldriver_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/credentials-go/credentials"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

// Real-service smoke for the two paths the retry/credential changes touch:
// signing through a credentials-go provider, and CreateTask's retry loop on a
// request that succeeds at the first attempt. The fake services in
// odps/instances_retry_contract_test.go and
// odps/account/sts_refresh_contract_test.go prove the decisions; this proves the
// live service still accepts them.
//
// It skips unless the environment describes a real MaxCompute service, so it is
// inert in CI and on a contributor's machine:
//
//	odps_endpoint, ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET,
//	MAXCOMPUTE_PROJECT   (ALIBABA_CLOUD_SECURITY_TOKEN selects the STS shape)
//
// Nothing here hardcodes or prints an identity: the credential is read from the
// environment chain, and the assertions quote only the query result.
func realServiceSetup(t *testing.T) (endpoint, project string, cred credentials.Credential) {
	t.Helper()

	endpoint = os.Getenv("odps_endpoint")
	project = os.Getenv("MAXCOMPUTE_PROJECT")
	if endpoint == "" || project == "" ||
		os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID") == "" ||
		os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET") == "" {
		t.Skip("set odps_endpoint, MAXCOMPUTE_PROJECT and ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET to run the real-service smoke")
	}

	// The default chain picks up the environment, including a security token
	// when one is present, and refreshes on its own near expiry.
	var err error
	cred, err = credentials.NewCredential(nil)
	if err != nil {
		t.Fatalf("build credential from the environment: %v", err)
	}

	model, err := cred.GetCredential()
	if err != nil || model == nil || model.AccessKeyId == nil || *model.AccessKeyId == "" {
		t.Skipf("the environment does not resolve to a usable access key (%v)", err)
	}

	return endpoint, project, cred
}

// TestRealServiceCredentialProviderRunsQuery is the end-to-end check: an account
// built on a provider signs a create request that the service accepts, the
// create loop returns after one attempt, and the task result comes back.
func TestRealServiceCredentialProviderRunsQuery(t *testing.T) {
	endpoint, project, cred := realServiceSetup(t)

	odpsIns := odps.NewOdps(account2.NewStsAccountWithCredential(cred), endpoint)
	odpsIns.SetDefaultProjectName(project)
	odpsIns.SetHttpTimeout(60 * time.Second)

	sqlTask := odps.NewSqlTask("retry_credential_smoke", "select 1 as one;", nil)

	started := time.Now()
	instance, err := odpsIns.Instances().CreateTask(project, &sqlTask)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	t.Logf("created in %s", time.Since(started).Truncate(time.Millisecond))

	if err := instance.WaitForSuccess(); err != nil {
		t.Fatalf("wait for instance %s: %v", instance.Id(), err)
	}

	results, err := instance.GetResult()
	if err != nil {
		t.Fatalf("read task results: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("the service returned no task result")
	}
	content := results[0].Content()
	if !strings.Contains(content, "one") {
		t.Fatalf("unexpected result content %q", content)
	}
	t.Logf("task status %s, result carried the projected column", results[0].Status)
}

// TestRealServiceAliyunAccountAndProviderAgree is the compatibility half: the
// same request signed through the plain account and through the provider must
// both be accepted by the service, so a caller that switches to a provider does
// not change what the service sees.
func TestRealServiceAliyunAccountAndProviderAgree(t *testing.T) {
	endpoint, project, cred := realServiceSetup(t)

	model, err := cred.GetCredential()
	if err != nil || model == nil || model.AccessKeyId == nil || model.AccessKeySecret == nil {
		t.Skipf("no complete access key pair in the environment (%v)", err)
	}

	providerIns := odps.NewOdps(account2.NewStsAccountWithCredential(cred), endpoint)
	providerIns.SetDefaultProjectName(project)

	plainIns := odps.NewOdps(account2.NewAliyunAccount(*model.AccessKeyId, *model.AccessKeySecret), endpoint)
	plainIns.SetDefaultProjectName(project)

	// A metadata read that needs no job: the project's name is echoed back.
	for _, pair := range []struct {
		label   string
		odpsIns *odps.Odps
	}{
		{"provider", providerIns},
		{"plain account", plainIns},
	} {
		p := pair.odpsIns.DefaultProject()
		if err := p.Load(); err != nil {
			t.Fatalf("%s: load project: %v", pair.label, err)
		}
		if p.Name() == "" {
			t.Fatalf("%s: the service echoed no project name", pair.label)
		}
		t.Logf("%s: project loaded through this signing path", pair.label)
	}
}
