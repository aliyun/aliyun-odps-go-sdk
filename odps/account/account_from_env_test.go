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

package account

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Credential environment variables read by AccountFromEnv. They are spelled out
// again here on purpose: a test that shares literals with the code under test
// cannot catch the code reading the wrong variable.
const (
	testEnvAccessId  = "ALIBABA_CLOUD_ACCESS_KEY_ID"
	testEnvAccessKey = "ALIBABA_CLOUD_ACCESS_KEY_SECRET"
	testEnvStsToken  = "ALIBABA_CLOUD_SECURITY_TOKEN"
)

// strPtr helps the case tables: a *string separates "exported with this value"
// from "not exported at all", which is exactly what these tests are about, so the
// empty string cannot stand for both.
func strPtr(s string) *string {
	return &s
}

// exportCredentialEnv exports exactly the given variables for the duration of the
// test and restores the previous state on cleanup. A nil *string means "not
// exported".
//
// This is written against os.LookupEnv/os.Setenv/os.Unsetenv rather than
// testing.T.Setenv because go.mod still declares go1.15 (and ci-go1.15.yml still
// runs it), where t.Setenv does not exist.
func exportCredentialEnv(t *testing.T, accessId, accessKey, stsToken *string) {
	t.Helper()

	variables := []struct {
		name  string
		value *string
	}{
		{name: testEnvAccessId, value: accessId},
		{name: testEnvAccessKey, value: accessKey},
		{name: testEnvStsToken, value: stsToken},
	}

	for _, v := range variables {
		name := v.name
		value := v.value

		prev, existed := os.LookupEnv(name)
		t.Cleanup(func() {
			if existed {
				assert.NoError(t, os.Setenv(name, prev))
			} else {
				assert.NoError(t, os.Unsetenv(name))
			}
		})

		if value == nil {
			assert.NoError(t, os.Unsetenv(name))
		} else {
			assert.NoError(t, os.Setenv(name, *value))
		}
	}
}

// TestAccountFromEnvIsConfiguredOnlyWithAUsableKeyPair pins the contract that
// `AccountFromEnv() != nil` means "there is a key pair that can be signed with".
//
// Before that contract, the found flag of each os.LookupEnv call was overwritten
// by the next one, so exporting only ALIBABA_CLOUD_ACCESS_KEY_SECRET yielded an
// account with an empty AccessId: the caller read it as "configured", and the
// empty access key surfaced later, as a service-side 4xx.
func TestAccountFromEnvIsConfiguredOnlyWithAUsableKeyPair(t *testing.T) {
	cases := []struct {
		name      string
		accessId  *string
		accessKey *string
	}{
		{"secret exported without id", nil, strPtr("test-secret")},
		{"id exported empty", strPtr(""), strPtr("test-secret")},
		{"id exported blank", strPtr("   "), strPtr("test-secret")},
		{"secret not exported", strPtr("test-id"), nil},
		{"secret exported empty", strPtr("test-id"), strPtr("")},
		{"secret exported blank", strPtr("test-id"), strPtr("\t ")},
		{"both exported blank", strPtr(" "), strPtr("   ")},
		{"nothing exported", nil, nil},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			exportCredentialEnv(t, tc.accessId, tc.accessKey, nil)

			assert.Nil(t, AccountFromEnv(),
				"AccountFromEnv must report \"not configured\" unless both the access key id and the secret are exported and non-blank")
		})
	}
}

// TestAccountFromEnvWithACompleteKeyPair checks the values actually handed to the
// account, in both the permanent-key and the temporary-credential shape.
func TestAccountFromEnvWithACompleteKeyPair(t *testing.T) {
	t.Run("no token exported gives an AliyunAccount", func(t *testing.T) {
		exportCredentialEnv(t, strPtr("test-id"), strPtr("test-secret"), nil)

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		assert.Equal(t, Aliyun, acct.GetType())

		ak, ok := acct.(*AliyunAccount)
		if !assert.True(t, ok, "unexpected concrete account type: %T", acct) {
			return
		}
		assert.Equal(t, "test-id", ak.AccessId())
		assert.Equal(t, "test-secret", ak.AccessKey())
	})

	t.Run("token exported empty stays a permanent key", func(t *testing.T) {
		exportCredentialEnv(t, strPtr("test-id"), strPtr("test-secret"), strPtr(""))

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		// The other half of the overwritten found flag: an exported-but-empty
		// ALIBABA_CLOUD_SECURITY_TOKEN selected the STS branch, producing an
		// account that sends an empty sts-token header.
		assert.Equal(t, Aliyun, acct.GetType())
		if _, isSts := acct.(*StsAccount); isSts {
			t.Fatalf("an empty security token must not select the STS account")
		}
	})

	t.Run("token exported blank stays a permanent key", func(t *testing.T) {
		exportCredentialEnv(t, strPtr("test-id"), strPtr("test-secret"), strPtr("  "))

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		assert.Equal(t, Aliyun, acct.GetType())
	})

	t.Run("token exported non-blank gives an StsAccount with that token", func(t *testing.T) {
		exportCredentialEnv(t, strPtr("test-id"), strPtr("test-secret"), strPtr("test-token"))

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		assert.Equal(t, STS, acct.GetType())

		sts, ok := acct.(*StsAccount)
		if !assert.True(t, ok, "unexpected concrete account type: %T", acct) {
			return
		}
		credential, err := sts.Credential()
		if !assert.NoError(t, err) {
			return
		}
		if !assert.NotNil(t, credential.AccessKeyId) || !assert.NotNil(t, credential.AccessKeySecret) || !assert.NotNil(t, credential.SecurityToken) {
			return
		}
		assert.Equal(t, "test-id", *credential.AccessKeyId)
		assert.Equal(t, "test-secret", *credential.AccessKeySecret)
		assert.Equal(t, "test-token", *credential.SecurityToken)
	})
}

// TestAccountFromEnvPassesNonblankValuesThroughByteForByte pins the other half of
// the contract: a value that counts as configured is handed to the account exactly
// as it was exported, surrounding whitespace included. The blank-rejection cases
// above show when a value is refused; this one shows that accepting a value never
// rewrites it, which is what "this decides configuration, it does not repair one"
// has to mean before anyone relies on it.
func TestAccountFromEnvPassesNonblankValuesThroughByteForByte(t *testing.T) {
	t.Run("permanent key pair", func(t *testing.T) {
		accessId := "  padded-id \t"
		accessKey := " padded-secret\n"
		exportCredentialEnv(t, &accessId, &accessKey, nil)

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		assert.Equal(t, Aliyun, acct.GetType())

		ak, ok := acct.(*AliyunAccount)
		if !assert.True(t, ok, "unexpected concrete account type: %T", acct) {
			return
		}
		assert.Equal(t, accessId, ak.AccessId(), "the access key id must reach the account byte-for-byte")
		assert.Equal(t, accessKey, ak.AccessKey(), "the access key secret must reach the account byte-for-byte")
	})

	t.Run("temporary credential", func(t *testing.T) {
		accessId := " padded-id "
		accessKey := "\tpadded-secret "
		securityToken := " padded-token \n"
		exportCredentialEnv(t, &accessId, &accessKey, &securityToken)

		acct := AccountFromEnv()
		if !assert.NotNil(t, acct) {
			return
		}
		assert.Equal(t, STS, acct.GetType())

		sts, ok := acct.(*StsAccount)
		if !assert.True(t, ok, "unexpected concrete account type: %T", acct) {
			return
		}
		credential, err := sts.Credential()
		if !assert.NoError(t, err) {
			return
		}
		if credential.AccessKeyId == nil || credential.AccessKeySecret == nil || credential.SecurityToken == nil {
			t.Fatalf("incomplete credential model: %+v", credential)
		}
		assert.Equal(t, accessId, *credential.AccessKeyId)
		assert.Equal(t, accessKey, *credential.AccessKeySecret)
		assert.Equal(t, securityToken, *credential.SecurityToken)
	})
}

// TestAccountFromEnvTokenDoesNotRescueAnIncompleteKeyPair covers the other reading
// of "both are required": the malformed-key cases in
// TestAccountFromEnvIsConfiguredOnlyWithAUsableKeyPair all leave the token unset,
// so on their own they would still pass an implementation that returned an
// StsAccount whenever a token was exported. A non-blank token is not a credential
// pair, so it must not turn an incomplete pair into an account.
func TestAccountFromEnvTokenDoesNotRescueAnIncompleteKeyPair(t *testing.T) {
	securityToken := "test-token"

	cases := []struct {
		name      string
		accessId  *string
		accessKey *string
	}{
		{"id not exported", nil, strPtr("test-secret")},
		{"id exported empty", strPtr(""), strPtr("test-secret")},
		{"id exported blank", strPtr("   "), strPtr("test-secret")},
		{"secret not exported", strPtr("test-id"), nil},
		{"secret exported empty", strPtr("test-id"), strPtr("")},
		{"secret exported blank", strPtr("test-id"), strPtr("\t ")},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			exportCredentialEnv(t, tc.accessId, tc.accessKey, &securityToken)

			assert.Nil(t, AccountFromEnv(),
				"a non-blank ALIBABA_CLOUD_SECURITY_TOKEN must not stand in for a missing or blank access key id/secret")
		})
	}
}
