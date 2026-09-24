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
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// These tests pin how temporary credentials reach a request, and where the
// boundary of "refresh" actually is. They use local fake providers only - no
// STS calls, no real keys - and replay with
//
//	go test -race -run TestStsAccountRetryContract ./odps/account/
//
// Contract in one line: the account interface has no expiry notion at all, so
// refresh is entirely the provider's job; the SDK's only promise is that the
// provider is consulted again for every signing, which is what makes rotation
// visible without rebuilding the client. The string form of an STS account has
// no provider, so it can never refresh.

// strp avoids pulling a helper dependency into the test just to build a
// CredentialModel.
func strp(value string) *string { return &value }

// fixedDate is the Date header these tests sign with. The signature covers it,
// so a fixed value is what lets a test compare "the same request signed with
// generation N" against the account's own output.
const fixedDate = "Wed, 01 Jan 2025 00:00:00 GMT"

func newSignedRequest(t *testing.T, method, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set(common.HttpHeaderDate, fixedDate)
	return req
}

// expectedAuthorization signs an identical request with a plain AliyunAccount,
// which is what the SDK does internally with the credential it just fetched.
func expectedAuthorization(t *testing.T, accessId, accessKey, regionID string, endpoint string) string {
	t.Helper()

	signer := NewAliyunAccount(accessId, accessKey)
	if regionID != "" {
		signer = NewAliyunAccount(accessId, accessKey, regionID)
	}

	req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
	if err := signer.SignRequest(req, endpoint); err != nil {
		t.Fatalf("expected signature: %v", err)
	}
	return req.Header.Get(common.HttpHeaderAuthorization)
}

// generation is one credential a fake provider hands out, with a label so a
// test can tell which generation signed a request.
type generation struct {
	accessId  string
	accessKey string
	token     string
}

func (g generation) model() *credentials.CredentialModel {
	model := &credentials.CredentialModel{
		AccessKeyId:     strp(g.accessId),
		AccessKeySecret: strp(g.accessKey),
	}
	if g.token != "" {
		model.SecurityToken = strp(g.token)
	}
	return model
}

// fakeCredential implements credentials.Credential (the alibabacloud-go
// provider shape) over a fixed list of generations. The last generation is
// repeated forever, which is enough to model "the token was refreshed and then
// stayed valid".
type fakeCredential struct {
	mu          sync.Mutex
	generations []generation
	next        int
	calls       int32
	failFrom    int32                        // when > 0, GetCredential errors from this call on
	fixed       *credentials.CredentialModel // served verbatim when fixedSet is true
	fixedSet    bool                         // lets the fake hand back a nil model
}

func (f *fakeCredential) advance() generation {
	f.mu.Lock()
	defer f.mu.Unlock()

	index := f.next
	if index >= len(f.generations) {
		index = len(f.generations) - 1
	}
	if f.next < len(f.generations)-1 {
		f.next++
	}

	return f.generations[index]
}

func (f *fakeCredential) GetCredential() (*credentials.CredentialModel, error) {
	count := atomic.AddInt32(&f.calls, 1)
	if f.failFrom > 0 && count >= f.failFrom {
		return nil, stderrors.New("fake provider is unavailable")
	}
	if f.fixedSet {
		if f.fixed == nil {
			return nil, nil
		}
		model := *f.fixed
		return &model, nil
	}

	g := f.advance()
	// A real provider returns a value per call; copying keeps this test honest
	// about the SDK not holding on to a shared model.
	model := g.model()
	return model, nil
}

func (f *fakeCredential) GetAccessKeyId() (*string, error) {
	model, err := f.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.AccessKeyId, nil
}

func (f *fakeCredential) GetAccessKeySecret() (*string, error) {
	model, err := f.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.AccessKeySecret, nil
}

func (f *fakeCredential) GetSecurityToken() (*string, error) {
	model, err := f.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.SecurityToken, nil
}

func (f *fakeCredential) GetBearerToken() *string { return nil }

func (f *fakeCredential) GetType() *string { return strp("sts") }

func (f *fakeCredential) callCount() int32 { return atomic.LoadInt32(&f.calls) }

// fakeCredentialProvider implements the SDK's own CredentialProvider, which is
// the extension point callers use when they do not want credentials-go.
type fakeCredentialProvider struct {
	mu          sync.Mutex
	model       *credentials.CredentialModel
	err         error
	calls       int32
	models      []*credentials.CredentialModel
	next        int
	typeQueries int32
}

func (p *fakeCredentialProvider) GetType() (*string, error) {
	atomic.AddInt32(&p.typeQueries, 1)
	return strp("sts"), nil
}

func (p *fakeCredentialProvider) GetCredential() (*credentials.CredentialModel, error) {
	atomic.AddInt32(&p.calls, 1)
	if p.err != nil {
		return nil, p.err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.models) > 0 {
		index := p.next
		if index >= len(p.models) {
			index = len(p.models) - 1
		}
		if p.next < len(p.models)-1 {
			p.next++
		}
		return p.models[index], nil
	}

	return p.model, nil
}

// TestStsAccountRetryContractProviderIsConsultedPerSigning is the refresh
// boundary: rotation reaches the next request without rebuilding anything,
// because signing asks the provider again every time.
func TestStsAccountRetryContractProviderIsConsultedPerSigning(t *testing.T) {
	generations := []generation{
		{accessId: "ak-gen-1", accessKey: "secret-gen-1", token: "token-gen-1"},
		{accessId: "ak-gen-2", accessKey: "secret-gen-2", token: "token-gen-2"},
	}
	endpoint := "http://127.0.0.1:1"

	t.Run("credentials-go provider", func(t *testing.T) {
		provider := &fakeCredential{generations: generations}
		stsAccount := NewStsAccountWithCredential(provider)

		for i, g := range generations {
			req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
			err := stsAccount.SignRequest(req, endpoint)
			if !assert.NoError(t, err, "signing %d", i) {
				return
			}
			assert.Equal(t, g.token, req.Header.Get(common.HttpHeaderAuthorizationSTSToken),
				"signing %d must carry the token of its own generation", i)
			assert.Equal(t,
				expectedAuthorization(t, g.accessId, g.accessKey, "", endpoint),
				req.Header.Get(common.HttpHeaderAuthorization),
				"signing %d must be signed with the matching secret, not a stale one", i)
		}

		// Past the last generation the provider keeps serving it, and signing
		// still consults it: the call count is one per signing, never cached.
		req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
		assert.NoError(t, stsAccount.SignRequest(req, endpoint))
		assert.Equal(t, int32(3), provider.callCount(), "the provider must be asked once per signing")
	})

	t.Run("SDK CredentialProvider", func(t *testing.T) {
		provider := &fakeCredentialProvider{models: []*credentials.CredentialModel{
			generations[0].model(), generations[1].model(),
		}}
		stsAccount := NewStsAccountWithProvider(provider)

		for i, g := range generations {
			req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
			if !assert.NoError(t, stsAccount.SignRequest(req, endpoint), "signing %d", i) {
				return
			}
			assert.Equal(t, g.token, req.Header.Get(common.HttpHeaderAuthorizationSTSToken))
			assert.Equal(t, expectedAuthorization(t, g.accessId, g.accessKey, "", endpoint),
				req.Header.Get(common.HttpHeaderAuthorization))
		}
		assert.Equal(t, int32(2), provider.calls)
	})

	t.Run("region is fixed at construction, not taken from the credential", func(t *testing.T) {
		provider := &fakeCredential{generations: generations}
		stsAccount := NewStsAccountWithCredential(provider, "cn-hangzhou")

		req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
		if !assert.NoError(t, stsAccount.SignRequest(req, endpoint)) {
			return
		}
		assert.Equal(t,
			expectedAuthorization(t, "ak-gen-1", "secret-gen-1", "cn-hangzhou", endpoint),
			req.Header.Get(common.HttpHeaderAuthorization),
			"an account built with a region must sign V4 for that region")
	})
}

// TestStsAccountRetryContractProviderFailureIsReported: when the provider
// cannot produce a credential, signing fails - it does not send a request
// signed with whatever was left over.
func TestStsAccountRetryContractProviderFailureIsReported(t *testing.T) {
	endpoint := "http://127.0.0.1:1"

	t.Run("credentials-go provider errors", func(t *testing.T) {
		provider := &fakeCredential{
			generations: []generation{{accessId: "ak-gen-1", accessKey: "secret-gen-1", token: "token-gen-1"}},
			failFrom:    1,
		}
		stsAccount := NewStsAccountWithCredential(provider)

		req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
		err := stsAccount.SignRequest(req, endpoint)
		if !assert.Error(t, err) {
			return
		}
		assert.Empty(t, req.Header.Get(common.HttpHeaderAuthorization), "a failed signing must not leave a signature")
		assert.NotContains(t, err.Error(), "secret-gen-1", "the failure must not echo credential material")
	})

	t.Run("SDK provider errors", func(t *testing.T) {
		provider := &fakeCredentialProvider{err: stderrors.New("metadata service unreachable")}
		stsAccount := NewStsAccountWithProvider(provider)

		req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
		err := stsAccount.SignRequest(req, endpoint)
		if !assert.Error(t, err) {
			return
		}
		assert.Contains(t, err.Error(), "metadata service unreachable")
	})
}

// TestStsAccountRetryContractStringTokenNeverRefreshes pins the other half of
// the answer: NewStsAccount(id, key, token) has no provider, so the token is
// fixed for the lifetime of the account. Long-running jobs must use a provider,
// or rebuild the account - there is no in-place refresh.
func TestStsAccountRetryContractStringTokenNeverRefreshes(t *testing.T) {
	endpoint := "http://127.0.0.1:1"
	stsAccount := NewStsAccount("ak-static", "secret-static", "token-static")

	first := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
	assert.NoError(t, stsAccount.SignRequest(first, endpoint))
	second := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
	assert.NoError(t, stsAccount.SignRequest(second, endpoint))

	assert.Equal(t, "token-static", first.Header.Get(common.HttpHeaderAuthorizationSTSToken))
	assert.Equal(t, first.Header.Get(common.HttpHeaderAuthorization),
		second.Header.Get(common.HttpHeaderAuthorization))
	assert.Equal(t, first.Header.Get(common.HttpHeaderAuthorizationSTSToken),
		second.Header.Get(common.HttpHeaderAuthorizationSTSToken),
		"a string STS account cannot rotate; the same token is sent forever")

	// Credential() reflects the same frozen values, so nothing downstream can
	// refresh from it either.
	model, err := stsAccount.Credential()
	if assert.NoError(t, err) && assert.NotNil(t, model) {
		assert.Equal(t, "ak-static", *model.AccessKeyId)
		assert.Equal(t, "token-static", *model.SecurityToken)
	}
}

// TestStsAccountRetryContractTokenFieldSelection states which field of a
// provider reply becomes the STS header.
func TestStsAccountRetryContractTokenFieldSelection(t *testing.T) {
	endpoint := "http://127.0.0.1:1"

	cases := []struct {
		name          string
		model         *credentials.CredentialModel
		wantToken     string
		wantHeaderSet bool
	}{
		{
			name: "security token is the temporary credential",
			model: &credentials.CredentialModel{
				AccessKeyId: strp("ak"), AccessKeySecret: strp("sk"), SecurityToken: strp("sts-token"),
			},
			wantToken:     "sts-token",
			wantHeaderSet: true,
		},
		{
			name: "security token wins when both fields are filled",
			model: &credentials.CredentialModel{
				AccessKeyId: strp("ak"), AccessKeySecret: strp("sk"),
				SecurityToken: strp("sts-token"), BearerToken: strp("bearer-token"),
			},
			wantToken:     "sts-token",
			wantHeaderSet: true,
		},
		{
			name: "bearer token still works as a fallback",
			model: &credentials.CredentialModel{
				AccessKeyId: strp("ak"), AccessKeySecret: strp("sk"), BearerToken: strp("bearer-token"),
			},
			wantToken:     "bearer-token",
			wantHeaderSet: true,
		},
		{
			name:          "a long-lived key sends no token",
			model:         &credentials.CredentialModel{AccessKeyId: strp("ak"), AccessKeySecret: strp("sk")},
			wantHeaderSet: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			provider := &fakeCredentialProvider{model: c.model}
			stsAccount := NewStsAccountWithProvider(provider)

			req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
			if !assert.NoError(t, stsAccount.SignRequest(req, endpoint)) {
				return
			}

			token := req.Header.Get(common.HttpHeaderAuthorizationSTSToken)
			if c.wantHeaderSet {
				assert.Equal(t, c.wantToken, token)
			} else {
				assert.Empty(t, token, "no token field means no authorization-sts-token header")
			}
			assert.NotEmpty(t, req.Header.Get(common.HttpHeaderAuthorization))
		})
	}
}

// TestStsAccountRetryContractIncompleteCredentialIsAnError: a provider reply
// that cannot be signed with is reported to the caller. It used to be a nil
// dereference, i.e. one bad provider reply killed the process (and printed its
// surroundings to stderr) instead of failing the call. Both provider shapes go
// through the same helper, so both are covered here.
func TestStsAccountRetryContractIncompleteCredentialIsAnError(t *testing.T) {
	endpoint := "http://127.0.0.1:1"

	const idValue = "leaked-check-id"
	const secretValue = "leaked-check-secret"
	const tokenValue = "leaked-check-token"

	cases := []struct {
		name  string
		model *credentials.CredentialModel
	}{
		{"nil credential", nil},
		{"empty credential", &credentials.CredentialModel{}},
		{"no access key id", &credentials.CredentialModel{AccessKeySecret: strp(secretValue)}},
		{"empty access key id", &credentials.CredentialModel{AccessKeyId: strp(""), AccessKeySecret: strp(secretValue)}},
		{"no access key secret", &credentials.CredentialModel{AccessKeyId: strp(idValue)}},
		{
			name: "no access key secret but a token present",
			model: &credentials.CredentialModel{
				AccessKeyId: strp(idValue), SecurityToken: strp(tokenValue),
			},
		},
	}

	for _, providerKind := range []string{"SDK provider", "credentials-go provider"} {
		for _, c := range cases {
			t.Run(providerKind+"/"+c.name, func(t *testing.T) {
				var stsAccount *StsAccount
				switch providerKind {
				case "SDK provider":
					stsAccount = NewStsAccountWithProvider(&fakeCredentialProvider{model: c.model})
				default:
					stsAccount = NewStsAccountWithCredential(&fakeCredential{fixed: c.model, fixedSet: true})
				}

				req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")

				var err error
				assert.NotPanics(t, func() { err = stsAccount.SignRequest(req, endpoint) },
					"an unusable credential must fail the signing, not the process")
				if !assert.Error(t, err) {
					return
				}
				assert.Empty(t, req.Header.Get(common.HttpHeaderAuthorization),
					"a failed signing must not leave a signature behind")
				assert.NotContains(t, err.Error(), idValue, "the error must not echo credential material")
				assert.NotContains(t, err.Error(), secretValue, "the error must not echo credential material")
				assert.NotContains(t, err.Error(), tokenValue, "the error must not echo credential material")
			})
		}
	}

	t.Run("a complete reply is signed", func(t *testing.T) {
		complete := &credentials.CredentialModel{
			AccessKeyId: strp(idValue), AccessKeySecret: strp(secretValue), SecurityToken: strp(tokenValue),
		}
		stsAccount := NewStsAccountWithProvider(&fakeCredentialProvider{model: complete})
		req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
		if !assert.NoError(t, stsAccount.SignRequest(req, endpoint)) {
			return
		}
		assert.Equal(t, tokenValue, req.Header.Get(common.HttpHeaderAuthorizationSTSToken))
		assert.Equal(t, expectedAuthorization(t, idValue, secretValue, "", endpoint),
			req.Header.Get(common.HttpHeaderAuthorization))
	})
}

// TestStsAccountRetryContractConcurrentSigning is the concurrency half of the
// credential acceptance: many goroutines signing through one account must each
// get a whole credential generation, never a mix, and must not race. Run it with
// -race.
func TestStsAccountRetryContractConcurrentSigning(t *testing.T) {
	endpoint := "http://127.0.0.1:1"
	generations := []generation{
		{accessId: "ak-1", accessKey: "secret-1", token: "token-1"},
		{accessId: "ak-2", accessKey: "secret-2", token: "token-2"},
		{accessId: "ak-3", accessKey: "secret-3", token: "token-3"},
	}

	// Pre-compute the signature of every generation for this exact request so
	// each observed Authorization can be matched to one generation.
	type validCredential struct {
		authorization string
		token         string
	}
	var valid []validCredential
	for _, g := range generations {
		valid = append(valid, validCredential{
			authorization: expectedAuthorization(t, g.accessId, g.accessKey, "", endpoint),
			token:         g.token,
		})
	}
	// The provider serves each generation a bounded number of times and then
	// sticks to the last one, so the set above covers every possible answer.
	provider := &fakeCredential{generations: generations}
	stsAccount := NewStsAccountWithCredential(provider)

	const goroutines = 8
	const perGoroutine = 25

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)
	mismatches := make(chan string, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < perGoroutine; i++ {
				req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
				if err := stsAccount.SignRequest(req, endpoint); err != nil {
					errs <- err
					continue
				}
				authorization := req.Header.Get(common.HttpHeaderAuthorization)
				token := req.Header.Get(common.HttpHeaderAuthorizationSTSToken)
				matched := false
				for _, v := range valid {
					if v.authorization == authorization && v.token == token {
						matched = true
						break
					}
				}
				if !matched {
					mismatches <- fmt.Sprintf("authorization=%q token=%q", authorization, token)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(mismatches)

	for err := range errs {
		t.Errorf("concurrent signing failed: %v", err)
	}
	for mismatch := range mismatches {
		t.Errorf("a torn credential pair was sent: %s", mismatch)
	}
	assert.Equal(t, int32(goroutines*perGoroutine), provider.callCount(),
		"every signing asks the provider")
}

// TestStsAccountRetryContractClockBoundary states, and keeps testable, the part
// a controllable clock cannot reach: expiry is not the SDK's decision. Nothing
// in account/ reads an expiration, and SignRequest stamps the Date header from
// the wall clock (in restclient.Do), so the only expiry behaviour a caller can
// rely on is the provider's own refresh window.
func TestStsAccountRetryContractClockBoundary(t *testing.T) {
	endpoint := "http://127.0.0.1:1"

	// A provider whose credential is already stale by an hour still gets used:
	// the SDK does not track expiry, so it neither pre-refreshes nor rejects.
	expired := generation{accessId: "ak-expired", accessKey: "secret-expired", token: "token-expired"}
	provider := &fakeCredentialProvider{models: []*credentials.CredentialModel{expired.model()}}
	stsAccount := NewStsAccountWithProvider(provider)

	req := newSignedRequest(t, http.MethodGet, endpoint+"/projects/p")
	_ = time.Now
	if !assert.NoError(t, stsAccount.SignRequest(req, endpoint)) {
		return
	}
	assert.Equal(t, expired.token, req.Header.Get(common.HttpHeaderAuthorizationSTSToken))
	assert.True(t, strings.HasPrefix(req.Header.Get(common.HttpHeaderAuthorization), "ODPS "),
		"V2 signature shape")
	assert.Equal(t, int32(1), provider.calls)
}
