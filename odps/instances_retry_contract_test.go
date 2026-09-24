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

package odps

import (
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// CreateInstance is the only place in the SDK that re-sends a non-idempotent
// request on its own, so these tests pin exactly which failures it repeats, how
// long it waits, and what the caller finally gets. They talk to a local fake
// service and drive the retry loop's clock, so no MaxCompute credentials are
// needed:
//
//	go test -race -run TestInstancesCreateRetryContract ./odps/
//
// The package-level seams are swapped, not run concurrently: tests in this file
// must stay serial (they do not call t.Parallel).

// retryContractAccount signs with a constant stand-in value. The fake service
// never checks it, and nothing here is a credential.
type retryContractAccount struct{}

func (retryContractAccount) GetType() account2.Provider { return account2.Aliyun }

func (retryContractAccount) SignRequest(req *http.Request, endpoint string) error {
	req.Header.Set(common.HttpHeaderAuthorization, "ODPS test-access-id:stand-in-signature")
	return nil
}

// retryContractReply is one scripted answer of the fake create service.
type retryContractReply struct {
	status  int
	body    string
	headers map[string]string
}

// retryContractObserved is what the fake service saw for one request.
type retryContractObserved struct {
	method        string
	path          string
	rawQuery      string
	body          string
	authorization string
	date          string
}

type retryContractService struct {
	mu       sync.Mutex
	observed []retryContractObserved
	replies  []retryContractReply
}

func (s *retryContractService) requests() []retryContractObserved {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]retryContractObserved, len(s.observed))
	copy(out, s.observed)
	return out
}

// newRetryContractService starts a fake service that answers the scripted
// replies in order. Anything beyond the script is answered with 200 and a plain
// marker, so an unexpected extra request cannot be mistaken for success.
func newRetryContractService(t *testing.T, replies ...retryContractReply) (string, *retryContractService) {
	t.Helper()

	service := &retryContractService{replies: replies}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		service.mu.Lock()
		attempt := len(service.observed)
		service.observed = append(service.observed, retryContractObserved{
			method:        r.Method,
			path:          r.URL.Path,
			rawQuery:      r.URL.RawQuery,
			body:          string(body),
			authorization: r.Header.Get(common.HttpHeaderAuthorization),
			date:          r.Header.Get(common.HttpHeaderDate),
		})
		extra := attempt >= len(service.replies)
		var reply retryContractReply
		if !extra {
			reply = service.replies[attempt]
		}
		service.mu.Unlock()

		if extra {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "UNEXPECTED-EXTRA-REQUEST")
			return
		}

		w.Header().Set("Content-Type", "application/xml")
		for name, value := range reply.headers {
			w.Header().Set(name, value)
		}
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
	}))
	t.Cleanup(server.Close)

	return server.URL, service
}

// withRetryContractClock replaces the create loop's sleep and clock: waits are
// recorded instead of taken, and the fake clock only moves by the recorded
// wait. That makes the 180-second retry budget walkable in memory and lets a
// test assert the exact backoff the loop would apply in production.
func withRetryContractClock(t *testing.T) *[]time.Duration {
	t.Helper()

	var waits []time.Duration
	current := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)

	originalSleep, originalNow := instanceCreateSleep, instanceCreateNow
	instanceCreateSleep = func(d time.Duration) {
		waits = append(waits, d)
		current = current.Add(d)
	}
	instanceCreateNow = func() time.Time { return current }
	t.Cleanup(func() {
		instanceCreateSleep, instanceCreateNow = originalSleep, originalNow
	})

	return &waits
}

func newRetryContractOdps(endpoint string) *Odps {
	odpsIns := NewOdps(retryContractAccount{}, endpoint)
	odpsIns.SetDefaultProjectName("retry_contract_project")
	return odpsIns
}

func conflictReply(retryAfter string, note string) retryContractReply {
	headers := map[string]string{
		"x-odps-request-id": "req-conflict",
	}
	if retryAfter != "" {
		headers["Retry-After"] = retryAfter
	}

	return retryContractReply{
		status:  http.StatusConflict,
		body:    fmt.Sprintf(`<Error><Code>ODPS-0420061</Code><Message>%s</Message></Error>`, note),
		headers: headers,
	}
}

func createdReply() retryContractReply {
	return retryContractReply{
		status:  http.StatusCreated,
		body:    "",
		headers: map[string]string{"Location": "http://service/projects/retry_contract_project/instances/i-created"},
	}
}

func retryContractSQLTask() *SQLTask {
	sqlTask := NewSqlTask("retry_contract_task", "select 1;", nil)
	return &sqlTask
}

// TestInstancesCreateRetryContractRepeatsOnlyUntilSuccess: a 409 is retried
// with the delay the service asked for, and the create then succeeds with the
// instance the service named in Location.
func TestInstancesCreateRetryContractRepeatsOnlyUntilSuccess(t *testing.T) {
	cases := []struct {
		name       string
		retryAfter string
		wantWait   time.Duration
	}{
		{"Retry-After honoured", "2", 2 * time.Second},
		{"no Retry-After falls back to the default", "", 5 * time.Second},
		{"zero Retry-After is not a hot loop", "0", 5 * time.Second},
		{"negative Retry-After is not a hot loop", "-30", 5 * time.Second},
		{"non-numeric Retry-After falls back", "Wed, 21 Oct 2026 07:28:00 GMT", 5 * time.Second},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			waits := withRetryContractClock(t)
			endpoint, service := newRetryContractService(t,
				conflictReply(c.retryAfter, "throttled"),
				createdReply(),
			)

			instance, err := NewInstances(newRetryContractOdps(endpoint)).CreateTask("retry_contract_project", retryContractSQLTask())
			if !assert.NoError(t, err) {
				return
			}

			assert.Equal(t, "i-created", instance.Id())
			assert.Equal(t, 2, len(service.requests()))
			if !assert.Len(t, *waits, 1, "one wait between the two attempts") {
				return
			}
			assert.Equal(t, c.wantWait, (*waits)[0])
		})
	}
}

// TestInstancesCreateRetryContractCapsBackoffAtTheRemainingBudget is the
// "退避上限" case. Retry-After is server controlled and the loop only checks its
// budget before waiting, so without a clamp one "Retry-After: 86400" would keep
// the call blocked for a day - long after the point where it gives up anyway.
func TestInstancesCreateRetryContractCapsBackoffAtTheRemainingBudget(t *testing.T) {
	waits := withRetryContractClock(t)
	endpoint, service := newRetryContractService(t,
		conflictReply("86400", "server asks for a day"),
		conflictReply("86400", "still throttled"),
	)

	instance, err := NewInstances(newRetryContractOdps(endpoint)).CreateTask("retry_contract_project", retryContractSQLTask())
	assert.Nil(t, instance)
	if !assert.Error(t, err) {
		return
	}

	assert.Equal(t, []time.Duration{instanceCreateMaxRetryDuration}, *waits,
		"the wait is cut to the remaining budget instead of a day")
	assert.Equal(t, 2, len(service.requests()),
		"after the budget is spent the loop stops, it does not send a third create")

	var httpErr restclient.HttpError
	if assert.True(t, stderrors.As(err, &httpErr), "the caller must still see the service's 409, got %#v", err) {
		assert.Equal(t, http.StatusConflict, httpErr.StatusCode)
		assert.Contains(t, string(httpErr.Body), "still throttled", "the last answer is what comes back")
	}
}

// TestInstancesCreateRetryContractStopsAtTheBudget: a service that keeps saying
// 409 is retried on the server's schedule until the budget runs out, and the
// error handed back is the last answer.
func TestInstancesCreateRetryContractStopsAtTheBudget(t *testing.T) {
	waits := withRetryContractClock(t)

	replies := []retryContractReply{}
	for i := 0; i < 6; i++ {
		replies = append(replies, conflictReply("60", fmt.Sprintf("throttled %d", i)))
	}
	endpoint, service := newRetryContractService(t, replies...)

	instance, err := NewInstances(newRetryContractOdps(endpoint)).CreateTask("retry_contract_project", retryContractSQLTask())
	assert.Nil(t, instance)
	if !assert.Error(t, err) {
		return
	}

	// 180s budget, 60s per wait: attempts at 0s, 60s, 120s, then the budget is
	// spent and the loop returns without a fourth send.
	assert.Equal(t, []time.Duration{60 * time.Second, 60 * time.Second, 60 * time.Second}, *waits)
	requests := service.requests()
	if !assert.Len(t, requests, 4) {
		return
	}
	for i, r := range requests {
		assert.Equal(t, http.MethodPost, r.method, "request %d", i)
		assert.Contains(t, r.path, "/instances", "request %d", i)
	}

	var httpErr restclient.HttpError
	if assert.True(t, stderrors.As(err, &httpErr)) {
		assert.Contains(t, string(httpErr.Body), "throttled 3", "the error is the newest answer, not the first")
	}
}

// TestInstancesCreateRetryContractDoesNotRepeatAnythingElse is the
// non-idempotency boundary: every failure that is not 409 returns after one
// POST, because the client cannot know whether the service already created the
// instance.
func TestInstancesCreateRetryContractDoesNotRepeatAnythingElse(t *testing.T) {
	cases := []struct {
		name   string
		reply  retryContractReply
		status int
	}{
		{"400", retryContractReply{status: http.StatusBadRequest, body: `<Error><Code>ODPS-0410061</Code></Error>`}, http.StatusBadRequest},
		{"401", retryContractReply{status: http.StatusUnauthorized, body: `<Error><Code>ODPS-0410042</Code></Error>`}, http.StatusUnauthorized},
		{"403", retryContractReply{status: http.StatusForbidden, body: `<Error><Code>ODPS-0420095</Code></Error>`}, http.StatusForbidden},
		// 429 is the one a caller would most expect to be retried. It is not:
		// throttling that arrives as 429 instead of 409 is handed straight
		// back, and a "Retry-After: 1" on it is ignored by the client.
		{"429", retryContractReply{
			status:  http.StatusTooManyRequests,
			body:    `<Error><Code>ODPS-0420105</Code><Message>throttled</Message></Error>`,
			headers: map[string]string{"Retry-After": "1"},
		}, http.StatusTooManyRequests},
		{"500", retryContractReply{status: http.StatusInternalServerError, body: `<Error><Code>ODPS-0110061</Code></Error>`}, http.StatusInternalServerError},
		{"503", retryContractReply{status: http.StatusServiceUnavailable, body: `<Error><Code>ODPS-0120001</Code></Error>`}, http.StatusServiceUnavailable},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			waits := withRetryContractClock(t)
			endpoint, service := newRetryContractService(t, c.reply)

			instance, err := NewInstances(newRetryContractOdps(endpoint)).CreateTask("retry_contract_project", retryContractSQLTask())
			assert.Nil(t, instance)
			if !assert.Error(t, err) {
				return
			}

			assert.Equal(t, 1, len(service.requests()), "only a 409 is repeated")
			assert.Empty(t, *waits, "no wait is scheduled for a failure that is not retried")

			var httpErr restclient.HttpError
			if assert.True(t, stderrors.As(err, &httpErr), "got %#v", err) {
				assert.Equal(t, c.status, httpErr.StatusCode)
			}
		})
	}
}

// TestInstancesCreateRetryContractTransportFailureIsNotRepeated covers the
// case that a status-code policy cannot: the connection died after the POST was
// written. The service answer is unknown, so the client must not send a second
// create that could produce a second instance.
func TestInstancesCreateRetryContractTransportFailureIsNotRepeated(t *testing.T) {
	waits := withRetryContractClock(t)

	var requests int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("test service does not support hijacking")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		// Accept the request, then hang up with no response at all.
		_ = conn.Close()
	}))
	server.Start()
	t.Cleanup(server.Close)

	instance, err := NewInstances(newRetryContractOdps(server.URL)).CreateTask("retry_contract_project", retryContractSQLTask())
	assert.Nil(t, instance)
	if !assert.Error(t, err) {
		return
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&requests),
		"an answer the client never saw must not be re-requested by the create loop")
	assert.Empty(t, *waits)

	var httpErr restclient.HttpError
	assert.False(t, stderrors.As(err, &httpErr), "a transport failure is not a service answer, got %#v", err)
}

// TestInstancesCreateRetryContractResendsTheWholeBody guards the classic retry
// mistake: repeating a request whose body was already consumed. The create loop
// builds a new request per attempt, so each send carries the complete job.
func TestInstancesCreateRetryContractResendsTheWholeBody(t *testing.T) {
	withRetryContractClock(t)
	endpoint, service := newRetryContractService(t,
		conflictReply("1", "throttled"),
		conflictReply("1", "throttled again"),
		createdReply(),
	)

	instance, err := NewInstances(newRetryContractOdps(endpoint)).CreateTask("retry_contract_project", retryContractSQLTask())
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "i-created", instance.Id())

	requests := service.requests()
	if !assert.Len(t, requests, 3) {
		return
	}

	first := requests[0]
	for i, r := range requests {
		assert.Equal(t, first.method, r.method)
		assert.Equal(t, first.path, r.path, "request %d went to a different resource", i)
		assert.Equal(t, first.rawQuery, r.rawQuery, "request %d lost its query", i)
		if !assert.Equalf(t, first.body, r.body, "request %d did not carry the same job body", i) {
			return
		}
		assert.Contains(t, r.body, "retry_contract_task", "request %d body is not the job", i)
		assert.Contains(t, r.body, "select 1", "request %d body is not the job", i)
		assert.NotEmpty(t, r.authorization, "request %d was not signed", i)
		assert.NotEmpty(t, r.date, "request %d has no Date to sign against", i)
	}
	assert.NotEmpty(t, first.body)
}

// TestInstancesCreateRetryContractUsesRotatedCredentialPerAttempt ties the two
// halves of the contract together: because every attempt is signed again, a
// credential the provider refreshed between attempts is used by the retry
// instead of the stale one the first attempt was rejected with.
func TestInstancesCreateRetryContractUsesRotatedCredentialPerAttempt(t *testing.T) {
	withRetryContractClock(t)

	endpoint, service := newRetryContractService(t,
		conflictReply("1", "expired token"),
		createdReply(),
	)

	provider := &rotatingTestProvider{}
	odpsIns := NewOdps(account2.NewStsAccountWithProvider(provider), endpoint)
	odpsIns.SetDefaultProjectName("retry_contract_project")

	instance, err := NewInstances(odpsIns).CreateTask("retry_contract_project", retryContractSQLTask())
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "i-created", instance.Id())

	requests := service.requests()
	if !assert.Len(t, requests, 2) {
		return
	}
	assert.Equal(t, int32(2), provider.callCount(),
		"each attempt asks the provider, so the retry is signed with the newest credential")
	for i, r := range requests {
		assert.True(t, strings.HasPrefix(r.authorization, "ODPS "), "request %d is not signed", i)
	}
}

// rotatingTestProvider hands out a new access key pair on every call, which is
// what a refreshed temporary credential looks like to the SDK. The values are
// stand-ins; the fake service never checks them.
type rotatingTestProvider struct {
	mu      sync.Mutex
	queries int32
}

func (p *rotatingTestProvider) GetType() (*string, error) {
	kind := "sts"
	return &kind, nil
}

func (p *rotatingTestProvider) callCount() int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queries
}

func (p *rotatingTestProvider) GetCredential() (*credentials.CredentialModel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries++
	round := fmt.Sprintf("%d", p.queries)

	id := "ak-round-" + round
	secret := "secret-round-" + round
	token := "token-round-" + round

	return &credentials.CredentialModel{
		AccessKeyId:     &id,
		AccessKeySecret: &secret,
		SecurityToken:   &token,
	}, nil
}

// TestInstancesCreateRetryContractReadPathIsNotRepeated is the read half of the
// same contract: instance status polling is idempotent, but it is still not
// repeated by the client - a transient 5xx while waiting aborts the wait with
// the service's error, which is what a caller has to handle if it wants
// tolerance.
func TestInstancesCreateRetryContractReadPathIsNotRepeated(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<Error><Code>ODPS-0120001</Code><Message>busy</Message></Error>`)
	}))
	t.Cleanup(server.Close)

	odpsIns := newRetryContractOdps(server.URL)
	instance := NewInstance(odpsIns, "retry_contract_project", "i-read")

	err := instance.WaitForSuccess()
	if !assert.Error(t, err, "a transient read failure ends WaitForSuccess") {
		return
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&requests), "the read path sends one request and gives up")

	var httpErr restclient.HttpError
	assert.True(t, stderrors.As(err, &httpErr), "the original service answer must survive, got %#v", err)
	assert.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
}
