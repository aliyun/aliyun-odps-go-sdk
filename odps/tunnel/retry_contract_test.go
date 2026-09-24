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

package tunnel

import (
	stderrors "errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// tunnel.Retry is the SDK's other retry, used by the tunnel session calls.
// These tests run against local fake services and need no credentials:
//
//	go test -race -run TestTunnelRetryContract ./odps/tunnel/

// withRetryContractSleep records the waits Retry schedules instead of taking
// them. Tests here stay serial because the seam is package level.
func withRetryContractSleep(t *testing.T) *[]time.Duration {
	t.Helper()

	var waits []time.Duration
	original := retrySleep
	retrySleep = func(d time.Duration) { waits = append(waits, d) }
	t.Cleanup(func() { retrySleep = original })

	return &waits
}

// retryContractTunnelAccount signs with a stand-in value; the fake service never
// checks it and nothing here is a credential.
type retryContractTunnelAccount struct{}

func (retryContractTunnelAccount) GetType() account.Provider { return account.Aliyun }

func (retryContractTunnelAccount) SignRequest(req *http.Request, endpoint string) error {
	req.Header.Set("Authorization", "ODPS test-access-id:stand-in-signature")
	return nil
}

func newRetryContractRestClient(endpoint string) restclient.RestClient {
	return restclient.NewOdpsRestClient(retryContractTunnelAccount{}, endpoint)
}

// TestTunnelRetryContractAttemptCountAndBackoff states the loop's shape: three
// attempts, doubling waits between them, the last error handed back unchanged.
func TestTunnelRetryContractAttemptCountAndBackoff(t *testing.T) {
	waits := withRetryContractSleep(t)

	last := stderrors.New("third failure")
	errFirst := stderrors.New("first failure")
	errSecond := stderrors.New("second failure")

	var seen []error
	seen = append(seen, errFirst, errSecond, last)

	var calls int32
	err := Retry(func() error {
		n := atomic.AddInt32(&calls, 1)
		if int(n) < len(seen) {
			return seen[n-1]
		}
		return last
	})

	assert.Equal(t, int32(maxRetryAttempts), atomic.LoadInt32(&calls))
	assert.Equal(t, []time.Duration{1 * time.Second, 2 * time.Second}, *waits,
		"the loop waits between attempts, not after the last one")
	assert.Same(t, last, err, "the caller must get the error of the final attempt")
}

// TestTunnelRetryContractStopsOnFirstSuccess is the control: a closure that
// succeeds costs one call and no wait.
func TestTunnelRetryContractStopsOnFirstSuccess(t *testing.T) {
	waits := withRetryContractSleep(t)

	var calls int32
	err := Retry(func() error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	assert.Empty(t, *waits)
}

// TestTunnelRetryContractDoesNotClassifyErrors documents the sharp edge: Retry
// repeats whatever its closure returns, including a 4xx the service will reject
// identically on the next attempt. A caller that must not repeat a request has
// to make that decision itself.
func TestTunnelRetryContractDoesNotClassifyErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `<Error><Code>scripted</Code></Error>`)
		}))
		t.Cleanup(server.Close)

		waits := withRetryContractSleep(t)
		client := newRetryContractRestClient(server.URL)
		req, err := client.NewRequestWithUrlQuery(http.MethodPost, "projects/p/tunnel", nil, url.Values{})
		if !assert.NoError(t, err) {
			return
		}

		var lastErr error
		err = Retry(func() error {
			res, doErr := client.Do(req)
			if doErr != nil {
				lastErr = doErr
				return doErr
			}
			if res.Body != nil {
				_ = res.Body.Close()
			}
			return nil
		})

		assert.Error(t, err, "status %d", status)
		assert.Equal(t, int32(maxRetryAttempts), atomic.LoadInt32(&hits),
			"status %d is repeated by tunnel.Retry regardless of what it means", status)
		assert.Len(t, *waits, maxRetryAttempts-1)

		var httpErr restclient.HttpError
		if assert.True(t, stderrors.As(lastErr, &httpErr)) {
			assert.Equal(t, status, httpErr.StatusCode)
		}
	}
}

// TestTunnelRetryContractReusedRequestBodyReplay shows what happens when a
// caller repeats the *same* http.Request through Retry, which is what the
// tunnel session calls do. For a body built from the in-memory types
// http.NewRequest recognises (strings.Reader, bytes.Reader, bytes.Buffer) the
// client replays the whole body on every attempt, so repeating one request
// object is safe.
func TestTunnelRetryContractReusedRequestBodyReplay(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var declared []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		declared = append(declared, r.ContentLength)
		mu.Unlock()
		if readErr != nil {
			t.Errorf("read body: %v", readErr)
		}

		if len(bodies) < maxRetryAttempts {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>scripted</Code></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	withRetryContractSleep(t)
	client := newRetryContractRestClient(server.URL)

	req, err := client.NewRequestWithUrlQuery(http.MethodPost, "projects/p/tunnel",
		strings.NewReader("payload-that-matters"), url.Values{})
	if !assert.NoError(t, err) {
		return
	}

	err = Retry(func() error {
		res, doErr := client.Do(req)
		if doErr != nil {
			return doErr
		}
		if res.Body != nil {
			_ = res.Body.Close()
		}
		return nil
	})
	assert.NoError(t, err, "the third attempt is scripted to succeed")

	mu.Lock()
	defer mu.Unlock()
	if !assert.Len(t, bodies, maxRetryAttempts) {
		return
	}
	for i, body := range bodies {
		assert.Equal(t, "payload-that-matters", body,
			"attempt %d did not replay the body, so the service would have seen a truncated request", i)
		assert.Equal(t, int64(len("payload-that-matters")), declared[i], "attempt %d declared a different length", i)
	}
}

// TestTunnelRetryContractReusedStreamingBodyIsSilentlyEmpty is the hazard the
// shape above does not cover: a body that is not one of the in-memory types
// carries no ContentLength and no GetBody, so re-sending the same request
// streams an EMPTY body and the client reports no error at all. A caller with a
// stream must build a fresh request per attempt, or buffer.
func TestTunnelRetryContractReusedStreamingBodyIsSilentlyEmpty(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()

		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<Error><Code>scripted</Code></Error>`)
	}))
	t.Cleanup(server.Close)

	withRetryContractSleep(t)
	client := newRetryContractRestClient(server.URL)

	req, err := client.NewRequestWithUrlQuery(http.MethodPost, "projects/p/tunnel",
		&retryContractStream{reader: strings.NewReader("payload-that-matters")}, url.Values{})
	if !assert.NoError(t, err) {
		return
	}
	assert.Zero(t, req.ContentLength, "an unrecognised body type has no declared length")
	assert.Nil(t, req.GetBody, "and no way to replay it")

	err = Retry(func() error {
		res, doErr := client.Do(req)
		if doErr != nil {
			return doErr
		}
		if res.Body != nil {
			_ = res.Body.Close()
		}
		return nil
	})
	assert.Error(t, err, "the service keeps answering 503")

	mu.Lock()
	defer mu.Unlock()
	if !assert.Len(t, bodies, maxRetryAttempts) {
		return
	}
	assert.Equal(t, "payload-that-matters", bodies[0], "the first attempt streams the body")
	for i := 1; i < len(bodies); i++ {
		assert.Empty(t, bodies[i],
			"attempt %d sent an empty body, and the client reported nothing - this is why a stream must not be re-sent as the same request", i)
	}
}

// retryContractStream is a plain io.Reader: not seekable, not one of the types
// http.NewRequest snapshots, so it is consumed by the first send.
type retryContractStream struct {
	reader io.Reader
}

func (s *retryContractStream) Read(p []byte) (int, error) { return s.reader.Read(p) }

// TestTunnelRetryContractFreshRequestPerAttempt is what a body-carrying tunnel
// caller must do instead: build the request inside the closure so each attempt
// gets its own body.
func TestTunnelRetryContractFreshRequestPerAttempt(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()

		if len(bodies) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>scripted</Code></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	withRetryContractSleep(t)
	client := newRetryContractRestClient(server.URL)

	err := Retry(func() error {
		req, reqErr := client.NewRequestWithUrlQuery(http.MethodPost, "projects/p/tunnel",
			strings.NewReader("payload-that-matters"), url.Values{})
		if reqErr != nil {
			return reqErr
		}

		res, doErr := client.Do(req)
		if doErr != nil {
			return doErr
		}
		if res.Body != nil {
			_ = res.Body.Close()
		}
		return nil
	})
	assert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	if !assert.Len(t, bodies, 3) {
		return
	}
	for i, body := range bodies {
		assert.Equal(t, "payload-that-matters", body, "attempt %d lost part of its body", i)
	}
}
