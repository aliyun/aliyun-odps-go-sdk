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

package restclient

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

// What these tests pin is the failure contract of the REST layer itself: how
// many times a request goes out, what the caller gets back, and what is lost
// on the way. They only talk to local fake services, so they need no
// MaxCompute credentials and can be replayed with
//
//	go test -race -run TestRestClientRetryContract ./odps/restclient/
//
// Headline result: RestClient.Do issues exactly one request per call. Nothing
// in this package retries a 429, a 5xx, a connection failure or a client
// timeout. The retries that exist in the SDK live in the callers
// (odps.Instances.CreateTask re-sends on 409 only, odps/tunnel.Retry re-sends
// whatever its closure returns), so the retry policy of a request is decided by
// the caller and has to be judged against that request's idempotency.

// replyScript is one served response: status plus body, with optional extra
// headers.
type replyScript struct {
	status  int
	body    string
	headers map[string]string
}

// retryContractServer serves replyScript[i] for request i+1 and answers 200 with
// an unmistakable marker body for any request beyond the script, so a retry
// cannot pass unnoticed: hits records the attempt count, and the marker is not
// something any caller can mistake for the service.
func retryContractServer(t *testing.T, replies ...replyScript) (string, *int32) {
	t.Helper()

	var hits int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := int(atomic.AddInt32(&hits, 1))

		if attempt > len(replies) {
			// Plain text on purpose: it cannot be decoded as XML or parsed as
			// an error document, so a request that went out twice is visible as
			// a failure and as an inflated hit count.
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `UNEXPECTED-SECOND-REQUEST-ON-ATTEMPT-%d`, attempt)
			return
		}

		reply := replies[attempt-1]
		w.Header().Set("Content-Type", "application/xml")
		for name, value := range reply.headers {
			w.Header().Set(name, value)
		}
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
	}))
	t.Cleanup(server.Close)

	return server.URL, &hits
}

func retryContractErrorBody(status int) string {
	return fmt.Sprintf(`<Error><Code>ODPS-%d</Code><Message>service said %d</Message><RequestId>req-1</RequestId></Error>`, status, status)
}

// TestRestClientRetryContractNon2xxIsNotRetried covers every status the service
// uses to say "try later" or "you failed": one attempt each, the caller gets a
// HttpError carrying status, reason, request id, server headers and the body.
func TestRestClientRetryContractNon2xxIsNotRetried(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"bad request", http.StatusBadRequest},
		{"unauthorized", http.StatusUnauthorized},
		{"forbidden", http.StatusForbidden},
		{"not found", http.StatusNotFound},
		{"conflict", http.StatusConflict},
		{"throttled", http.StatusTooManyRequests},
		{"internal error", http.StatusInternalServerError},
		{"bad gateway", http.StatusBadGateway},
		{"unavailable", http.StatusServiceUnavailable},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			endpoint, hits := retryContractServer(t, replyScript{
				status: c.status,
				body:   retryContractErrorBody(c.status),
				headers: map[string]string{
					"x-odps-request-id": "req-1",
					"Retry-After":       "1",
				},
			})

			client := NewOdpsRestClient(MockAccount{}, endpoint)
			var decoded struct {
				Code string `xml:"Code"`
			}

			err := client.GetWithModel("projects/p/instances/i", url.Values{}, nil, &decoded)
			if !assert.Error(t, err, "a non-2xx response must surface as an error") {
				return
			}
			assert.Equal(t, int32(1), atomic.LoadInt32(hits),
				"the REST layer must not re-send a failed request by itself")

			var httpErr HttpError
			if !assert.True(t, stderrors.As(err, &httpErr), "the error must stay inspectable as HttpError, got %#v", err) {
				return
			}
			assert.Equal(t, c.status, httpErr.StatusCode)
			assert.Equal(t, "req-1", httpErr.RequestId)
			assert.Contains(t, string(httpErr.Body), fmt.Sprintf("ODPS-%d", c.status))
			if !assert.NotNil(t, httpErr.ErrorMessage, "the error body must be parsed for the caller") {
				return
			}
			assert.Equal(t, fmt.Sprintf("ODPS-%d", c.status), httpErr.ErrorMessage.ErrorCode)
			assert.Equal(t, fmt.Sprintf("service said %d", c.status), httpErr.ErrorMessage.Message)

			// Server headers survive the failure: Retry-After is how a caller
			// decides to back off, so it must stay reachable from the error.
			if !assert.NotNil(t, httpErr.Response, "HttpError must keep the response") {
				return
			}
			assert.Equal(t, "1", httpErr.Response.Header.Get("Retry-After"))

			// ... but the body is already consumed by NewHttpNotOk, so the only
			// readable copy of the server error is HttpError.Body.
			remaining, readErr := io.ReadAll(httpErr.Response.Body)
			assert.Empty(t, remaining, "HttpError.Response.Body is drained; use HttpError.Body")
			if assert.Error(t, readErr, "the drained body is closed, not just empty") {
				assert.Contains(t, readErr.Error(), "closed response body")
			}
		})
	}
}

// TestRestClientRetryContract2xxIsSuccess is the control for the table above:
// 2xx never errors at the Do level, including the 2xx codes the SDK uses on
// writes. The 204 sub-case records a trap: Do is happy with an empty body, but
// DoWithModel still feeds it to the XML decoder and the caller gets io.EOF.
func TestRestClientRetryContract2xxIsSuccess(t *testing.T) {
	t.Run("200 and 201 decode", func(t *testing.T) {
		for _, status := range []int{http.StatusOK, http.StatusCreated} {
			endpoint, hits := retryContractServer(t, replyScript{
				status: status,
				body:   `<TestResponse><Message>ok</Message></TestResponse>`,
			})

			client := NewOdpsRestClient(MockAccount{}, endpoint)
			var decoded struct {
				Message string `xml:"Message"`
			}

			assert.NoError(t, client.GetWithModel("projects/p", url.Values{}, nil, &decoded), "status %d", status)
			assert.Equal(t, int32(1), atomic.LoadInt32(hits))
			assert.Equal(t, "ok", decoded.Message)
		}
	})

	t.Run("204 has no body to decode", func(t *testing.T) {
		endpoint, hits := retryContractServer(t,
			replyScript{status: http.StatusNoContent},
			replyScript{status: http.StatusNoContent})

		client := NewOdpsRestClient(MockAccount{}, endpoint)

		// The request itself is a success.
		req, err := client.NewRequest(http.MethodGet, "projects/p", nil)
		assert.NoError(t, err)
		res, err := client.Do(req)
		if assert.NoError(t, err, "2xx must not error at the Do level") && res != nil {
			assert.Equal(t, http.StatusNoContent, res.StatusCode)
			_ = res.Body.Close()
		}

		// A model-parsing caller of the same response sees io.EOF.
		var decoded struct {
			Message string `xml:"Message"`
		}
		modelErr := client.GetWithModel("projects/p", url.Values{}, nil, &decoded)
		if assert.Error(t, modelErr, "an empty success body cannot be decoded into a model") {
			assert.True(t, stderrors.Is(modelErr, io.EOF), "the caller must be able to tell it was EOF, got %#v", modelErr)
		}
		var httpErr HttpError
		assert.False(t, stderrors.As(modelErr, &httpErr), "the failure comes from decoding, not from the status")
		assert.Equal(t, int32(2), atomic.LoadInt32(hits), "one attempt per call, still no retry")
	})
}

// TestRestClientRetryContractTransportErrorKeepsOriginalError shows what a
// caller gets when the connection dies before a response: one attempt, the
// transport error itself (not an HttpError), with the service's answer never
// seen. A caller cannot tell from this error whether the request was executed,
// which is why re-sending a write on it is not obviously safe.
func TestRestClientRetryContractTransportErrorKeepsOriginalError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		return
	}

	var dialed int32
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			atomic.AddInt32(&dialed, 1)
			// Drain the request so it is definitely on the wire, then hang up
			// without any response. This is the shape a caller cannot retry
			// safely: the service may have acted before the connection died.
			reader := bufio.NewReader(conn)
			for {
				line, readErr := reader.ReadString('\n')
				if readErr != nil || line == "\r\n" {
					break
				}
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })

	client := NewOdpsRestClient(MockAccount{}, "http://"+listener.Addr().String())
	var decoded struct {
		Message string `xml:"Message"`
	}

	callErr := client.GetWithModel("projects/p", url.Values{}, nil, &decoded)
	if !assert.Error(t, callErr) {
		return
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&dialed),
		"a connection that died after the request was sent must not be dialled again")

	var httpErr HttpError
	assert.False(t, stderrors.As(callErr, &httpErr), "a transport failure is not an HttpError")

	var urlErr *url.Error
	assert.True(t, stderrors.As(callErr, &urlErr), "the transport error must stay a *url.Error, got %#v", callErr)
	assert.Contains(t, callErr.Error(), "EOF", "the raw transport failure is what the caller sees")

	// Connection refused (nothing listening) is the other common shape and it
	// also stops after one client-level attempt.
	_ = listener.Close()
	refusedClient := NewOdpsRestClient(MockAccount{}, "http://"+listener.Addr().String())
	refusedErr := refusedClient.GetWithModel("projects/p", url.Values{}, nil, &decoded)
	if !assert.Error(t, refusedErr) {
		return
	}
	assert.Contains(t, refusedErr.Error(), "connection refused")
	assert.False(t, stderrors.As(refusedErr, &httpErr))
}

// TestRestClientRetryContractTimeoutIsOneAttempt pins that a timeout bounds the
// call once - the client does not retry on timeout - and records the two very
// different shapes of the error, because a caller has to know them to react:
// HttpTimeout (http.Client.Timeout) only says "timeout" in text and is not
// comparable to context.DeadlineExceeded, while a deadline carried by the
// request context stays comparable.
func TestRestClientRetryContractTimeoutIsOneAttempt(t *testing.T) {
	t.Run("HttpTimeout is text-only", func(t *testing.T) {
		release, hits := retryContractBlockingServer(t)

		client := NewOdpsRestClient(MockAccount{}, release)
		client.HttpTimeout = 150 * time.Millisecond

		var decoded struct {
			Message string `xml:"Message"`
		}

		started := time.Now()
		err := client.GetWithModel("projects/p", url.Values{}, nil, &decoded)
		elapsed := time.Since(started)

		if !assert.Error(t, err) {
			return
		}
		assert.Less(t, elapsed, 2*time.Second,
			"one attempt at HttpTimeout=150ms; a longer call would mean an implicit retry")
		assert.Equal(t, int32(1), atomic.LoadInt32(hits))
		// The text says "deadline exceeded", but the error is produced by
		// http.Client.Timeout and is NOT comparable to context.DeadlineExceeded:
		// a caller that wants to tell a timeout from another transport failure
		// has to match text, which is why the request-context form below is the
		// one worth using in new code.
		assert.Contains(t, err.Error(), "deadline exceeded")
		assert.False(t, stderrors.Is(err, context.DeadlineExceeded),
			"a Client.Timeout is not comparable to context.DeadlineExceeded, got %#v", err)
	})

	t.Run("request context deadline stays typed", func(t *testing.T) {
		endpoint, hits := retryContractBlockingServer(t)

		client := NewOdpsRestClient(MockAccount{}, endpoint)
		client.HttpTimeout = 0 // no client-level timeout: only the ctx deadline

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/projects/p", nil)
		if !assert.NoError(t, err) {
			return
		}
		_, err = client.Do(req)
		if !assert.Error(t, err) {
			return
		}
		assert.True(t, stderrors.Is(err, context.DeadlineExceeded),
			"a context deadline must be comparable, got %#v", err)
		assert.Equal(t, int32(1), atomic.LoadInt32(hits))
	})
}

// retryContractBlockingServer starts a fake service that accepts requests but
// never answers them, and returns its URL together with a pointer to the number
// of requests it was entered with.
func retryContractBlockingServer(t *testing.T) (string, *int32) {
	t.Helper()

	var hits int32
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		<-release
	}))
	// Cleanups run LIFO: releasing the handler is registered last so it happens
	// before server.Close(), which waits for in-flight handlers.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	return server.URL, &hits
}

// TestRestClientRetryContractErrorBodyReadFailure: the service answered 5xx but
// the connection died mid-body. NewHttpNotOk ignores the read error, so the
// caller keeps the status line and whatever bytes arrived, and loses the parsed
// error code. This is the shape a retry would have to reason about without
// knowing what the service actually said.
func TestRestClientRetryContractErrorBodyReadFailure(t *testing.T) {
	var hits int32

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
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
		// Declare 200 bytes and then hang up with a truncated body.
		_, _ = conn.Write([]byte("HTTP/1.1 503 Service Unavailable\r\n" +
			"Content-Type: application/xml\r\n" +
			"x-odps-request-id: req-trunc\r\n" +
			"Content-Length: 200\r\n" +
			"Connection: close\r\n\r\n" +
			"<Error><Code>ODPS-503</Code>"))
		_ = conn.Close()
	}))
	server.Start()
	t.Cleanup(server.Close)

	client := NewOdpsRestClient(MockAccount{}, server.URL)
	var decoded struct {
		Message string `xml:"Message"`
	}

	err := client.GetWithModel("projects/p", url.Values{}, nil, &decoded)
	if !assert.Error(t, err) {
		return
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&hits), "a truncated error body must not be re-requested")

	var httpErr HttpError
	if !assert.True(t, stderrors.As(err, &httpErr), "got %#v", err) {
		return
	}
	assert.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
	assert.Equal(t, "req-trunc", httpErr.RequestId)
	assert.Contains(t, string(httpErr.Body), "ODPS-503", "the bytes that did arrive are kept")
	assert.Nil(t, httpErr.ErrorMessage, "a truncated document does not parse; only the raw body survives")
	assert.Contains(t, httpErr.Error(), "503 Service Unavailable")
}

// TestRestClientRetryContractSuccessBodyReadFailure: the status said success
// but the payload never arrived whole. The decode error is what the caller
// sees, it is not an HttpError, and nothing re-sends - so a create whose 201
// body is truncated cannot be told apart from "the service did not do it".
func TestRestClientRetryContractSuccessBodyReadFailure(t *testing.T) {
	var hits int32

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
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
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n" +
			"Content-Type: application/xml\r\n" +
			"Content-Length: 200\r\n" +
			"Connection: close\r\n\r\n" +
			"<TestResponse><Message>hel"))
		_ = conn.Close()
	}))
	server.Start()
	t.Cleanup(server.Close)

	client := NewOdpsRestClient(MockAccount{}, server.URL)
	var decoded struct {
		Message string `xml:"Message"`
	}

	err := client.GetWithModel("projects/p", url.Values{}, nil, &decoded)
	if !assert.Error(t, err, "a truncated success body must be reported") {
		return
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&hits))

	var httpErr HttpError
	assert.False(t, stderrors.As(err, &httpErr), "2xx is not an HttpError even when its body is broken")
	assert.True(t, stderrors.Is(err, io.ErrUnexpectedEOF), "the caller must still see why decoding failed, got %#v", err)
}

// TestRestClientRetryContractParseFuncSeesOnly2xx explains why the create
// retry loop keys off the error and not the response: Do short-circuits on any
// non-2xx before the parse func runs, so a 409 handler branch inside a parse
// func is unreachable and the 409 only ever arrives as HttpError.
func TestRestClientRetryContractParseFuncSeesOnly2xx(t *testing.T) {
	t.Run("409 never reaches the parse func", func(t *testing.T) {
		endpoint, hits := retryContractServer(t, replyScript{
			status: http.StatusConflict,
			body:   retryContractErrorBody(http.StatusConflict),
		})

		client := NewOdpsRestClient(MockAccount{}, endpoint)
		var parseCalls int
		err := client.DoWithParseFunc(mustRetryContractRequest(t, client, "GET", "projects/p", nil), func(res *http.Response) error {
			parseCalls++
			return nil
		})

		var httpErr HttpError
		assert.True(t, stderrors.As(err, &httpErr))
		assert.Equal(t, http.StatusConflict, httpErr.StatusCode)
		assert.Zero(t, parseCalls, "the parse func only runs on 2xx")
		assert.Equal(t, int32(1), atomic.LoadInt32(hits))
	})

	t.Run("201 reaches it once", func(t *testing.T) {
		endpoint, hits := retryContractServer(t, replyScript{
			status:  http.StatusCreated,
			body:    "",
			headers: map[string]string{"Location": "http://service/projects/p/instances/i-1"},
		})

		client := NewOdpsRestClient(MockAccount{}, endpoint)
		var seenStatus int
		err := client.DoWithParseFunc(mustRetryContractRequest(t, client, "POST", "projects/p/instances", nil), func(res *http.Response) error {
			seenStatus = res.StatusCode
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, http.StatusCreated, seenStatus)
		assert.Equal(t, int32(1), atomic.LoadInt32(hits))
	})
}

func mustRetryContractRequest(t *testing.T, client RestClient, method, resource string, body io.Reader) *http.Request {
	t.Helper()

	req, err := client.NewRequest(method, resource, body)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return req
}

// TestRestClientRetryContractSignatureIsFreshPerCall guards the assumption the
// retry callers rely on: every Do re-signs with the Date header it just wrote,
// so a request that is sent again is never signed against a stale timestamp.
func TestRestClientRetryContractSignatureIsFreshPerCall(t *testing.T) {
	type seen struct {
		authorization string
		date          string
	}
	var observed []seen

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = append(observed, seen{
			authorization: r.Header.Get("Authorization"),
			date:          r.Header.Get("Date"),
		})
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `<TestResponse><Message>ok</Message></TestResponse>`)
	}))
	t.Cleanup(server.Close)

	client := NewOdpsRestClient(alienMockAccount{}, server.URL)
	var decoded struct {
		Message string `xml:"Message"`
	}

	assert.NoError(t, client.GetWithModel("projects/p", url.Values{}, nil, &decoded))
	assert.NoError(t, client.GetWithModel("projects/p", url.Values{}, nil, &decoded))

	if !assert.Len(t, observed, 2) {
		return
	}
	for i, o := range observed {
		assert.NotEmpty(t, o.date, "call %d must carry a Date", i)
		assert.True(t, strings.HasPrefix(o.authorization, "ODPS "),
			"call %d must be signed, got %q", i, o.authorization)
	}
	if observed[0].date != observed[1].date {
		assert.NotEqual(t, observed[0].authorization, observed[1].authorization,
			"a new Date must produce a new signature, not a cached one")
	}
}

// alienMockAccount signs with the real algorithm so the assertions above look
// at a genuine Authorization header.
type alienMockAccount struct{}

func (alienMockAccount) GetType() account.Provider { return account.Aliyun }

func (alienMockAccount) SignRequest(req *http.Request, endpoint string) error {
	// Reuse the production signer with fixed non-secret values: nothing here is
	// a credential, and the fake service never checks the signature.
	signer := account.NewAliyunAccount("test-access-id", "test-access-key")
	return signer.SignRequest(req, endpoint)
}
