// Licensed under the Apache License, Version 2.0.

package sqldriver

import (
	"context"
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestUnsupportedDatabaseSQLOperations(t *testing.T) {
	f := newFakeService(t)
	db, err := sql.Open("odps", f.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginTx(t.Context(), nil)
	require.ErrorIs(t, err, ErrTransactionsUnsupported)
	require.Nil(t, tx)
	stmt, err := db.PrepareContext(t.Context(), "select 1")
	require.ErrorIs(t, err, ErrPreparedStatementsUnsupported)
	require.Nil(t, stmt)
	require.Zero(t, f.count(&f.submits))
}

func TestRejectConflictingTunnelOptionsBeforeSubmission(t *testing.T) {
	f := newFakeService(t)
	cfg := f.config()
	cfg.TunnelQuotaName = "test-quota"
	_, err := sql.Open("odps", cfg.FormatDsn())
	require.ErrorContains(t, err, "cannot be configured both")
	require.Zero(t, f.count(&f.submits))
}

func TestDatabaseSQLUsesNamedQuotaForResultTransfer(t *testing.T) {
	const quota = "test-quota"
	var route, create, read atomic.Int32
	var tunnelHost atomic.Value
	f := newFakeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) bool {
		query := r.URL.Query()
		switch {
		case strings.HasSuffix(r.URL.Path, "/tunnel"):
			route.Add(1)
			assert.Equal(t, quota, query.Get("quotaName"))
			_, _ = io.WriteString(w, tunnelHost.Load().(string))
			return true
		case query.Has("downloads"):
			create.Add(1)
			assert.Equal(t, quota, query.Get("quotaName"))
		case query.Has("data"):
			read.Add(1)
			assert.Equal(t, quota, query.Get("quotaName"))
		}
		return false
	})
	endpoint, err := url.Parse(f.tunnel.URL)
	require.NoError(t, err)
	tunnelHost.Store(endpoint.Host)
	f.mu.Lock()
	f.succeedAfterPolls = 1
	f.mu.Unlock()
	cfg := f.config()
	cfg.TunnelEndpoint = ""
	cfg.TunnelQuotaName = quota
	db, err := sql.Open("odps", cfg.FormatDsn())
	require.NoError(t, err)
	defer db.Close()
	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "select 1").Scan(&value))
	require.Equal(t, "row-0", value)
	require.EqualValues(t, 1, route.Load())
	require.EqualValues(t, 1, create.Load())
	require.EqualValues(t, 1, read.Load())
}

func TestDatabaseSQLCancelsOutstandingRequests(t *testing.T) {
	const instance = "/instances/" + fakeInstanceId
	const project = "/projects/" + fakeProjectName

	for _, tc := range []struct {
		name, method, path, param string
		exec, deadline            bool
		responseStatus            int
		instanceKnown             bool
		configure                 func(*odps.Config)
	}{
		{name: "query submit deadline", method: http.MethodPost, path: "/instances", deadline: true},
		{name: "exec submit", method: http.MethodPost, path: "/instances", exec: true},
		{name: "submit with instance ID", method: http.MethodPost, path: "/instances", deadline: true, responseStatus: http.StatusOK, instanceKnown: true},
		{name: "incomplete error response", method: http.MethodPost, path: "/instances", deadline: true, responseStatus: http.StatusServiceUnavailable},
		{name: "instance status", method: http.MethodGet, path: instance, instanceKnown: true},
		{name: "task status", method: http.MethodGet, path: instance, param: "taskstatus", instanceKnown: true},
		{name: "failed task result", method: http.MethodGet, path: instance, param: "result", instanceKnown: true},
		{name: "Tunnel route", method: http.MethodGet, path: project + "/tunnel", param: "quotaName", instanceKnown: true,
			configure: func(cfg *odps.Config) {
				cfg.TunnelEndpoint = ""
				cfg.TunnelQuotaName = "test-quota"
			}},
		{name: "LogView host lookup", method: http.MethodGet, path: "/webconsole/host", instanceKnown: true,
			configure: enableLogView},
		{name: "LogView token", method: http.MethodPost, path: project + "/authorization", param: "sign_bearer_token", instanceKnown: true,
			configure: enableLogView},
		{name: "LogView region", method: http.MethodGet, path: project, instanceKnown: true,
			configure: enableLogView},
		{name: "Tunnel session", method: http.MethodPost, path: instance, instanceKnown: true},
		{name: "result HTTP headers", method: http.MethodGet, path: instance, param: "data", instanceKnown: true},
		{name: "compressed result header", method: http.MethodGet, path: instance, param: "data", responseStatus: http.StatusOK, instanceKnown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			requestCanceled := make(chan struct{})
			var requests atomic.Int32
			f := newFakeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) bool {
				params := r.URL.Query()
				if tc.param == "result" && params.Has("taskstatus") {
					writeXml(w, `<Instance><Tasks><Task Type="SQL"><Name>task</Name><Status>Failed</Status></Task></Tasks></Instance>`)
					return true
				}
				if tc.path == project && r.URL.Path == "/webconsole/host" {
					_, _ = io.WriteString(w, "https://job-insights.invalid")
					return true
				}
				if r.Method != tc.method || !strings.HasSuffix(r.URL.Path, tc.path) ||
					(tc.param != "" && !params.Has(tc.param)) {
					return false
				}
				_, _ = io.Copy(io.Discard, r.Body)
				if requests.Add(1) != 1 {
					http.Error(w, "unexpected repeated request", http.StatusBadRequest)
					return true
				}
				if tc.responseStatus != 0 {
					if tc.path == "/instances" && tc.instanceKnown {
						w.Header().Set("Location", r.URL.Path+"/"+fakeInstanceId)
					}
					if tc.param == "data" {
						w.Header().Set("Content-Encoding", "deflate")
					}
					w.WriteHeader(tc.responseStatus)
					w.(http.Flusher).Flush()
				}
				close(started)
				select {
				case <-r.Context().Done():
					close(requestCanceled)
				case <-t.Context().Done():
				}
				return true
			})
			f.mu.Lock()
			f.succeedAfterPolls = 1
			f.mu.Unlock()
			cfg := f.config()
			cfg.HttpTimeout = 0
			if tc.configure != nil {
				tc.configure(cfg)
			}
			db, err := sql.Open("odps", cfg.FormatDsn())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			var ctx context.Context
			var cancel context.CancelFunc
			var expectedError error
			if tc.deadline {
				ctx, cancel = context.WithTimeout(t.Context(), 600*time.Millisecond)
				expectedError = context.DeadlineExceeded
			} else {
				ctx, cancel = context.WithCancel(t.Context())
				expectedError = context.Canceled
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if tc.exec {
					_, err := db.ExecContext(ctx, "select 1")
					done <- err
					return
				}
				rows, err := db.QueryContext(ctx, "select 1")
				if rows != nil {
					_ = rows.Close()
				}
				done <- err
			}()

			select {
			case <-started:
			case err := <-done:
				t.Fatalf("database/sql returned before the request reached the handler: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("the request did not reach the handler")
			}
			if !tc.deadline {
				cancel()
			}
			select {
			case err = <-done:
				require.ErrorIs(t, err, expectedError)
				var interrupted *CanceledError
				if tc.instanceKnown {
					require.ErrorAs(t, err, &interrupted)
					require.Equal(t, fakeInstanceId, interrupted.InstanceID)
				} else {
					require.False(t, strings.Contains(err.Error(), fakeInstanceId))
				}
			case <-time.After(3 * time.Second):
				t.Fatal("database/sql did not return after cancellation")
			}
			select {
			case <-requestCanceled:
			case <-time.After(3 * time.Second):
				t.Fatal("the server did not observe the HTTP request being canceled")
			}
			require.EqualValues(t, 1, requests.Load())
			require.Zero(t, f.count(&f.terminates))
		})
	}
}

func enableLogView(cfg *odps.Config) {
	cfg.Others = map[string]string{"enableLogview": "true"}
}

func TestDatabaseSQLDeadlineLimitsRetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		match  func(*http.Request) bool
	}{
		{"submission conflict", http.StatusConflict, func(r *http.Request) bool {
			return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/instances")
		}},
		{"Tunnel read failure", http.StatusServiceUnavailable, func(r *http.Request) bool {
			return r.URL.Query().Has("data")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			f := newFakeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) bool {
				if !tc.match(r) {
					return false
				}
				attempts.Add(1)
				w.Header().Set("Retry-After", "60")
				http.Error(w, fmt.Sprintf("fixture %s", tc.name), tc.status)
				return true
			})
			f.mu.Lock()
			f.succeedAfterPolls = 1
			f.mu.Unlock()
			db, err := sql.Open("odps", f.dsn())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			_, err = callWithin(t, 2*time.Second, func() error {
				rows, err := db.QueryContext(ctx, "select 1")
				if rows != nil {
					_ = rows.Close()
				}
				return err
			})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.EqualValues(t, 1, attempts.Load())
		})
	}
}

func TestDatabaseSQLBindsBeforeSubmissionAndPingsProject(t *testing.T) {
	var submits, pings atomic.Int32
	f := newFakeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/instances"):
			submits.Add(1)
			var model struct {
				Job struct {
					Tasks struct {
						SQL struct {
							Query string
						}
					}
				}
			}
			if err := xml.NewDecoder(r.Body).Decode(&model); err != nil {
				t.Error(err)
			}
			if query := model.Job.Tasks.SQL.Query; !strings.Contains(query, "'?', 'O\\'Reilly\\\\北京'") {
				t.Errorf("unexpected submitted SQL: %q", query)
			}
			return false
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/projects/"+fakeProjectName):
			if pings.Add(1) == 1 {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
			} else {
				_, _ = io.WriteString(w, "<Project><Name>"+fakeProjectName+"</Name></Project>")
			}
			return true
		}
		return false
	})
	f.succeedAfterPolls = 1
	db, err := sql.Open("odps", f.dsn())
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(t.Context(), "select '?', ?;", "O'Reilly\\北京")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "select ?;")
	require.Error(t, err)
	require.EqualValues(t, 1, submits.Load())

	require.ErrorContains(t, db.PingContext(t.Context()), "denied")
	require.NoError(t, db.PingContext(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, db.PingContext(ctx), context.Canceled)
	require.EqualValues(t, 2, pings.Load())
}
