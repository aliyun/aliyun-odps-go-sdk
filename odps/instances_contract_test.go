// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

func TestInstanceListWirePagination(t *testing.T) {
	var listCalls, cachedCalls atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		q := r.URL.Query()
		switch r.URL.Path {
		case "/api/projects/fixture/instances":
			assert.Equal(t, "yes", q.Get("onlyowner"))
			assert.Equal(t, "Suspended", q.Get("status"))
			assert.Equal(t, "interactive", q.Get("quotaindex"))
			assert.Equal(t, "10:20", q.Get("daterange"))
			if listCalls.Add(1) == 1 {
				assert.Empty(t, q.Get("marker"))
				_, _ = io.WriteString(w, `<Instances><Marker>next</Marker><Instance><Name>i1</Name><Owner>alice</Owner><Status>Suspended</Status><StartTime>Fri, 20 Nov 2026 00:00:00 GMT</StartTime></Instance></Instances>`)
			} else {
				assert.Equal(t, "next", q.Get("marker"))
				_, _ = io.WriteString(w, `<Instances><Instance><Name>i2</Name><Owner>alice</Owner><Status>Suspended</Status><EndTime>Fri, 20 Nov 2026 01:00:00 GMT</EndTime></Instance></Instances>`)
			}
		case "/api/projects/fixture/cachedinstances":
			assert.Equal(t, "no", q.Get("onlyowner"))
			assert.False(t, q.Has("status"))
			switch cachedCalls.Add(1) {
			case 1:
				assert.Empty(t, q.Get("marker"))
				_, _ = io.WriteString(w, `<Instances><Marker>cached-next</Marker><Content>{"id":"i3"}</Content></Instances>`)
			case 2:
				assert.Equal(t, "cached-next", q.Get("marker"))
				_, _ = io.WriteString(w, `<Instances/>`)
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	var listed []*odps.Instance
	require.NoError(t, service.Instances().List(func(instance *odps.Instance) {
		listed = append(listed, instance)
	}, odps.InstanceFilter.OnlyOwner(), odps.InstanceFilter.Status(odps.InstanceSuspended),
		odps.InstanceFilter.QuotaIndex("interactive"), odps.InstanceFilter.TimeRange(time.Unix(10, 0), time.Unix(20, 0))))
	require.Len(t, listed, 2)
	require.Equal(t, "i1", listed[0].Id())
	require.Equal(t, "i2", listed[1].Id())
	require.Equal(t, "alice", listed[0].Owner())
	require.Equal(t, odps.InstanceSuspended, listed[0].Status())
	require.Equal(t, 0, listed[0].StartTime().UTC().Hour())
	require.Equal(t, 1, listed[1].EndTime().UTC().Hour())
	require.Error(t, odps.NewInstances(service, "absent").List(func(*odps.Instance) { t.Fatal("unexpected instance") }))

	queued, err := service.Instances().ListInstancesQueued(odps.InstanceFilter.Status(0))
	require.NoError(t, err)
	require.Equal(t, []string{`{"id":"i3"}`}, queued)
	_, err = service.Instances().ListInstancesQueued()
	require.Error(t, err)
}

func TestOfflineInstanceDiagnostics(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/projects/fixture/instances/diagnostics" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPut {
			var body struct{ Status string }
			assert.NoError(t, xml.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, "Terminated", body.Status)
			w.WriteHeader(http.StatusOK)
			return
		}
		assert.Equal(t, http.MethodGet, r.Method)
		q := r.URL.Query()
		if !q.Has("cached") {
			assert.Equal(t, "fixture_task", q.Get("taskname"))
		}
		switch {
		case q.Has("instanceprogress"):
			_, _ = io.WriteString(w, `<Progress><Stage ID="s1"><Status>RUNNING</Status><InputRecords>15</InputRecords></Stage></Progress>`)
		case q.Has("instancedetail"):
			_, _ = io.WriteString(w, `{"detail":"distributed plan"}`)
		case q.Has("instancesummary"):
			_, _ = io.WriteString(w, `{"Instance":{"JsonSummary":"{}","Summary":"finished"}}`)
		case q.Has("instancequota"):
			_, _ = io.WriteString(w, `{"quota":"batch"}`)
		case q.Has("cached"):
			_, _ = io.WriteString(w, `{"state":"Running"}`)
		default:
			t.Errorf("unexpected query %s", r.URL.RawQuery)
		}
	})
	instance := service.Instance("diagnostics")
	stages, err := instance.GetTaskProgress("fixture_task")
	require.NoError(t, err)
	require.Len(t, stages, 1)
	require.Equal(t, "s1", stages[0].ID)
	require.Equal(t, 15, stages[0].InputRecords)
	detail, err := instance.GetTaskDetail("fixture_task")
	require.NoError(t, err)
	require.JSONEq(t, `{"detail":"distributed plan"}`, string(detail))
	summary, err := instance.GetTaskSummary("fixture_task")
	require.NoError(t, err)
	require.Equal(t, &odps.TaskSummary{JsonSummary: "{}", Summary: "finished"}, summary)
	quota, err := instance.GetTaskQuotaJson("fixture_task")
	require.NoError(t, err)
	require.JSONEq(t, `{"quota":"batch"}`, quota)
	cached, err := instance.GetCachedInfo()
	require.NoError(t, err)
	require.JSONEq(t, `{"state":"Running"}`, cached)
	require.NoError(t, instance.Terminate())

	missing := service.Instance("missing")
	_, err = missing.GetTaskProgress("fixture_task")
	require.Error(t, err)
	_, err = missing.GetTaskDetail("fixture_task")
	require.Error(t, err)
	_, err = missing.GetTaskSummary("fixture_task")
	require.Error(t, err)
	_, err = missing.GetTaskQuotaJson("fixture_task")
	require.Error(t, err)
	_, err = missing.GetCachedInfo()
	require.Error(t, err)
	require.Error(t, missing.Terminate())
}

func TestMaxQASessionAndResultWireContract(t *testing.T) {
	var connections, submissions atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/api/connection/mcqa":
			connections.Add(1)
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "fixture", q.Get("project"))
			assert.Equal(t, "interactive", q.Get("quota"))
			assert.Equal(t, "batch", q.Get("fallbackQuota"))
			assert.Equal(t, "true", q.Get("fallback"))
			_, _ = io.WriteString(w, `{"quotaNickName":"interactive","connInfo":"negotiated","regionId":"cn-fixture"}`)
		case "/api/mcqa/projects/fixture/instances":
			assert.Equal(t, http.MethodPost, r.Method)
			switch submissions.Add(1) {
			case 1:
				assert.True(t, q.Has("tryWait"))
				assert.Equal(t, "negotiated", r.Header.Get(common.HttpHeaderMaxQASessionID))
				assert.JSONEq(t, `{"FallbackQuota":"batch","Fallback":"true"}`, r.Header.Get(common.HttpHeaderODPSFallbackInfos))
				w.Header().Set("Location", "/api/mcqa/projects/fixture/instances/q1")
				w.Header().Set(common.HttpHeaderMaxQAQueryCookie, "cookie-1")
			default:
				assert.False(t, q.Has("tryWait"))
				assert.Equal(t, "reuse", r.Header.Get(common.HttpHeaderMaxQASessionID))
				assert.Empty(t, r.Header.Get(common.HttpHeaderODPSFallbackInfos))
				w.Header().Set("Location", "/api/mcqa/projects/fixture/instances/q2")
			}
			w.WriteHeader(http.StatusCreated)
		case "/api/mcqa/projects/fixture/instances/q1":
			assert.Equal(t, "negotiated", r.Header.Get(common.HttpHeaderMaxQASessionID))
			assert.Equal(t, "cookie-1", r.Header.Get(common.HttpHeaderMaxQAQueryCookie))
			switch {
			case r.Method == http.MethodPut:
				w.WriteHeader(http.StatusOK)
			case q.Has("instanceprogress"):
				_, _ = io.WriteString(w, `<Progress/>`)
			case q.Has("instancedetail"):
				_, _ = io.WriteString(w, `{"query":"interactive"}`)
			default:
				assert.True(t, q.Has("instancestatus"))
				w.Header().Set(common.HttpHeaderMaxQAQueryCookie, "cookie-2")
				_, _ = io.WriteString(w, `<Instance><Status>Terminated</Status><Tasks><Task><Name>fixture_query</Name><Status>SUCCESS</Status><Result Transform="Base64">aWQKNQo=</Result></Task></Tasks></Instance>`)
			}
		case "/api/mcqa/projects/fixture/instances/q2":
			assert.Equal(t, "reuse", r.Header.Get(common.HttpHeaderMaxQASessionID))
			assert.Empty(t, r.Header.Get(common.HttpHeaderMaxQAQueryCookie))
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	withDiscovery := options.NewSQLTaskOptions(options.WithTaskName("fixture_query"), options.WithInstanceOption(
		options.WithTryWait(true), options.WithMaxQAOptions(options.WithMaxQAQuotaName("interactive"), options.WithMaxQAFallback("batch", true)),
	))
	instance, err := service.ExecSQlWithOption("select 5", withDiscovery)
	require.NoError(t, err)
	require.True(t, instance.IsAsync())
	require.Equal(t, "fixture", instance.ProjectName())
	require.True(t, instance.MaxQA.IsMaxQA)
	require.Equal(t, "cookie-1", instance.MaxQA.QueryCookie)
	_, err = instance.GetTaskProgress("fixture_query")
	require.NoError(t, err)
	detail, err := instance.GetTaskDetail("fixture_query")
	require.NoError(t, err)
	require.JSONEq(t, `{"query":"interactive"}`, string(detail))
	require.NoError(t, instance.Terminate())
	results, err := instance.GetResult()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "id\n5\n", results[0].Content())
	require.True(t, instance.IsSync())
	require.Equal(t, "cookie-2", instance.MaxQA.QueryCookie)

	withReuse := options.NewSQLTaskOptions(options.WithInstanceOption(options.WithMaxQAOptions(options.WithMaxQASessionID("reuse"))))
	instance, err = service.ExecSQlWithOption("select 6", withReuse)
	require.NoError(t, err)
	_, err = instance.GetResult()
	require.Error(t, err)
	_, err = instance.GetTaskProgress("fixture_query")
	require.Error(t, err)
	_, err = instance.GetTaskDetail("fixture_query")
	require.Error(t, err)
	require.Error(t, instance.Terminate())
	require.Equal(t, int32(1), connections.Load())
	require.Equal(t, int32(2), submissions.Load())
}

func TestWaitForSuccessReportsTheFailedTask(t *testing.T) {
	const replies = "<Instance><Tasks>" +
		"<Task><Name>setup</Name><Status>SUCCESS</Status><Result>setup complete</Result></Task>" +
		"<Task><Name>compute</Name><Status>FAILED</Status><Result>compute failed</Result></Task>" +
		"</Tasks></Instance>"
	for _, inline := range []bool{false, true} {
		name := "poll"
		if inline {
			name = "inline"
		}
		t.Run(name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && inline:
					w.Header().Set("Location", "/api/projects/fixture/instances/job")
					_, _ = io.WriteString(w, replies)
				case r.Method == http.MethodGet && !inline:
					if r.URL.Query().Has("taskstatus") || r.URL.Query().Has("result") {
						_, _ = io.WriteString(w, replies)
					} else {
						_, _ = io.WriteString(w, "<Instance><Status>Terminated</Status></Instance>")
					}
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})

			if inline {
				instance, err := service.ExecSQLContext(t.Context(), "select 1")
				require.ErrorContains(t, err, "compute failed")
				require.NotNil(t, instance)
				require.Equal(t, "job", instance.Id())
				require.ErrorContains(t, instance.WaitForSuccessContext(t.Context()), "compute failed")
				return
			}
			require.ErrorContains(t, service.Instance("job").WaitForSuccessContext(t.Context()), "compute failed")
		})
	}
}

func TestWaitForSuccessRejectsUnknownTaskStatus(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("taskstatus") {
			_, _ = io.WriteString(w, "<Instance><Tasks><Task><Name>query</Name><Status>UNRECOGNIZED</Status></Task></Tasks></Instance>")
			return
		}
		_, _ = io.WriteString(w, "<Instance><Status>Terminated</Status></Instance>")
	})
	err := service.Instance("job").WaitForSuccessContext(t.Context())
	require.ErrorContains(t, err, "query")
	require.ErrorContains(t, err, "TASK_STATUS_UNKNOWN")
}

func TestMaxQAWaitUsesQueryStatus(t *testing.T) {
	var polls atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "active", r.Header.Get(common.HttpHeaderMaxQASessionID))
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/mcqa/projects/fixture/instances":
			w.Header().Set("Location", r.URL.Path+"/job")
			w.Header().Set(common.HttpHeaderMaxQAQueryCookie, "request-cookie")
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet && r.URL.Path == "/api/mcqa/projects/fixture/instances/job":
			assert.Equal(t, "request-cookie", r.Header.Get(common.HttpHeaderMaxQAQueryCookie))
			assert.True(t, r.URL.Query().Has("instancestatus"))
			if polls.Add(1) == 1 {
				_, _ = io.WriteString(w, "<Instance><Status>Running</Status></Instance>")
				return
			}
			_, _ = io.WriteString(w, "<Instance><Status>Terminated</Status><Tasks><Task><Name>query</Name><Status>SUCCESS</Status></Task></Tasks></Instance>")
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	opts := options.NewSQLTaskOptions(options.WithInstanceOption(options.WithMaxQAOptions(options.WithMaxQASessionID("active"))))
	instance, err := service.ExecSQlWithOption("select 1", opts)
	require.NoError(t, err)
	require.NoError(t, instance.WaitForSuccessContext(t.Context()))
	require.EqualValues(t, 2, polls.Load())
}
