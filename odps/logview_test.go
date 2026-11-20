package odps_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	odpsaccount "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogViewEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name      string
		version   options.LogViewVersion
		discovery string
		override  string
		host      string
		path      string
		legacy    bool
	}{
		{"auto job insight", options.Auto, "https://insight.example.test", "", "insight.example.test", "/cn-shanghai/job-insights", false},
		{"auto legacy fallback", options.Auto, "", "", "logview.alibaba-inc.com", "/logview", true},
		{"legacy custom host", options.LegacyLogView, "", "https://mock.example.test", "mock.example.test", "/logview", true},
		{"job insight default", options.JobInsight, "", "", "maxcompute.console.aliyun.com", "/cn-shanghai/job-insights", false},
		{"job insight custom host", options.JobInsight, "", "https://mock.example.test", "mock.example.test", "/cn-shanghai/job-insights", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/webconsole/host"):
					fmt.Fprint(w, tc.discovery)
				case strings.HasSuffix(r.URL.Path, "/authorization"):
					fmt.Fprint(w, "<Authorization><Result>test-token</Result></Authorization>")
				case strings.HasSuffix(r.URL.Path, "/logview/host"):
					w.WriteHeader(http.StatusNotFound)
				default:
					t.Errorf("unexpected route: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			client := odps.NewOdps(odpsaccount.NewAliyunAccount("test-ak", "test-sk", "cn-shanghai"), server.URL+"/api")
			client.SetDefaultProjectName("test_project")
			client.Options.LogViewVersion = tc.version
			instance := odps.NewInstance(client, "test_project", "instance")
			logview := odps.NewLogView(client)
			if tc.override != "" {
				if tc.legacy {
					logview.SetLogViewHost(tc.override)
				} else {
					logview.SetJobInsightHost(tc.override)
				}
			}
			require.Equal(t, "https://"+tc.host, logview.LogViewHost())
			result, err := logview.GenerateLogView(instance, 24)
			require.NoError(t, err)
			u, err := url.Parse(result)
			require.NoError(t, err)
			require.Equal(t, tc.host, u.Host)
			require.Equal(t, tc.path, u.Path)
			require.Equal(t, server.URL+"/api", u.Query().Get("h"))
			require.Equal(t, "test_project", u.Query().Get("p"))
			require.Equal(t, "instance", u.Query().Get("i"))
			if tc.legacy {
				require.Equal(t, "test-token", u.Query().Get("token"))
			}
		})
	}
}

func TestGenerateLogViewV2(t *testing.T) {
	client := odps.NewOdps(odpsaccount.NewAliyunAccount("test-ak", "test-sk", "cn-shanghai"), "https://example.test/api")
	client.Options.LogViewVersion = options.JobInsight
	instance := odps.NewInstance(client, "test_project", "instance")
	result, err := odps.NewLogView(client).GenerateLogViewV2(instance, "cn-shanghai")
	require.NoError(t, err)
	require.Equal(t, "https://maxcompute.console.aliyun.com/cn-shanghai/job-insights?h=https://example.test/api&p=test_project&i=instance", result)
}

func TestLogViewConcurrentDiscovery(t *testing.T) {
	var hosts, regions atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/webconsole/host":
			hosts.Add(1)
			fmt.Fprint(w, "https://insight.example.test")
		case "/api/projects/fixture":
			regions.Add(1)
			fmt.Fprint(w, "<Project><Name>fixture</Name><Region>cn-fixture</Region></Project>")
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	service.Options.LogViewVersion = options.JobInsight
	instance := service.Instance("shared")
	logview := service.LogView()
	var requests sync.WaitGroup
	for range 16 {
		requests.Go(func() {
			result, err := logview.GenerateLogView(instance, 1)
			assert.NoError(t, err)
			assert.Contains(t, result, "https://insight.example.test/cn-fixture/job-insights")
		})
	}
	requests.Wait()
	require.Positive(t, hosts.Load())
	require.Positive(t, regions.Load())
	require.Equal(t, "cn", service.Options.RegionId)
	previousHosts, previousRegions := hosts.Load(), regions.Load()
	require.Equal(t, "https://insight.example.test", logview.LogViewHost())
	require.Equal(t, "cn-fixture", service.RegionId())
	require.Equal(t, previousHosts, hosts.Load())
	require.Equal(t, previousRegions, regions.Load())
}

func TestJobInsightTrimsHostAndUsesInstanceRegion(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/webconsole/host":
			fmt.Fprint(w, " https://insight.example.test\n")
		case "/api/projects/fixture":
			fmt.Fprint(w, "<Project><Region>cn-fixture</Region></Project>")
		case "/api/projects/alternate":
			fmt.Fprint(w, "<Project><Region>cn-alternate</Region></Project>")
		case "/api/projects/incomplete":
			fmt.Fprint(w, "<Project/>")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	service.Options.LogViewVersion = options.JobInsight
	require.Equal(t, "cn-fixture", service.RegionId())

	instance := odps.NewInstance(service, "alternate", "instance")
	logview := service.LogView()
	result, err := logview.GenerateLogView(instance, 1)
	require.NoError(t, err)
	require.Contains(t, result, "/cn-alternate/job-insights?")
	require.Contains(t, result, "&p=alternate")
	service.SetDefaultProjectName("alternate")
	require.Equal(t, "cn-alternate", service.RegionId())

	service.SetDefaultProjectName("incomplete")
	require.Equal(t, "cn", service.RegionId())
	service.SetDefaultProjectName("unavailable")
	require.Equal(t, "cn", service.RegionId())
}

func TestLogViewReturnsDiscoveryAndTokenErrors(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/webconsole/host", "/api/logview/host":
			fmt.Fprint(w, "https://%invalid")
		case "/api/projects/fixture/authorization":
			fmt.Fprint(w, "<Authorization><Result>test-token</Result></Authorization>")
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	})
	for _, version := range []options.LogViewVersion{options.JobInsight, options.LegacyLogView} {
		service.Options.LogViewVersion = version
		_, err := service.LogView().GenerateLogView(service.Instance("instance"), 1)
		require.ErrorContains(t, err, "URL escape")
	}

	service.Options.LogViewVersion = options.LegacyLogView
	instance := odps.NewInstance(service, "unavailable", "instance")
	_, err := service.LogView().GenerateLogView(instance, 1)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = service.GenerateLogViewContext(ctx, instance, 1)
	require.ErrorIs(t, err, context.Canceled)

	service.Options.LogViewVersion = 3
	logview := service.LogView()
	require.Equal(t, "Unknown LogView Version 3", logview.LogViewHost())
	_, err = logview.GenerateLogView(instance, 1)
	require.ErrorContains(t, err, "Unknown LogView Version 3")
}
