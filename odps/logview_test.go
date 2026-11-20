package odps_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	odpsaccount "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
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
