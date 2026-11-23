// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

func metadataFixture(t *testing.T, handler http.HandlerFunc) *odps.Odps {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	service := odps.NewOdps(account2.NewAliyunAccount("test-ak", "test-sk"), server.URL+"/api")
	service.SetDefaultProjectName("fixture")
	return service
}

func TestFunctionExistsReturnsMetadataErrors(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "<Function><Alias>incomplete")
	})
	fn, err := odps.NewFunctions(service).Get("broken")
	require.NoError(t, err)
	exists, err := fn.Exist()
	require.Error(t, err)
	require.False(t, exists)
}

func TestFunctionMutationClosesResponse(t *testing.T) {
	done := make(chan string, 3)
	release := make(chan struct{})
	defer close(release)
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			done <- r.Method
		case <-release:
		}
	})
	functions := odps.NewFunctions(service)
	fn, err := functions.Get("fixture")
	require.NoError(t, err)
	for _, tc := range []struct {
		method string
		call   func() error
	}{
		{http.MethodPost, func() error { return functions.Create("", "", *fn) }},
		{http.MethodPut, func() error { return functions.Update("", "", *fn) }},
		{http.MethodDelete, func() error { return functions.Delete(fn.Name()) }},
	} {
		t.Run(tc.method, func(t *testing.T) {
			require.NoError(t, tc.call())
			select {
			case method := <-done:
				require.Equal(t, tc.method, method)
			case <-time.After(2 * time.Second):
				t.Fatal("client left the response body open")
			}
		})
	}
}

func TestProjectMetadataCaching(t *testing.T) {
	var propertyRequests, clusterRequests, extendedRequests, updates atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/projects/fixture", r.URL.Path)
		if r.Method == http.MethodPut {
			var update struct {
				Name       string
				Properties []common.Property `xml:"Properties>Property"`
			}
			assert.NoError(t, xml.NewDecoder(r.Body).Decode(&update))
			assert.Equal(t, "fixture", update.Name)
			assert.Equal(t, []common.Property{{Name: "fixture.setting", Value: "value"}}, update.Properties)
			updates.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		assert.Equal(t, http.MethodGet, r.Method)
		q := r.URL.Query()
		switch {
		case q.Get("properties") == "all":
			propertyRequests.Add(1)
			_, _ = io.WriteString(w, `<Project><Properties><Property><Name>flag</Name><Value>true</Value></Property></Properties></Project>`)
		case q.Get("isGroupApi") == "true":
			clusterRequests.Add(1)
			_, _ = io.WriteString(w, `<Project><DefaultCluster>c1</DefaultCluster><Clusters><Name>c1</Name><QuotaId>quota-1</QuotaId></Clusters></Project>`)
		case q.Has("extended"):
			extendedRequests.Add(1)
			_, _ = io.WriteString(w, `<Project><ExtendedProperties><Property><Name>label</Name><Value>demo</Value></Property></ExtendedProperties></Project>`)
		default:
			w.Header().Set("x-odps-owner", "owner")
			w.Header().Set("x-odps-creation-time", "Fri, 20 Nov 2026 00:00:00 GMT")
			w.Header().Set("Last-Modified", "Fri, 20 Nov 2026 01:00:00 GMT")
			_, _ = io.WriteString(w, `<Project><Name>fixture</Name><Type>managed</Type><State>Available</State><Region>cn-test</Region><TenantId>123</TenantId><Comment>metadata</Comment></Project>`)
		}
	})
	project := service.DefaultProject()
	require.NoError(t, project.Load())
	require.True(t, project.Existed())
	require.EqualValues(t, odps.ProjectStatusAvailable, project.Status())
	require.Equal(t, "owner", project.Owner())
	require.Equal(t, "123", project.TenantId())
	require.Equal(t, "cn-test", project.RegionId())
	require.Equal(t, "metadata", project.Comment())
	require.True(t, project.LastModifiedTime().After(project.CreationTime()))
	for range 2 {
		props, err := project.GetAllProperties()
		require.NoError(t, err)
		require.Equal(t, common.Properties{{Name: "flag", Value: "true"}}, props)
		extended, err := project.GetExtendedProperties()
		require.NoError(t, err)
		require.Equal(t, common.Properties{{Name: "label", Value: "demo"}}, extended)
		cluster, err := project.GetDefaultCluster()
		require.NoError(t, err)
		require.Equal(t, "c1", cluster)
		clusters, err := project.GetClusters()
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		require.Equal(t, "quota-1", clusters[0].QuotaId)
	}
	require.NoError(t, project.Update(map[string]string{"fixture.setting": "value"}))
	require.EqualValues(t, 1, propertyRequests.Load())
	require.EqualValues(t, 1, extendedRequests.Load())
	require.EqualValues(t, 1, clusterRequests.Load())
	require.EqualValues(t, 1, updates.Load())

	fresh := service.DefaultProject()
	clusters, err := fresh.GetClusters()
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.EqualValues(t, 2, clusterRequests.Load())
}

func TestProjectListingPagination(t *testing.T) {
	for _, failOnSecondPage := range []bool{false, true} {
		t.Run(fmt.Sprint(failOnSecondPage), func(t *testing.T) {
			var requests atomic.Int32
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				assert.Equal(t, "/api/projects", r.URL.Path)
				q := r.URL.Query()
				assert.Equal(t, "fixture_", q.Get("name"))
				assert.Equal(t, "group-a", q.Get("group"))
				if q.Get("marker") == "" {
					_, _ = io.WriteString(w, `<Projects><Marker>next</Marker><Project><Name>fixture_a</Name><State>ReadOnly</State></Project></Projects>`)
					return
				}
				assert.Equal(t, "next", q.Get("marker"))
				if failOnSecondPage {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `<Error><Code>ServiceUnavailable</Code><Message>listing unavailable</Message></Error>`)
					return
				}
				_, _ = io.WriteString(w, `<Projects><Project><Name>fixture_b</Name><State>Frozen</State></Project></Projects>`)
			})
			projects, err := service.Projects().List(odps.ProjectFilter.NamePrefix("fixture_"), odps.ProjectFilter.Group("group-a"))
			if failOnSecondPage {
				requireEmulatorServiceError(t, err, http.StatusServiceUnavailable, "ServiceUnavailable")
				require.Len(t, projects, 1)
			} else {
				require.NoError(t, err)
				require.Len(t, projects, 2)
				require.Equal(t, "fixture_b", projects[1].Name())
				require.EqualValues(t, odps.ProjectStatusFrozen, projects[1].Status())
			}
			require.Equal(t, "fixture_a", projects[0].Name())
			require.EqualValues(t, odps.ProjectStatusReadOnly, projects[0].Status())
			require.EqualValues(t, 2, requests.Load())
		})
	}
}

func TestBatchLoadTablesMetadata(t *testing.T) {
	const table = `<Table><Name>sample</Name><Project>fixture</Project><SchemaName>analytics</SchemaName><Schema>{"Columns":[{"name":"id","type":"bigint"}]}</Schema></Table>`
	for _, tc := range []struct {
		name, xml, want string
	}{
		{"valid schema", "<Tables>" + table + "</Tables>", ""},
		{"schema is not JSON", `<Tables><Table><Name>sample</Name><Schema>invalid</Schema></Table></Tables>`, "invalid character"},
		{"metadata is not XML", "<Tables><Table>", "unexpected EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/projects/fixture/tables", r.URL.Path)
				assert.Equal(t, "analytics", r.URL.Query().Get("curr_schema"))
				assert.True(t, r.URL.Query().Has("query"))
				var request struct {
					Tables []struct{ Name, Schema, Project string } `xml:"Table"`
				}
				assert.NoError(t, xml.NewDecoder(r.Body).Decode(&request))
				if assert.Len(t, request.Tables, 1) {
					assert.Equal(t, "sample", request.Tables[0].Name)
					assert.Equal(t, "analytics", request.Tables[0].Schema)
					assert.Equal(t, "fixture", request.Tables[0].Project)
				}
				_, _ = io.WriteString(w, tc.xml)
			})
			tables := odps.NewTables(service, "fixture", "analytics")
			loaded, err := tables.BatchLoadTables([]string{"sample"})
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				require.Nil(t, loaded)
				return
			}
			require.NoError(t, err)
			require.Len(t, loaded, 1)
			require.True(t, loaded[0].IsLoaded())
			require.False(t, loaded[0].IsLoadedExtended())
			require.Equal(t, "sample", loaded[0].Name())
			require.Equal(t, "analytics", loaded[0].SchemaName())
			require.Equal(t, "BIGINT", loaded[0].Schema().Columns[0].Type.Name())
		})
	}
}

func TestProjectLoadInvalidatesDeletedProject(t *testing.T) {
	var deleted atomic.Bool
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if deleted.Load() {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchProject</Code><Message>project removed</Message></Error>`)
			return
		}
		_, _ = io.WriteString(w, `<Project><Name>fixture</Name><State>Available</State></Project>`)
	})
	project := service.DefaultProject()
	require.NoError(t, project.Load())
	require.True(t, project.Existed())
	deleted.Store(true)
	var response restclient.HttpError
	require.ErrorAs(t, project.Load(), &response)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.False(t, project.Existed())
}

func TestSchemaExistsRejectsInvalidMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		created string
		updated string
		body    string
		err     string
	}{
		{name: "missing creation header", updated: time.Unix(60, 0).UTC().Format(http.TimeFormat), body: `<Schema><Name>reporting</Name></Schema>`},
		{name: "invalid XML", body: `<Schema>`, err: "unexpected EOF"},
		{name: "invalid creation date", created: "bad-date", body: `<Schema/>`, err: "schema creation time"},
		{name: "invalid modification date", updated: "bad-date", body: `<Schema/>`, err: "schema last modified time"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/api/projects/fixture/schemas/reporting", r.URL.Path)
				w.Header().Set("x-odps-creation-time", test.created)
				w.Header().Set("Last-Modified", test.updated)
				_, _ = io.WriteString(w, test.body)
			})
			schema := service.Schema("reporting")
			exists, err := schema.Exists()
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
				require.False(t, exists)
				require.False(t, schema.IsLoaded())
				return
			}
			require.NoError(t, err)
			require.True(t, exists)
			require.True(t, schema.IsLoaded())
			require.True(t, schema.CreateTime().IsZero())
			require.Equal(t, int64(60), schema.ModifiedTime().Unix())
		})
	}
}
