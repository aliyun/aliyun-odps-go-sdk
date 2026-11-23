// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	odpsaccount "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

type metadataSigner struct {
	want bool
	t    *testing.T
	odpsaccount.Account
}

func (s metadataSigner) SignRequest(r *http.Request, endpoint string) error {
	if s.want {
		assert.Equal(s.t, "catalog-check", r.Header.Get("x-odps-call-context"))
		assert.Equal(s.t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", r.Header.Get("traceparent"))
		assert.Contains(s.t, r.Header.Get("x-odps-user-agent"), "integration")
	}
	return s.Account.SignRequest(r, endpoint)
}

func newV1Client(t *testing.T, handler http.HandlerFunc) *odps.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := odps.NewClient(odpsaccount.NewAliyunAccount("test-ak", "test-sk"), odps.ClientOptions{
		Endpoint: server.URL + "/api", Project: "alpha", Schema: "analytics",
	})
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestScopedClientCatalogFetchAndPagination(t *testing.T) {
	var lists, gets atomic.Int32
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "beta", q.Get("curr_project"))
		assert.Equal(t, "reporting", q.Get("curr_schema"))
		switch r.URL.Path {
		case "/api/projects/beta/tables":
			lists.Add(1)
			assert.Equal(t, "2", q.Get("maxitems"))
			assert.Equal(t, "a", q.Get("name"))
			switch q.Get("marker") {
			case "":
				fmt.Fprint(w, "<Tables><Table><Name>alpha_view</Name><Type>VIRTUAL_VIEW</Type></Table><Marker>page-2</Marker></Tables>")
			case "page-2":
				fmt.Fprint(w, "<Tables><Table><Name>absent_type</Name></Table><Marker>page-2</Marker></Tables>")
			}
		case "/api/projects/beta/tables/alpha_view":
			gets.Add(1)
			if q.Has("extended") {
				fmt.Fprint(w, "<Table><Schema>{\"fileNum\":17}</Schema></Table>")
				return
			}
			fmt.Fprint(w, "<Table><Type>VIRTUAL_VIEW</Type><Schema>{\"columns\":[{\"name\":\"id\",\"type\":\"BIGINT\"}]}</Schema></Table>")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	scoped, err := client.WithScope(odps.Scope{Project: "beta", Schema: "reporting"})
	require.NoError(t, err)
	require.Equal(t, odps.Scope{Project: "alpha", Schema: "analytics"}, client.Scope())
	list := odps.TableListOptions{PageOptions: odps.PageOptions{PageSize: 2}, NamePrefix: "a"}
	for item, err := range scoped.Tables().List(t.Context(), list) {
		require.NoError(t, err)
		require.Equal(t, odps.VirtualView, item.Type)
		require.Equal(t, odps.TableRef{Project: "beta", Schema: "reporting", Name: "alpha_view"}, item.Ref)
		break
	}
	require.EqualValues(t, 1, lists.Load())
	page, err := scoped.Tables().ListPage(t.Context(), list)
	require.NoError(t, err)
	require.Equal(t, "page-2", page.NextPageToken)
	var kinds []odps.TableType
	var listErr error
	for item, err := range scoped.Tables().List(t.Context(), list) {
		if err != nil {
			listErr = err
			break
		}
		kinds = append(kinds, item.Type)
	}
	require.ErrorContains(t, listErr, "repeated page token")
	require.Equal(t, []odps.TableType{odps.VirtualView, odps.TableTypeUnknown}, kinds)
	_, err = scoped.Tables().ListPage(t.Context(), odps.TableListOptions{PageOptions: odps.PageOptions{PageSize: -1}})
	require.ErrorContains(t, err, "page size")
	_, err = scoped.Tables().Get(t.Context(), odps.TableRef{Name: ".."})
	require.ErrorContains(t, err, "table name")
	require.Zero(t, gets.Load())

	ref := odps.TableRef{Project: "beta", Schema: "reporting", Name: "alpha_view"}
	table, err := client.Tables().Get(t.Context(), ref)
	require.NoError(t, err)
	require.Equal(t, ref, table.Ref)
	require.Equal(t, "id", table.Schema.Columns[0].Name)
	require.EqualValues(t, 1, gets.Load())
	extended, err := scoped.Tables().GetExtended(t.Context(), ref)
	require.NoError(t, err)
	require.Equal(t, 17, extended.Schema.FileNum)
	require.Equal(t, "id", extended.Schema.Columns[0].Name)
	require.EqualValues(t, 3, gets.Load())
}

func TestClientCatalogUsesScopedMetadataLoaders(t *testing.T) {
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/projects":
			fmt.Fprint(w, "<Projects><Project><Name>alpha</Name></Project><Marker>more</Marker></Projects>")
		case "/api/projects/alpha":
			fmt.Fprint(w, "<Project><Name>alpha</Name><Region>cn</Region></Project>")
		case "/api/projects/alpha/schemas":
			fmt.Fprint(w, "<Schemas><Schema><Name>analytics</Name><Owner>alice</Owner></Schema></Schemas>")
		case "/api/projects/alpha/schemas/analytics":
			fmt.Fprint(w, "<Schema><Name>analytics</Name><Owner>alice</Owner></Schema>")
		case "/api/projects/alpha/resources":
			assert.Equal(t, "analytics", r.URL.Query().Get("curr_schema"))
			fmt.Fprint(w, "<Resources><Resource><Name>example.txt</Name><ResourceType>file</ResourceType></Resource></Resources>")
		case "/api/projects/alpha/resources/example.txt":
			assert.Equal(t, "analytics", r.URL.Query().Get("curr_schema"))
			assert.True(t, r.URL.Query().Has("meta"))
			w.Header().Set("x-odps-resource-type", "file")
		case "/api/projects/alpha/registration/functions/example":
			assert.Equal(t, "analytics", r.URL.Query().Get("curr_schema"))
			fmt.Fprint(w, "<Function><Alias>example</Alias><Owner>alice</Owner></Function>")
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	ctx := t.Context()
	project, err := client.Projects().Get(ctx, "")
	require.NoError(t, err)
	require.Equal(t, "cn", project.Region)
	for item, err := range client.Projects().List(ctx, odps.ProjectListOptions{Group: "team", User: "alice"}) {
		require.NoError(t, err)
		require.Equal(t, "alpha", item.Name)
		break
	}
	schema, err := client.Schemas().Get(ctx, "")
	require.NoError(t, err)
	require.Equal(t, "alice", schema.Owner)
	for item, err := range client.Schemas().List(ctx, odps.SchemaListOptions{}) {
		require.NoError(t, err)
		require.Equal(t, "analytics", item.Name)
	}
	resource, err := client.Resources().Get(ctx, odps.ResourceRef{Name: "example.txt"})
	require.NoError(t, err)
	require.Equal(t, odps.ResourceTypeFile, resource.Type)
	for item, err := range client.Resources().List(ctx, odps.ResourceListOptions{}) {
		require.NoError(t, err)
		require.Equal(t, resource.Ref, item.Ref)
	}
	function, err := client.Functions().Get(ctx, odps.FunctionRef{Name: "example"})
	require.NoError(t, err)
	require.Equal(t, "analytics", function.Ref.Schema)
	require.Equal(t, "alice", function.Owner)
}

func TestClientSQLJobLifecycle(t *testing.T) {
	var submitted, cancelled atomic.Int32
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "alpha", q.Get("curr_project"))
		switch r.Method {
		case http.MethodPost:
			task := readSubmittedSQL(t, r)
			assert.Equal(t, "select 1", task.Query)
			assert.JSONEq(t, "{\"odps.default.schema\":\"analytics\"}", task.Properties.Get("settings"))
			w.Header().Set("Location", "/projects/alpha/instances/job-1")
			if submitted.Add(1) == 2 {
				fmt.Fprint(w, "<incomplete>")
				return
			}
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			switch {
			case q.Has("taskstatus"):
				fmt.Fprint(w, "<Instance><Tasks><Task><Name>example</Name><Status>SUCCESS</Status></Task></Tasks></Instance>")
			case q.Has("result"):
				fmt.Fprint(w, "<Instance><Tasks><Task><Name>example</Name><Result>1</Result></Task></Tasks></Instance>")
			default:
				fmt.Fprint(w, "<Instance><Status>Terminated</Status></Instance>")
			}
		case http.MethodPut:
			cancelled.Add(1)
		}
	})
	ctx := t.Context()
	req := odps.SQLRequest{Statement: "select 1", Options: options.SQLTaskOptions{TaskName: "example"}}
	job, err := client.SubmitSQL(ctx, req)
	require.NoError(t, err)
	require.Equal(t, odps.SQLJobRef{Project: "alpha", InstanceID: "job-1", TaskName: "example"}, job.Ref())
	resumed, err := client.ResumeSQL(job.Ref())
	require.NoError(t, err)
	status, err := resumed.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, odps.InstanceTerminated, status)
	result, err := resumed.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, job.Ref(), result.Ref())
	inline, err := result.Inline(ctx)
	require.NoError(t, err)
	require.Len(t, inline, 1)
	require.Equal(t, "1", inline[0].Content())

	done, cancel := context.WithCancel(ctx)
	cancel()
	_, err = job.Wait(done)
	require.ErrorIs(t, err, context.Canceled)
	var jobErr *odps.SQLJobError
	require.ErrorAs(t, err, &jobErr)
	require.Equal(t, job.Ref(), jobErr.Job)
	require.Zero(t, cancelled.Load())
	require.NoError(t, job.Cancel(ctx))
	require.EqualValues(t, 1, cancelled.Load())

	accepted, err := client.SubmitSQL(ctx, req)
	require.Error(t, err)
	require.NotNil(t, accepted)
	require.Equal(t, job.Ref(), accepted.Ref())
}

func TestClientChecksOptionsAndSignsRequestMetadata(t *testing.T) {
	credentials := odpsaccount.NewAliyunAccount("test-ak", "test-sk")
	for _, opts := range []odps.ClientOptions{
		{Project: "p", Endpoint: "://bad"},
		{Project: "..", Endpoint: "https://example.test"},
		{Project: "p", Schema: "bad/schema", Endpoint: "https://example.test"},
		{Project: "p", HTTPTimeout: -1, Endpoint: "https://example.test"},
	} {
		_, err := odps.NewClient(credentials, opts)
		require.Error(t, err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "catalog-check", r.Header.Get("x-odps-call-context"))
		assert.Equal(t, "tenant=sample", r.Header.Get("tracestate"))
		fmt.Fprint(w, "<Project><Name>alpha</Name></Project>")
	}))
	t.Cleanup(server.Close)
	client, err := odps.NewClient(metadataSigner{want: true, t: t, Account: credentials},
		odps.ClientOptions{Project: "alpha", Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	ctx := odps.WithRequestMetadata(t.Context(), odps.RequestMetadata{
		CallContext: "catalog-check", UserAgentSuffix: "integration",
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", TraceState: "tenant=sample",
	})
	require.NoError(t, client.Ping(ctx))
}
