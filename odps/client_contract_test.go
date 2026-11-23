// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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
			assert.JSONEq(t, "{\"odps.default.schema\":\"analytics\",\"odps.namespace.schema\":\"true\"}", task.Properties.Get("settings"))
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

func TestCatalogPropagatesScopedValidationAndServiceErrors(t *testing.T) {
	var requests atomic.Int32
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch {
		case r.URL.Path == "/api/projects/alpha/tables" && r.URL.Query().Get("marker") == "":
			assert.Equal(t, "alice", r.URL.Query().Get("owner"))
			assert.Equal(t, "VIRTUAL_VIEW", r.URL.Query().Get("type"))
			assert.Empty(t, r.URL.Query().Get("curr_schema"))
			fmt.Fprint(w, "<Tables><Table><Name>first</Name></Table><Table><Name>second</Name></Table><Marker>next</Marker></Tables>")
		case r.URL.Path == "/api/projects/alpha/tables/view" && !r.URL.Query().Has("extended"):
			fmt.Fprint(w, "<Table><Schema>{\"columns\":[{\"name\":\"id\",\"type\":\"bigint\"}]}</Schema></Table>")
		case r.URL.Path == "/api/projects/beta":
			fmt.Fprint(w, "<Project/>")
		default:
			http.Error(w, "fixture rejected the request", http.StatusForbidden)
		}
	})
	_, err := client.WithScope(odps.Scope{Project: "bad/project"})
	require.ErrorContains(t, err, "project name")
	rootScope, err := client.WithScope(odps.Scope{})
	require.NoError(t, err)
	require.Equal(t, odps.Scope{Project: "alpha"}, rootScope.Scope())

	ctx := t.Context()
	badType := odps.TableTypeUnknown
	_, err = rootScope.Tables().ListPage(ctx, odps.TableListOptions{Type: &badType})
	require.ErrorContains(t, err, "table type")
	_, err = client.Tables().Get(ctx, odps.TableRef{Project: "..", Name: "view"})
	require.ErrorContains(t, err, "project name")
	_, err = client.Projects().Get(ctx, "..")
	require.ErrorContains(t, err, "project name")
	_, err = client.Schemas().Get(ctx, "bad/schema")
	require.ErrorContains(t, err, "schema name")
	_, err = client.Resources().Get(ctx, odps.ResourceRef{Name: ".."})
	require.ErrorContains(t, err, "resource name")
	_, err = client.Functions().Get(ctx, odps.FunctionRef{Name: ".."})
	require.ErrorContains(t, err, "function name")
	require.Zero(t, requests.Load())

	viewType := odps.VirtualView
	opts := odps.TableListOptions{Owner: "alice", Type: &viewType}
	for _, test := range []struct {
		name    string
		cancel  bool
		already bool
		want    string
	}{
		{name: "second page rejection", want: "fixture rejected"},
		{name: "cancel between rows", cancel: true, want: context.Canceled.Error()},
		{name: "cancel before fetching", cancel: true, already: true, want: context.Canceled.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if test.already {
				cancel()
			}
			var got []string
			var seqErr error
			for entry, err := range rootScope.Tables().List(ctx, opts) {
				if err != nil {
					seqErr = err
					break
				}
				got = append(got, entry.Ref.Name)
				if test.cancel {
					cancel()
				}
			}
			require.ErrorContains(t, seqErr, test.want)
			switch {
			case test.already:
				require.Empty(t, got)
			case test.cancel:
				require.Equal(t, []string{"first"}, got)
			default:
				require.Equal(t, []string{"first", "second"}, got)
			}
		})
	}

	_, err = client.Tables().Get(ctx, odps.TableRef{Name: "missing"})
	require.ErrorContains(t, err, "fixture rejected")
	extended, err := client.Tables().GetExtended(ctx, odps.TableRef{Name: "view"})
	require.ErrorContains(t, err, "fixture rejected")
	require.Empty(t, extended.Schema.Columns)
	meta, err := client.Projects().Get(ctx, "beta")
	require.NoError(t, err)
	require.Equal(t, "beta", meta.Name)
	_, err = client.Projects().Get(ctx, "denied")
	require.ErrorContains(t, err, "fixture rejected")
	_, err = client.Schemas().Get(ctx, "denied")
	require.ErrorContains(t, err, "fixture rejected")
	_, err = client.Resources().Get(ctx, odps.ResourceRef{Name: "denied"})
	require.ErrorContains(t, err, "fixture rejected")
	_, err = client.Functions().Get(ctx, odps.FunctionRef{Name: "denied"})
	require.ErrorContains(t, err, "fixture rejected")

	badPage := odps.PageOptions{PageSize: -1}
	_, err = client.Projects().ListPage(ctx, odps.ProjectListOptions{PageOptions: badPage})
	require.ErrorContains(t, err, "page size")
	_, err = client.Schemas().ListPage(ctx, odps.SchemaListOptions{PageOptions: badPage})
	require.ErrorContains(t, err, "page size")
	_, err = client.Resources().ListPage(ctx, odps.ResourceListOptions{PageOptions: badPage})
	require.ErrorContains(t, err, "page size")
	_, err = client.Projects().ListPage(ctx, odps.ProjectListOptions{})
	require.ErrorContains(t, err, "fixture rejected")
	_, err = client.Schemas().ListPage(ctx, odps.SchemaListOptions{})
	require.ErrorContains(t, err, "fixture rejected")
	_, err = rootScope.Resources().ListPage(ctx, odps.ResourceListOptions{})
	require.ErrorContains(t, err, "fixture rejected")
}

func TestSQLJobErrorsPreserveIdentityAndContext(t *testing.T) {
	var requests atomic.Int32
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method == http.MethodPost && r.URL.Query().Has("tryWait") {
			w.Header().Set("Location", "/projects/alpha/instances/ready")
			fmt.Fprint(w, "<Instance><Tasks><Task><Name>example</Name><Status>SUCCESS</Status><Result>ready</Result></Task></Tasks></Instance>")
			return
		}
		http.Error(w, "fixture rejected the request", http.StatusForbidden)
	})
	ctx := t.Context()
	done, cancel := context.WithCancel(ctx)
	cancel()
	_, err := client.SubmitSQL(done, odps.SQLRequest{Statement: "select 1"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = client.SubmitSQL(ctx, odps.SQLRequest{Statement: " "})
	require.ErrorContains(t, err, "statement is required")
	for _, invalid := range []odps.SQLJobRef{
		{Project: "..", InstanceID: "id"}, {InstanceID: "bad/id"},
	} {
		_, err = client.ResumeSQL(invalid)
		require.Error(t, err)
	}
	require.Zero(t, requests.Load())
	missing, err := client.SubmitSQL(ctx, odps.SQLRequest{Statement: "select 1"})
	require.ErrorContains(t, err, "fixture rejected")
	require.Nil(t, missing)

	normal, err := client.ResumeSQL(odps.SQLJobRef{InstanceID: "job"})
	require.NoError(t, err)
	_, err = normal.Status(done)
	require.ErrorIs(t, err, context.Canceled)
	_, err = normal.Status(ctx)
	require.ErrorContains(t, err, "fixture rejected")
	_, err = normal.Wait(ctx)
	var known *odps.SQLJobError
	require.ErrorAs(t, err, &known)
	require.Equal(t, "job", known.Job.InstanceID)

	job, err := client.SubmitSQL(ctx, odps.SQLRequest{
		Statement: "select 1",
		Options: options.SQLTaskOptions{
			DefaultSchema: "analytics", Hints: map[string]string{"odps.namespace.schema": "true"},
			InstanceOption: options.NewCreateInstanceOptions(options.WithTryWait(true)),
		},
	})
	require.NoError(t, err)
	status, err := job.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, odps.InstanceTerminated, status)
	result, err := job.Wait(ctx)
	require.NoError(t, err)
	_, err = result.Inline(done)
	require.ErrorIs(t, err, context.Canceled)
	tasks, err := result.Inline(ctx)
	require.NoError(t, err)
	require.Equal(t, "ready", tasks[0].Content())

	_, err = client.TunnelEndpoint(ctx, "bad/project", "")
	require.ErrorContains(t, err, "project name")
	_, err = client.TunnelEndpoint(ctx, "", "fixture-quota")
	require.ErrorContains(t, err, "fixture rejected")
}

func TestInstanceCatalogListsJobsWithTypedFilters(t *testing.T) {
	var requests atomic.Int32
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, "/api/projects/alpha/instances", r.URL.Path)
		q := r.URL.Query()
		switch q.Get("marker") {
		case "":
			fmt.Fprint(w, "<Instances><Instance><Name>first</Name><Owner>alice</Owner><Status>Running</Status></Instance><Marker>next</Marker></Instances>")
		case "next":
			assert.Equal(t, "1", q.Get("maxitems"))
			assert.Equal(t, "yes", q.Get("onlyowner"))
			assert.Equal(t, "Running", q.Get("status"))
			assert.Equal(t, "shared", q.Get("quotaindex"))
			assert.Equal(t, "0:60", q.Get("daterange"))
			fmt.Fprint(w, "<Instances><Instance><Name>second</Name><Status>Suspended</Status></Instance></Instances>")
		default:
			http.Error(w, "fixture page missing", http.StatusNotFound)
		}
	})
	instances := client.Instances()
	ctx := t.Context()
	_, err := instances.ListPage(ctx, odps.InstanceListOptions{PageOptions: odps.PageOptions{PageSize: -1}})
	require.ErrorContains(t, err, "page size")
	_, err = instances.ListPage(ctx, odps.InstanceListOptions{Status: odps.InstanceStatusUnknown})
	require.ErrorContains(t, err, "status filter")
	_, err = instances.ListPage(ctx, odps.InstanceListOptions{StartTime: time.Unix(0, 0)})
	require.ErrorContains(t, err, "start and end time")
	require.Zero(t, requests.Load())
	for item, err := range instances.List(ctx, odps.InstanceListOptions{}) {
		require.NoError(t, err)
		require.Equal(t, odps.SQLJobRef{Project: "alpha", InstanceID: "first"}, item.Ref)
		require.Equal(t, "alice", item.Owner)
		break
	}
	require.EqualValues(t, 1, requests.Load())

	opts := odps.InstanceListOptions{
		PageOptions: odps.PageOptions{PageSize: 1, PageToken: "next"},
		OnlyOwner:   true, Status: odps.InstanceRunning, QuotaIndex: "shared",
		StartTime: time.Unix(0, 0), EndTime: time.Unix(60, 0),
	}
	page, err := instances.ListPage(ctx, opts)
	require.NoError(t, err)
	require.Empty(t, page.NextPageToken)
	require.Equal(t, "second", page.Items[0].Ref.InstanceID)
	require.Equal(t, odps.InstanceSuspended, page.Items[0].Status)
	opts.PageToken = "absent"
	_, err = instances.ListPage(ctx, opts)
	require.ErrorContains(t, err, "fixture page missing")
}

func TestSQLJobRejectsTerminatedInstanceWithoutTaskStatus(t *testing.T) {
	client := newV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/projects/alpha/instances/ready?ignored=true")
			w.WriteHeader(http.StatusCreated)
			return
		}
		assert.Equal(t, "/api/projects/alpha/instances/ready", r.URL.Path)
		if r.URL.Query().Has("taskstatus") {
			fmt.Fprint(w, "<Instance><Tasks/></Instance>")
			return
		}
		fmt.Fprint(w, "<Instance><Status>Terminated</Status></Instance>")
	})
	job, err := client.SubmitSQL(t.Context(), odps.SQLRequest{Statement: "select 1;"})
	require.NoError(t, err)
	require.Equal(t, "ready", job.Ref().InstanceID)
	recovered, err := client.ResumeSQL(job.Ref())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = recovered.Wait(ctx)
	require.ErrorContains(t, err, "terminated without task status")
	var jobErr *odps.SQLJobError
	require.ErrorAs(t, err, &jobErr)
	require.Equal(t, job.Ref(), jobErr.Job)
}
