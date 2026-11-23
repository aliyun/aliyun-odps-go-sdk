// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestProjectsExistsDistinguishesMissingAndUnavailableProjects(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/projects/fixture":
			fmt.Fprint(w, "<Project><Name>fixture</Name></Project>")
		case "/api/projects/absent":
			w.WriteHeader(http.StatusNotFound)
		case "/api/projects/malformed":
			fmt.Fprint(w, "<Project")
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	})
	exists, err := service.Projects().Exists("fixture")
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = service.Projects().Exists("absent")
	require.NoError(t, err)
	require.False(t, exists)
	for _, name := range []string{"forbidden", "malformed"} {
		exists, err = service.Projects().Exists(name)
		require.Error(t, err)
		require.False(t, exists)
	}
}

func TestTableExistsReadsBasicMetadata(t *testing.T) {
	var extendedRequests atomic.Int32
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/projects/fixture/tables/visible":
			if r.URL.Query().Has("extended") {
				extendedRequests.Add(1)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, `<Table><Name>visible</Name><Schema>{"comment":"correct","owner":"alice","columns":[{"name":"id","type":"BIGINT"}]}</Schema></Table>`)
		case "/api/projects/fixture/tables/malformed":
			fmt.Fprint(w, `<Table><Name>wrong</Name><Schema>{"columns":false}</Schema></Table>`)
		case "/api/projects/fixture/tables/denied":
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	table := service.Table("visible")
	exists, err := table.Exists()
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, table.IsLoaded())
	require.False(t, table.IsLoadedExtended())
	require.Equal(t, "fixture", table.ProjectName())
	require.Equal(t, "correct", table.Comment())
	require.Equal(t, "alice", table.Owner())
	require.Zero(t, extendedRequests.Load())
	require.Error(t, table.LoadExtendedInfo())
	require.EqualValues(t, 1, extendedRequests.Load())

	table = service.Table("malformed")
	exists, err = table.Exists()
	require.Error(t, err)
	require.False(t, exists)
	require.False(t, table.IsLoaded())
	require.Equal(t, "malformed", table.Name())

	exists, err = service.Table("missing").Exists()
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = service.Table("denied").Exists()
	require.Error(t, err)
	require.False(t, exists)
}

func TestCatalogDDLUsesTargetProjectAndQuotesColumnNames(t *testing.T) {
	submitted := make(chan submittedSQL, 3)
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			assert.Equal(t, "/api/projects/target/instances", r.URL.Path)
			task := readSubmittedSQL(t, r)
			assert.JSONEq(t, `{"odps.namespace.schema":"true"}`, task.Properties.Get("settings"))
			submitted <- task
			w.Header().Set("Location", "/projects/target/instances/ddl")
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			assert.Equal(t, "/api/projects/target/instances/ddl", r.URL.Path)
			if r.URL.Query().Has("taskstatus") {
				fmt.Fprint(w, "<Instance><Tasks><Task><Status>SUCCESS</Status></Task></Tasks></Instance>")
			} else {
				fmt.Fprint(w, "<Instance><Status>Terminated</Status></Instance>")
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	target := odps.NewSchema(service, "target", "named")
	require.NoError(t, target.ChangeComment("changed"))
	table := target.Tables().Get("summary")
	require.NoError(t, table.ChangeColumnName("order", "sale value"))
	require.NoError(t, table.AlterColumnType("sale value", datatype.BigIntType))
	close(submitted)
	var queries []string
	for task := range submitted {
		queries = append(queries, task.Query)
	}
	require.Equal(t, []string{
		"alter schema `target`.`named` set comment 'changed';",
		"alter table target.`named`.`summary` change column `order` rename to `sale value`;",
		"alter table target.`named`.`summary` change column `sale value` `sale value` BIGINT;",
	}, queries)
}

func TestProjectTunnelDiscoveryValidatesResponse(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, r.URL.Query().Has("service"))
		switch r.URL.Query().Get("quotaName") {
		case "batch":
			fmt.Fprint(w, " tunnel.example.test:8443/api\n")
		case "invalid":
			fmt.Fprint(w, "%invalid")
		case "empty":
			fmt.Fprint(w, "  ")
		case "truncated":
			w.Header().Set("Content-Length", "30")
			fmt.Fprint(w, "short")
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	project := service.DefaultProject()
	endpoint, err := project.GetTunnelEndpoint("batch")
	require.NoError(t, err)
	require.Equal(t, "http://tunnel.example.test:8443/api", endpoint)
	for _, quota := range []string{"invalid", "empty", "truncated", ""} {
		endpoint, err = project.GetTunnelEndpoint(quota)
		require.Error(t, err)
		require.Empty(t, endpoint)
		if quota == "invalid" || quota == "empty" {
			require.ErrorContains(t, err, "invalid Tunnel endpoint")
		}
	}
}

func TestTableListKeepsTypeAndLoadCommitsAtomically(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/projects/fixture/tables":
			require.Equal(t, "analytics", r.URL.Query().Get("curr_schema"))
			fmt.Fprint(w, "<Tables><Table><Name>visible</Name><Owner>alice</Owner><Type>VIRTUAL_VIEW</Type></Table></Tables>")
		case "/api/projects/fixture/tables/visible":
			if r.URL.Query().Has("extended") {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			fmt.Fprint(w, "<Table><Schema>{\"columns\":[{\"name\":\"i\",\"type\":\"bigint\"}]}</Schema></Table>")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	tables := odps.NewTables(service, "fixture", "analytics")
	var view *odps.Table
	tables.List(func(table *odps.Table, err error) {
		require.NoError(t, err)
		view = table
	})
	require.NotNil(t, view)
	require.Equal(t, odps.VirtualView, view.Type())
	require.Equal(t, "alice", view.Owner())
	require.Equal(t, "analytics", view.SchemaName())
	require.Error(t, view.Load())
	require.False(t, view.IsLoaded())
	require.Equal(t, odps.VirtualView, view.Type())
	require.Empty(t, view.Schema().Columns)
}
