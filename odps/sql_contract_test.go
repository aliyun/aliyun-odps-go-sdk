// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/xml"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

type submittedSQL struct {
	Name       string
	Properties common.Properties `xml:"Config>Property"`
	Query      string
}

func readSubmittedSQL(t *testing.T, r *http.Request) submittedSQL {
	t.Helper()
	var req struct {
		SQL submittedSQL `xml:"Job>Tasks>SQL"`
	}
	assert.NoError(t, xml.NewDecoder(r.Body).Decode(&req))
	return req.SQL
}

func TestSQLSubmissionSchemaPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		options  *options.SQLTaskOptions
		settings string
	}{
		{"nil options", nil, `{"odps.default.schema":"client_schema"}`},
		{"client schema", &options.SQLTaskOptions{}, `{"odps.default.schema":"client_schema"}`},
		{"option schema", &options.SQLTaskOptions{DefaultSchema: "call_schema"}, `{"odps.default.schema":"call_schema"}`},
		{"explicit hint", &options.SQLTaskOptions{
			DefaultSchema: "call_schema", Hints: map[string]string{"odps.default.schema": "hint_schema"},
		}, `{"odps.default.schema":"hint_schema"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/projects/fixture/instances", r.URL.Path)
				task := readSubmittedSQL(t, r)
				assert.Equal(t, "AnonymousSQLTask", task.Name)
				assert.Equal(t, "sql", task.Properties.Get("type"))
				assert.JSONEq(t, tc.settings, task.Properties.Get("settings"))
				w.Header().Set("Location", "/projects/fixture/instances/submitted")
				w.WriteHeader(http.StatusCreated)
			})
			service.SetCurrentSchemaName("client_schema")
			require.NotPanics(t, func() {
				instance, err := service.ExecSQlWithOption("select 1", tc.options)
				require.NoError(t, err)
				require.Equal(t, "submitted", instance.Id())
			})
		})
	}
}

func TestSQLSubmissionOptionsCanBeShared(t *testing.T) {
	shared := &options.SQLTaskOptions{
		TaskName: "custom_task",
		Hints:    map[string]string{"custom.setting": "on"},
		Aliases:  map[string]string{"old.jar": "current.jar"},
	}
	newClient := func(schema string) *odps.Odps {
		service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
			task := readSubmittedSQL(t, r)
			assert.Equal(t, "custom_task", task.Name)
			assert.JSONEq(t, `{"custom.setting":"on","odps.default.schema":"`+schema+`"}`, task.Properties.Get("settings"))
			assert.JSONEq(t, `{"old.jar":"current.jar"}`, task.Properties.Get("aliases"))
			w.Header().Set("Location", "/projects/fixture/instances/submitted")
			w.WriteHeader(http.StatusCreated)
		})
		service.SetCurrentSchemaName(schema)
		return service
	}
	first, second := newClient("first"), newClient("second")
	var requests sync.WaitGroup
	for range 8 {
		for _, client := range []*odps.Odps{first, second} {
			requests.Go(func() {
				_, err := client.ExecSQlWithOption("select 1", shared)
				assert.NoError(t, err)
			})
		}
	}
	requests.Wait()
	require.Empty(t, shared.DefaultSchema)
	require.Equal(t, map[string]string{"custom.setting": "on"}, shared.Hints)

	task := odps.NewSQLTaskWithOptions("select 1", nil)
	require.Equal(t, "AnonymousSQLTask", task.GetName())
	first.SetDefaultProjectName("")
	_, err := first.ExecSQl("select 1")
	require.ErrorContains(t, err, "default project")
	_, err = first.ExecSQlWithOption("select 1", shared)
	require.ErrorContains(t, err, "default project")
}

func TestSQLTaskCSVMatchesTaskAndReportsInvalidResults(t *testing.T) {
	matching := `<Task><Name>chosen</Name><Status>SUCCESS</Status><Result><![CDATA[value
7
]]></Result></Task>`
	unrelated := `<Task><Name>other</Name><Result><![CDATA[wrong
9
]]></Result></Task>`
	for _, tc := range []struct {
		name, tasks, wantError string
		header                 bool
		wantRows               [][]string
	}{
		{name: "matching task has priority", tasks: unrelated + matching, wantRows: [][]string{{"7"}}},
		{name: "include column names", tasks: matching, header: true, wantRows: [][]string{{"value"}, {"7"}}},
		{name: "legacy single task", tasks: unrelated, wantRows: [][]string{{"9"}}},
		{name: "missing task on multi-task instance", tasks: unrelated + `<Task><Name>another</Name></Task>`, wantError: "chosen"},
		{name: "no tasks", wantError: "failed to get result"},
		{name: "empty result", tasks: `<Task><Name>chosen</Name><Result/></Task>`},
		{name: "malformed header", tasks: `<Task><Name>chosen</Name><Result>"unterminated</Result></Task>`, wantError: "parse error"},
		{name: "failed task", tasks: unrelated + `<Task><Name>chosen</Name><Status>FAILED</Status><Result>ODPS-0130131: table not found</Result></Task>`, wantError: "ODPS-0130131"},
		{name: "malformed response", tasks: `<Task>`, wantError: "XML"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.True(t, r.URL.Query().Has("result"))
				_, _ = io.WriteString(w, "<Instance><Tasks>"+tc.tasks+"</Tasks></Instance>")
			})
			task := odps.NewSqlTask("chosen", "select 7", nil)
			reader, err := task.GetSelectResultAsCsv(service.Instance("fixture"), tc.header)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			rows, err := reader.ReadAll()
			require.NoError(t, err)
			require.Equal(t, tc.wantRows, rows)
		})
	}
}

func TestSQLTaskCSVHonorsResultFormatAndEncoding(t *testing.T) {
	for _, test := range []struct {
		name, result, wantError string
		wantRows                [][]string
	}{
		{
			name: "declared CSV",
			result: `<Result Format="CSV"><![CDATA[value,other
"has,comma","say ""hello"""
]]></Result>`,
			wantRows: [][]string{{"value", "other"}, {"has,comma", `say "hello"`}},
		},
		{
			name:     "wrapped CSV",
			result:   `<Result Format="csv" Transform="Base64">` + base64.StdEncoding.EncodeToString([]byte("value\nhello\n")) + `</Result>`,
			wantRows: [][]string{{"value"}, {"hello"}},
		},
		{
			name: "a text result is not treated as malformed CSV",
			result: `<Result Format="text"><![CDATA[value
one,two
]]></Result>`,
			wantError: "not CSV",
		},
		{
			name:      "bad base64 is not silently truncated",
			result:    `<Result Format="csv" Transform="Base64">` + base64.StdEncoding.EncodeToString([]byte("value\nhello\n")) + `!</Result>`,
			wantError: "decode Base64",
		},
		{
			name:      "unknown encoding",
			result:    `<Result Format="csv" Transform="unknown">value</Result>`,
			wantError: "unsupported transform",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := metadataFixture(t, func(writer http.ResponseWriter, request *http.Request) {
				assert.True(t, request.URL.Query().Has("result"))
				_, _ = io.WriteString(writer, "<Instance><Tasks><Task><Name>fixture</Name><Status>SUCCESS</Status>"+
					test.result+"</Task></Tasks></Instance>")
			})
			task := odps.NewSqlTask("fixture", "select 1", nil)
			reader, err := task.GetSelectResultAsCsv(service.Instance("fixture"), true)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			rows, err := reader.ReadAll()
			require.NoError(t, err)
			require.Equal(t, test.wantRows, rows)
		})
	}
}

func TestSQLTaskCSVKeepsFieldCountValidation(t *testing.T) {
	service := metadataFixture(t, func(writer http.ResponseWriter, request *http.Request) {
		assert.True(t, request.URL.Query().Has("result"))
		_, _ = io.WriteString(writer, `<Instance><Tasks><Task><Name>fixture</Name><Status>SUCCESS</Status><Result Format="csv"><![CDATA[a,b
1,2,3
]]></Result></Task></Tasks></Instance>`)
	})
	task := odps.NewSqlTask("fixture", "select 1", nil)
	reader, err := task.GetSelectResultAsCsv(service.Instance("fixture"), true)
	require.NoError(t, err)
	_, err = reader.ReadAll()
	require.ErrorIs(t, err, csv.ErrFieldCount)
}

func TestTableDDLKeepsHintsAndAliasesSeparate(t *testing.T) {
	for _, namespace := range []string{"", "analytics"} {
		for _, external := range []bool{false, true} {
			kind, taskName := "table", "SQLCreateTableTask"
			if external {
				kind, taskName = "external table", "SQLCreateExternalTableTask"
			}
			t.Run(namespace+"/"+kind, func(t *testing.T) {
				hints := map[string]string{"custom.setting": "on"}
				aliases := map[string]string{"old.jar": "current.jar"}
				service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
					switch r.Method {
					case http.MethodPost:
						task := readSubmittedSQL(t, r)
						assert.Equal(t, taskName, task.Name)
						assert.Contains(t, task.Query, "create "+kind)
						settings := `{"custom.setting":"on","odps.namespace.schema":"false"}`
						if namespace != "" {
							settings = `{"custom.setting":"on","odps.namespace.schema":"true"}`
							assert.Contains(t, task.Query, "`analytics`.")
						}
						assert.JSONEq(t, settings, task.Properties.Get("settings"))
						assert.JSONEq(t, `{"old.jar":"current.jar"}`, task.Properties.Get("aliases"))
						w.Header().Set("Location", "/projects/fixture/instances/ddl")
						w.WriteHeader(http.StatusCreated)
					case http.MethodGet:
						if !r.URL.Query().Has("taskstatus") {
							assert.Empty(t, r.URL.Query().Get("taskname"))
							_, _ = io.WriteString(w, "<Instance><Status>Terminated</Status></Instance>")
							return
						}
						_, _ = io.WriteString(w, "<Instance><Tasks><Task><Name>"+taskName+"</Name><Status>SUCCESS</Status></Task></Tasks></Instance>")
					default:
						w.WriteHeader(http.StatusMethodNotAllowed)
					}
				})
				service.SetCurrentSchemaName(namespace)
				builder := tableschema.NewSchemaBuilder().Name("example").Column(tableschema.Column{Name: "id", Type: datatype.BigIntType})
				if external {
					schema := builder.StorageHandler("fixture.Handler").Location("oss://fixture/data").Build()
					require.NoError(t, service.Tables().CreateExternal(schema, false, nil, nil, hints, aliases))
				} else {
					require.NoError(t, service.Tables().Create(builder.Build(), false, hints, aliases))
				}
				require.Equal(t, map[string]string{"custom.setting": "on"}, hints)
			})
		}
	}
}
