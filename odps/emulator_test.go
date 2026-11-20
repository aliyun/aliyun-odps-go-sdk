// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func emulatorService(t *testing.T) *odps.Odps {
	t.Helper()
	endpoint := os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("MAXCOMPUTE_EMULATOR_ENDPOINT is unset")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback(), "emulator must run on loopback")
	service := odps.NewOdps(account2.NewAliyunAccount("test-ak", "test-sk"), endpoint+"/api")
	service.SetDefaultProjectName("test_project")
	service.SetCurrentSchemaName("default")
	service.SetHttpTimeout(5 * time.Second)
	return service
}

func TestEmulatorProjectTableAndPartition(t *testing.T) {
	service := emulatorService(t)
	project := service.DefaultProject()
	require.NoError(t, project.Load())
	require.Equal(t, "test_project", project.Name())
	tunnel, err := project.GetTunnelEndpointContext(t.Context())
	require.NoError(t, err)
	require.Equal(t, os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT"), tunnel)

	name := fmt.Sprintf("go_sdk_metadata_%d", time.Now().UnixNano())
	tables := service.Tables()
	table := tables.Get(name)
	exists, err := table.Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.False(t, table.IsLoaded(), "a missing table has no loaded metadata")

	schema := tableschema.NewSchemaBuilder().Name(name).
		Column(tableschema.Column{Name: "id", Type: datatype.BigIntType}).
		PartitionColumn(tableschema.Column{Name: "ds", Type: datatype.StringType}).Build()
	require.NoError(t, tables.Create(schema, false, nil, nil))
	t.Cleanup(func() { require.NoError(t, tables.Delete(name, true)) })
	table = tables.Get(name)
	require.NoError(t, table.Load())
	require.True(t, table.IsLoaded())
	require.Equal(t, "BIGINT", table.Schema().Columns[0].Type.Name())
	require.Equal(t, "ds", table.PartitionColumns()[0].Name)
	require.NoError(t, table.LoadExtendedInfo())
	require.True(t, table.IsLoadedExtended())
	require.NoError(t, table.AddPartition(true, "ds=20261119"))
	values, err := table.GetPartitionValues()
	require.NoError(t, err)
	require.Len(t, values, 1)
	partition, err := table.GetPartition("ds=20261119")
	require.NoError(t, err)
	require.NoError(t, partition.Load())
	_, err = table.GetPartition("ds=missing")
	require.Error(t, err)

	var seen []string
	tables.List(func(table *odps.Table, err error) {
		require.NoError(t, err)
		seen = append(seen, table.Name())
	}, nil, odps.TableFilter.NamePrefix(name))
	require.Equal(t, []string{name}, seen)
	require.NoError(t, table.DeletePartition(true, "ds=20261119"))
	values, err = table.GetPartitionValues()
	require.NoError(t, err)
	require.Empty(t, values)
}

func requireEmulatorServiceError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var response restclient.HttpError
	require.ErrorAs(t, err, &response)
	require.Equal(t, status, response.StatusCode)
	require.NotNil(t, response.ErrorMessage)
	require.Equal(t, code, response.ErrorMessage.ErrorCode)
}

func TestEmulatorFunctionMetadataLifecycle(t *testing.T) {
	service := emulatorService(t)
	project := service.DefaultProjectName()
	suffix := time.Now().UnixNano()
	schema := fmt.Sprintf("go_sdk_function_schema_%d", suffix)
	require.NoError(t, service.Schemas().Create(schema, false, "Go SDK function fixtures"))
	t.Cleanup(func() { require.NoError(t, service.Schemas().Delete(schema)) })
	service.SetCurrentSchemaName(schema)
	resourceName := fmt.Sprintf("go_sdk_function_%d.jar", suffix)
	resources := odps.NewResources(service)
	// The emulator stores the dependency as an opaque file.
	dependency := odps.NewFileResource(resourceName)
	dependency.SetResourceType(odps.ResourceTypeJar)
	dependency.SetReader(strings.NewReader("SDK metadata fixture"))
	resources.ChunkSize = 32
	require.NoError(t, resources.CreateFileResource(project, schema, dependency, false))
	t.Cleanup(func() { require.NoError(t, resources.Delete(resourceName)) })

	functions := odps.NewFunctions(service)
	require.Equal(t, project, functions.ProjectName())
	require.Equal(t, schema, functions.SchemaName())
	_, err := functions.Get("")
	require.Error(t, err)
	require.Error(t, functions.Create("", "", odps.Function{}))
	require.Error(t, functions.Update("", "", odps.Function{}))
	require.Error(t, functions.Delete(""))

	name := fmt.Sprintf("go_sdk_function_%d", suffix)
	function, err := functions.Get(name)
	require.NoError(t, err)
	require.Equal(t, project, function.Project())
	exists, err := function.Exist()
	require.NoError(t, err)
	require.False(t, exists)
	function.SetClassPath("example.First")
	function.SetResources([]string{"go_sdk_missing_dependency.jar"})
	require.ErrorContains(t, functions.Create("", "", *function), "unavailable resource")
	function.SetResources([]string{resourceName})
	require.NoError(t, functions.Create("", "", *function))
	t.Cleanup(func() { _ = functions.Delete(name) })
	requireEmulatorServiceError(t, functions.Create(project, schema, *function), http.StatusBadRequest, "FunctionAlreadyExists")
	exists, err = function.Exist()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "example.First", function.ClassPath())
	require.Equal(t, []string{resourceName}, function.Resources())
	require.Equal(t, "emulator", function.Owner())

	function.SetClassPath("example.Updated")
	require.NoError(t, functions.Update(project, schema, *function))
	reloaded, err := functions.Get(name)
	require.NoError(t, err)
	require.NoError(t, reloaded.Load())
	require.Equal(t, "example.Updated", reloaded.ClassPath())
	require.False(t, reloaded.IsSQLFunction())
	require.NoError(t, functions.Delete(name))
	exists, err = function.Exist()
	require.NoError(t, err)
	require.False(t, exists)
	requireEmulatorServiceError(t, functions.Delete(name), http.StatusNotFound, "NoSuchObject")
	saved, err := resources.Get(resourceName).Exist()
	require.NoError(t, err)
	require.True(t, saved)
}

func TestEmulatorInstanceSuccessAndFailure(t *testing.T) {
	service := emulatorService(t)
	task := odps.NewSqlTask("fixture_select", "select cast(5 as bigint) as id, 'ok' as note", nil)
	instances := service.Instances()
	completed, err := instances.CreateTaskWithPriority(service.DefaultProjectName(), &task, 5)
	require.NoError(t, err)
	require.False(t, completed.IsLoaded())
	require.NoError(t, completed.WaitForSuccessContext(t.Context()))
	require.True(t, completed.IsLoaded())
	require.Equal(t, odps.InstanceTerminated, completed.Status())
	require.Equal(t, "emulator", completed.Owner())
	require.False(t, completed.StartTime().IsZero())
	require.False(t, completed.EndTime().IsZero())
	require.Equal(t, "fixture_select", completed.TaskNameCommitted())
	tasks, err := completed.GetTasks()
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, odps.TaskStatus(odps.TaskSuccess), tasks[0].Status)
	results, err := completed.GetResult()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "id,note\n5,ok\n", results[0].Content())
	snapshot := instances.Get(completed.Id())
	require.NoError(t, snapshot.Load())
	require.Equal(t, completed.TaskResults(), snapshot.TaskResults())

	failed, err := service.ExecSQl("select * from go_sdk_absent_table_for_instance_test")
	require.NoError(t, err)
	failure := failed.WaitForSuccessContext(t.Context())
	require.ErrorContains(t, failure, "go_sdk_absent_table_for_instance_test")
	require.EqualError(t, failed.WaitForSuccessContext(t.Context()), failure.Error())

	missing := instances.Get("invalid-instance")
	requireEmulatorServiceError(t, missing.Load(), http.StatusNotFound, "NoSuchInstance")
	_, err = missing.GetTasks()
	requireEmulatorServiceError(t, err, http.StatusNotFound, "NoSuchInstance")
	_, err = missing.GetResult()
	requireEmulatorServiceError(t, err, http.StatusNotFound, "NoSuchInstance")
}

func TestEmulatorTablePaginationAndLoadState(t *testing.T) {
	service := emulatorService(t)
	tables := service.Tables()
	prefix := fmt.Sprintf("go_sdk_page_%d_", time.Now().UnixNano())
	names := []string{prefix + "a", prefix + "b", prefix + "c"}
	for _, name := range names {
		schema := tableschema.NewSchemaBuilder().Name(name).Column(
			tableschema.Column{Name: "id", Type: datatype.BigIntType},
		).Build()
		require.NoError(t, tables.Create(schema, true, nil, nil))
		t.Cleanup(func() { require.NoError(t, tables.Delete(name, true)) })
	}

	table := tables.Get(names[0])
	require.False(t, table.IsLoaded())
	require.False(t, table.IsLoadedExtended())
	require.NoError(t, table.LoadExtendedInfo())
	require.True(t, table.IsLoadedExtended())
	require.False(t, table.IsLoaded(), "extended metadata can be loaded independently")
	require.Equal(t, "id", table.Schema().Columns[0].Name)
	exists, err := table.Exists()
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, table.IsLoaded())
	require.Equal(t, service.DefaultProjectName(), table.ProjectName())

	var gotNames []string
	tables.List(func(table *odps.Table, err error) {
		require.NoError(t, err)
		gotNames = append(gotNames, table.Name())
	}, func(values url.Values) {
		values.Set("maxitems", "1")
	}, odps.TableFilter.NamePrefix(prefix))
	require.Equal(t, names, gotNames)

	unknown := odps.NewTables(service, "go_sdk_missing_project", "")
	errorCount := 0
	unknown.List(func(table *odps.Table, err error) {
		require.Nil(t, table)
		requireEmulatorServiceError(t, err, http.StatusNotFound, "NoSuchProject")
		errorCount++
	})
	require.Equal(t, 1, errorCount)
}

func TestEmulatorTableTruncateRenameAndDelete(t *testing.T) {
	service := emulatorService(t)
	name := fmt.Sprintf("go_sdk_table_%d", time.Now().UnixNano())
	target := name + "_renamed"
	tables := service.Tables()
	schema := tableschema.NewSchemaBuilder().Name(name).
		Column(tableschema.Column{Name: "id", Type: datatype.BigIntType}).Build()
	require.NoError(t, tables.Create(schema, false, nil, nil))
	t.Cleanup(func() {
		require.NoError(t, tables.Delete(name, true))
		require.NoError(t, tables.Delete(target, true))
	})
	require.ErrorContains(t, tables.Create(schema, false, nil, nil), "TableAlreadyExists")

	table := service.Table(name)
	insert, err := table.ExecSql("fixture_insert", "insert into "+name+" values (17)")
	require.NoError(t, err)
	require.NoError(t, insert.WaitForSuccessContext(t.Context()))

	readRows := func(tableName string) string {
		t.Helper()
		query, err := service.ExecSQl("select id from " + tableName)
		require.NoError(t, err)
		require.NoError(t, query.WaitForSuccessContext(t.Context()))
		require.Len(t, query.TaskResults(), 1)
		result := query.TaskResults()[0]
		return result.Content()
	}
	require.Equal(t, "id\n17\n", readRows(name))
	require.NoError(t, table.Rename(target))
	exists, err := table.Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, "id\n17\n", readRows(target))

	renamed := tables.Get(target)
	require.NoError(t, renamed.Truncate())
	require.Equal(t, "id\n", readRows(target))
	require.NoError(t, renamed.Delete())
	exists, err = renamed.Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.ErrorContains(t, renamed.Truncate(), "NoSuchTable")
}

func TestEmulatorPartitionSpecsAndStrictDDL(t *testing.T) {
	service := emulatorService(t)
	name := fmt.Sprintf("go_sdk_partition_%d", time.Now().UnixNano())
	schema := tableschema.NewSchemaBuilder().Name(name).
		Column(tableschema.Column{Name: "id", Type: datatype.BigIntType}).
		PartitionColumn(tableschema.Column{Name: "ds", Type: datatype.StringType}).
		PartitionColumn(tableschema.Column{Name: "region", Type: datatype.StringType}).Build()
	tables := service.Tables()
	require.NoError(t, tables.Create(schema, false, nil, nil))
	t.Cleanup(func() { require.NoError(t, tables.Delete(name, true)) })

	table := tables.Get(name)
	const value = "ds=20261120/region=east=2"
	const malformed = "ds=20261120/region"
	require.ErrorContains(t, table.AddPartitions(false, []string{value, malformed}), "column=value")
	values, err := table.GetPartitionValues()
	require.NoError(t, err)
	require.Empty(t, values)

	require.NoError(t, table.AddPartition(false, value))
	require.ErrorContains(t, table.AddPartition(false, value), "PartitionAlreadyExists")
	require.NoError(t, table.AddPartition(true, value))

	partition, err := table.GetPartition(value)
	require.NoError(t, err)
	require.Equal(t, value, partition.Value())
	require.Equal(t, "ds='20261120',region='east=2'", partition.Spec())
	require.NoError(t, partition.Load())
	require.NoError(t, partition.LoadExtended())
	require.False(t, partition.CreatedTime().IsZero())
	partitions, err := table.GetPartitions()
	require.NoError(t, err)
	require.Len(t, partitions, 1)
	require.Equal(t, value, partitions[0].Value())

	_, err = table.GetPartition(malformed)
	require.ErrorContains(t, err, "column=value")
	invalid := odps.NewPartition(service, service.DefaultProjectName(), service.CurrentSchemaName(), name, malformed)
	require.ErrorContains(t, invalid.Load(), "column=value")
	require.ErrorContains(t, invalid.LoadExtended(), "column=value")
	require.ErrorContains(t, table.DeletePartition(false, malformed), "column=value")
	require.NoError(t, table.DeletePartition(false, value))
	require.ErrorContains(t, table.DeletePartition(false, value), "NoSuchPartition")
	require.NoError(t, table.DeletePartition(true, value))
}

func TestEmulatorSchemaLifecycle(t *testing.T) {
	service := emulatorService(t)
	client := http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, os.Getenv("MAXCOMPUTE_EMULATOR_ENDPOINT")+"/capabilities", nil)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var capabilities struct {
		Schemas []string `json:"schemas"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&capabilities))
	if len(capabilities.Schemas) == 0 {
		t.Skip("this Emulator image does not provide schema management")
	}

	name := fmt.Sprintf("go_sdk_schema_%d", time.Now().UnixNano())
	project := service.DefaultProjectName()
	schemas := service.Schemas()
	schema := schemas.Get(name)
	exists, err := schema.Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.False(t, schema.IsLoaded())
	require.NoError(t, schemas.Create(name, false, "test & readback"))
	t.Cleanup(func() { _ = schemas.Delete(name) })
	requireEmulatorServiceError(t, schemas.Create(name, false, "duplicate"), http.StatusConflict, "ObjectAlreadyExists")
	require.NoError(t, schemas.Create(name, true, "must preserve comment"))
	exists, err = schema.Exists()
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, schema.IsLoaded())
	require.Equal(t, name, schema.Name())
	require.Equal(t, project, schema.ProjectName())
	require.Equal(t, "test & readback", schema.Comment())
	require.Equal(t, "MANAGED", schema.Type())
	require.Equal(t, "emulator", schema.Owner())
	require.False(t, schema.CreateTime().IsZero())
	require.False(t, schema.ModifiedTime().IsZero())

	var listed []string
	require.NoError(t, schemas.ListWithFilter(func(item *odps.Schema, err error) {
		require.NoError(t, err)
		listed = append(listed, item.Name())
	}, nil, odps.SchemaFilter.NamePrefix(name), odps.SchemaFilter.Owner("emulator")))
	require.Equal(t, []string{name}, listed)

	tableSchema := tableschema.NewSchemaBuilder().Name("schema_scoped").
		Column(tableschema.Column{Name: "id", Type: datatype.BigIntType}).Build()
	require.NoError(t, schema.Tables().Create(tableSchema, false, nil, nil))
	exists, err = schema.Tables().Get("schema_scoped").Exists()
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = service.Table("schema_scoped").Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, schemas.Delete(name))
	exists, err = schema.Exists()
	require.NoError(t, err)
	require.False(t, exists)
	require.False(t, schema.IsLoaded())
}

func TestEmulatorFileResourceLifecycle(t *testing.T) {
	service := emulatorService(t)
	resources := odps.NewResources(service)
	resources.ChunkSize = 4
	name := fmt.Sprintf("go_sdk_resource_%d.txt", time.Now().UnixNano())
	file := odps.NewFileResource(name)
	file.SetComment("first")
	file.SetReader(strings.NewReader("multi-chunk upload"))
	require.NoError(t, resources.CreateFileResource("", "", file, false))
	t.Cleanup(func() { _ = resources.Delete(name) })
	resource := resources.Get(name)
	exists, err := resource.Exist()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, name, resource.Name())
	require.Equal(t, odps.ResourceTypeFile, resource.ResourceType())
	require.Equal(t, "first", resource.Comment())

	file.SetReader(strings.NewReader("replacement"))
	file.SetComment("updated")
	require.NoError(t, resources.UpdateFileResource("", "", file))
	require.NoError(t, resource.Load())
	require.Equal(t, "updated", resource.Comment())

	var found bool
	resources.List(func(resource *odps.Resource, err error) {
		require.NoError(t, err)
		if resource.Name() == name {
			found = true
		}
	}, nil)
	require.True(t, found)
	require.NoError(t, resources.Delete(name))
	exists, err = resources.Get(name).Exist()
	require.NoError(t, err)
	require.False(t, exists)

	require.ErrorContains(t, resources.CreateFileResource("", "", odps.NewFileResource(""), false), "name")
	require.ErrorContains(t, resources.CreateFileResource("", "", odps.NewFileResource(name), false), "reader")
}

type resourceEOFReader struct {
	*strings.Reader
}

func (r resourceEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.Len() == 0 {
		return n, io.EOF
	}
	return n, err
}

func TestEmulatorResourceKeepsFinalReadWithEOF(t *testing.T) {
	service := emulatorService(t)
	resources := odps.NewResources(service)
	resources.ChunkSize = 4
	name := fmt.Sprintf("go_sdk_tail_%d.py", time.Now().UnixNano())
	file := odps.NewPyResource(name)
	file.SetReader(resourceEOFReader{strings.NewReader("abcdefg")})
	require.NoError(t, resources.CreateFileResource("", "", file, false))
	t.Cleanup(func() { require.NoError(t, resources.Delete(name)) })
	loaded := resources.Get(name)
	require.NoError(t, loaded.Load())
	require.Equal(t, odps.ResourceTypePy, loaded.ResourceType())
	require.EqualValues(t, 7, loaded.Model.ResourceSize)
	require.Equal(t, "7ac66c0f148de9519b8bd264312c4d64", loaded.Model.ContentMD5)

	failed := odps.NewFileResource(name + "_failed")
	failed.SetReader(iotest.ErrReader(io.ErrClosedPipe))
	require.ErrorIs(t, resources.CreateFileResource("", "", failed, false), io.ErrClosedPipe)
	exists, err := resources.Get(failed.Name()).Exist()
	require.NoError(t, err)
	require.False(t, exists)
	resources.ChunkSize = 0
	require.ErrorContains(t, resources.CreateFileResource("", "", failed, false), "chunk size")
}

func TestEmulatorTableResourcesAndPagination(t *testing.T) {
	service := emulatorService(t)
	resources := odps.NewResources(service)
	project, schema := service.DefaultProjectName(), service.CurrentSchemaName()
	prefix := fmt.Sprintf("go_sdk_table_res_%d_", time.Now().UnixNano())
	names := []string{prefix + "a", prefix + "b"}
	table := prefix + "source"
	tables := service.Tables()
	definition := tableschema.NewSchemaBuilder().Name(table).Column(
		tableschema.Column{Name: "id", Type: datatype.BigIntType},
	).PartitionColumn(tableschema.Column{Name: "ds", Type: datatype.StringType}).Build()
	require.NoError(t, tables.Create(definition, false, nil, nil))
	t.Cleanup(func() { require.NoError(t, tables.Delete(table, true)) })
	require.NoError(t, tables.Get(table).AddPartition(false, "ds=sample"))

	resource := odps.NewTableResource(project, table, "ds='sample'")
	require.ErrorContains(t, resources.CreateTableResource("", "", resource, false), "resource name")
	for _, name := range names {
		resource.SetName(name)
		require.NoError(t, resources.CreateTableResource("", "", resource, false))
		t.Cleanup(func() { require.NoError(t, resources.Delete(name)) })
	}
	requireEmulatorServiceError(t, resources.CreateTableResource(project, schema, resource, false),
		http.StatusBadRequest, "ResourceAlreadyExists")
	resource.SetComment("updated metadata")
	require.NoError(t, resources.CreateTableResource(project, schema, resource, true))
	got := resources.Get(resource.Name())
	require.NoError(t, got.Load())
	require.Equal(t, schema, got.SchemaName())
	require.Equal(t, odps.ResourceTypeTable, got.ResourceType())
	require.Equal(t, project+"."+table+" partition(ds='sample')", got.Model.SourceTableName)
	require.Equal(t, "updated metadata", got.Comment())

	var listed []string
	resources.List(func(item *odps.Resource, err error) {
		require.NoError(t, err)
		listed = append(listed, item.Name())
	}, odps.ResourceFilter.NamePrefix(prefix), func(args url.Values) { args.Set("maxitems", "1") })
	require.Equal(t, names, listed)
	resources.List(func(item *odps.Resource, err error) {
		require.Nil(t, item)
		requireEmulatorServiceError(t, err, http.StatusBadRequest, "InvalidParameter")
	}, func(args url.Values) { args.Set("maxitems", "invalid") })
}
