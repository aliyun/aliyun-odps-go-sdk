// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
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
	project, schema := service.DefaultProjectName(), service.CurrentSchemaName()
	suffix := time.Now().UnixNano()
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
