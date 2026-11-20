// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
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
