// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

type Odps struct {
	defaultProject string
	currentSchema  string

	Options    options.OdpsOptions
	account    account2.Account
	restClient restclient.RestClient
	rb         common.ResourceBuilder
	projects   Projects
	region     atomic.Pointer[projectRegion]
}

type projectRegion struct {
	project string
	id      string
}

func NewOdps(account account2.Account, endpoint string) *Odps {
	return NewOdpsWithHTTPTransport(account, endpoint, nil)
}

// NewOdpsWithHTTPTransport sends signed ODPS, Tunnel and Storage requests
// through transport. The caller owns the transport and its connection pool.
// A nil transport selects the SDK default.
func NewOdpsWithHTTPTransport(account account2.Account, endpoint string, transport http.RoundTripper) *Odps {
	ins := Odps{
		account:    account,
		restClient: restclient.NewOdpsRestClientWithTransport(account, endpoint, transport),
		Options:    *options.NewOdpsOptions(),
	}
	if aliyunAccount, ok := account.(*account2.AliyunAccount); ok {
		if aliyunAccount.RegionId() != "" {
			ins.Options.RegionId = aliyunAccount.RegionId()
		}
	}
	ins.projects = NewProjects(&ins)
	return &ins
}

func (odps *Odps) Account() account2.Account {
	return odps.account
}

func (odps *Odps) RestClient() restclient.RestClient {
	return odps.restClient
}

func (odps *Odps) SetTcpConnectTimeout(t time.Duration) {
	odps.restClient.TcpConnectionTimeout = t
}

func (odps *Odps) SetHttpTimeout(t time.Duration) {
	odps.restClient.HttpTimeout = t
}

func (odps *Odps) SetUserAgent(userAgent string) {
	odps.restClient.SetUserAgent(userAgent)
}

func (odps *Odps) DefaultProject() *Project {
	return NewProject(odps.defaultProject, odps)
}

func (odps *Odps) DefaultProjectName() string {
	return odps.defaultProject
}

func (odps *Odps) CurrentSchemaName() string {
	return odps.currentSchema
}

func (odps *Odps) SetDefaultProjectName(projectName string) {
	odps.defaultProject = projectName
	odps.rb.SetProject(projectName)
	odps.restClient.SetDefaultProject(projectName)
}

func (odps *Odps) SetCurrentSchemaName(schemaName string) {
	odps.currentSchema = schemaName
	odps.restClient.SetCurrentSchema(schemaName)
}

func (odps *Odps) Projects() *Projects {
	return &odps.projects
}

func (odps *Odps) Project(name string) *Project {
	return NewProject(name, odps)
}

func (odps *Odps) Schemas() *Schemas {
	return NewSchemas(odps, odps.DefaultProjectName())
}

func (odps *Odps) Schema(name string) *Schema {
	return NewSchema(odps, odps.DefaultProjectName(), name)
}

func (odps *Odps) Tables() *Tables {
	return NewTables(odps, odps.DefaultProjectName(), odps.CurrentSchemaName())
}

func (odps *Odps) Table(name string) *Table {
	return NewTable(odps, odps.DefaultProjectName(), odps.CurrentSchemaName(), name)
}

func (odps *Odps) Instances() *Instances {
	return NewInstances(odps)
}

func (odps *Odps) Instance(instanceId string) *Instance {
	return NewInstance(odps, odps.defaultProject, instanceId)
}

func (odps *Odps) LogView() *LogView {
	return NewLogView(odps)
}

func (odps *Odps) Tenant() *Tenant {
	return NewTenant(odps)
}

func (odps *Odps) RegionId() string {
	return odps.regionID(context.Background(), odps.defaultProject)
}

func (odps *Odps) regionID(ctx context.Context, projectName string) string {
	configured := odps.Options.RegionId
	if configured != "cn" {
		return configured
	}
	if cached := odps.region.Load(); cached != nil && cached.project == projectName {
		return cached.id
	}
	project := odps.Project(projectName)
	if err := project.load(ctx); err != nil {
		return configured
	}
	regionID := project.RegionId()
	if regionID == "" {
		return configured
	}
	odps.region.Store(&projectRegion{project: projectName, id: regionID})
	return regionID
}

func (odps *Odps) ExecSQlWithHints(sql string, hints map[string]string) (*Instance, error) {
	return odps.ExecSQLContext(context.Background(), sql, hints)
}

func (odps *Odps) ExecSQl(sql string, hints ...map[string]string) (*Instance, error) {
	return odps.ExecSQLContext(context.Background(), sql, hints...)
}

// ExecSQLContext submits a SQL instance using ctx. If the request is canceled
// before its response arrives, the server may already have accepted the job.
func (odps *Odps) ExecSQLContext(ctx context.Context, sql string, hints ...map[string]string) (*Instance, error) {
	option := options.NewSQLTaskOptions()
	if len(hints) > 0 {
		option.Hints = hints[0]
	}
	return odps.execSQLWithOption(ctx, sql, option)
}

// ExecSQlWithOption Create a SQLTask with options.SQLTaskOptions
func (odps *Odps) ExecSQlWithOption(sql string, option *options.SQLTaskOptions) (*Instance, error) {
	return odps.execSQLWithOption(context.Background(), sql, option)
}

func (odps *Odps) execSQLWithOption(ctx context.Context, sql string, option *options.SQLTaskOptions) (*Instance, error) {
	if odps.defaultProject == "" {
		return nil, errors.New("default project has not been set for odps")
	}
	if option == nil {
		option = options.NewSQLTaskOptions()
	}
	taskOptions := *option
	if taskOptions.Hints == nil {
		taskOptions.Hints = make(map[string]string)
	}
	if taskOptions.DefaultSchema == "" {
		taskOptions.DefaultSchema = odps.currentSchema
	}
	task := NewSQLTaskWithOptions(sql, &taskOptions)
	instances := NewInstances(odps, odps.defaultProject)
	i, err := instances.createTask(ctx, odps.defaultProject, &task, taskOptions.InstanceOption)
	return i, errors.WithStack(err)
}
