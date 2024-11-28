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

package common

import (
	"net/url"
	"path"
)

const (
	ProjectsPath        = "/projects"
	SchemasPath         = "/schemas"
	TablesPath          = "/tables"
	RegistrationPath    = "/registration"
	FunctionsPath       = "/functions"
	EventsPath          = "/events"
	ResourcesPath       = "/resources"
	InstancesPath       = "/instances"
	CachedInstancesPath = "/cachedinstances"
	VolumesPath         = "/volumes"
	StreamsPath         = "/streams"
	TopologiesPath      = "/topologies"
	XFlowsPath          = "/xflows"
	StreamJobsPath      = "/streamjobs"
	ServersPath         = "/servers"
	MatricesPath        = "/matrices"
	OfflineModelsPath   = "/offlinemodels"
	UsersPath           = "/users"
	RolesPath           = "/roles"
	SessionsPath        = "/session"
	AuthPath            = "/auth"
	AuthorizationPath   = "/authorization"
	TunnelPath          = "/tunnel"
)

type ResourceBuilder struct {
	ProjectName string
}

func NewResourceBuilder(projectName string) ResourceBuilder {
	return ResourceBuilder{ProjectName: url.QueryEscape(projectName)}
}

func (rb *ResourceBuilder) SetProject(name string) {
	rb.ProjectName = url.QueryEscape(name)
}

func (rb *ResourceBuilder) Projects() string {
	return ProjectsPath
}

func (rb *ResourceBuilder) Project() string {
	return path.Join(ProjectsPath, rb.ProjectName)
}

func (rb *ResourceBuilder) Tables() string {
	return path.Join(ProjectsPath, rb.ProjectName, TablesPath)
}

func (rb *ResourceBuilder) Table(schemaName, tableName string) string {
	if schemaName == "" {
		return path.Join(ProjectsPath, rb.ProjectName, TablesPath, url.PathEscape(tableName))
	}
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath, url.PathEscape(schemaName), TablesPath, url.PathEscape(tableName))
}

func (rb *ResourceBuilder) Schemas() string {
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath)
}

func (rb *ResourceBuilder) Schema(schemaName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath, url.PathEscape(schemaName))
}

func (rb *ResourceBuilder) Functions() string {
	return path.Join(ProjectsPath, rb.ProjectName, RegistrationPath, FunctionsPath)
}

func (rb *ResourceBuilder) Function(functionName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, RegistrationPath, FunctionsPath, url.PathEscape(functionName))
}

func (rb *ResourceBuilder) XFlows() string {
	return path.Join(ProjectsPath, rb.ProjectName, XFlowsPath)
}

func (rb *ResourceBuilder) XFlow(xFlowName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, XFlowsPath, url.PathEscape(xFlowName))
}

func (rb *ResourceBuilder) Instances() string {
	return path.Join(ProjectsPath, rb.ProjectName, InstancesPath)
}

func (rb *ResourceBuilder) CachedInstances() string {
	return path.Join(ProjectsPath, rb.ProjectName, CachedInstancesPath)
}

func (rb *ResourceBuilder) Instance(instanceId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, InstancesPath, url.PathEscape(instanceId))
}

func (rb *ResourceBuilder) Resources() string {
	return path.Join(ProjectsPath, rb.ProjectName, ResourcesPath)
}

func (rb *ResourceBuilder) Resource(resourceName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, ResourcesPath, url.PathEscape(resourceName))
}

func (rb *ResourceBuilder) Volumes() string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath)
}

func (rb *ResourceBuilder) Volume(volumeName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath, url.PathEscape(volumeName))
}

func (rb *ResourceBuilder) VolumePartition(volumeName, partitionKey string) string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath, url.PathEscape(volumeName), partitionKey)
}

func (rb *ResourceBuilder) Users() string {
	return path.Join(ProjectsPath, rb.ProjectName, UsersPath)
}

func (rb *ResourceBuilder) User(userId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, UsersPath, url.PathEscape(userId))
}

func (rb *ResourceBuilder) Roles() string {
	return path.Join(ProjectsPath, rb.ProjectName, RolesPath)
}

func (rb *ResourceBuilder) Role(roleName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, RolesPath, url.PathEscape(roleName))
}

func (rb *ResourceBuilder) Auth() string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthPath)
}

func (rb *ResourceBuilder) Authorization() string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthorizationPath)
}

func (rb *ResourceBuilder) AuthorizationId(instanceId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthorizationPath, url.PathEscape(instanceId))
}

func (rb *ResourceBuilder) Tunnel() string {
	return path.Join(ProjectsPath, rb.ProjectName, TunnelPath)
}
