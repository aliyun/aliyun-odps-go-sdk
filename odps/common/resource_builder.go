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

// ResourceBuilder is a helper to build resource path
type ResourceBuilder struct {
	ProjectName string
}

// NewResourceBuilder creates a new ResourceBuilder
func NewResourceBuilder(projectName string) ResourceBuilder {
	return ResourceBuilder{ProjectName: url.QueryEscape(projectName)}
}

// SetProject sets the project name
func (rb *ResourceBuilder) SetProject(name string) {
	rb.ProjectName = url.QueryEscape(name)
}

// Projects returns the projects resource path
func (rb *ResourceBuilder) Projects() string {
	return ProjectsPath
}

// Project returns the project resource path
func (rb *ResourceBuilder) Project() string {
	return path.Join(ProjectsPath, rb.ProjectName)
}

// Tables returns the tables resource path
func (rb *ResourceBuilder) Tables() string {
	return path.Join(ProjectsPath, rb.ProjectName, TablesPath)
}

// Table returns the table resource path
func (rb *ResourceBuilder) Table(tableName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, TablesPath, url.PathEscape(tableName))
}

// Schemas returns the schemas resource path
func (rb *ResourceBuilder) Schemas() string {
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath)
}

// Schema returns the schema resource path
func (rb *ResourceBuilder) Schema(schemaName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath, url.PathEscape(schemaName))
}

// TableStream returns the table stream resource path
func (rb *ResourceBuilder) TableStream(tableName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, TablesPath, url.PathEscape(tableName), StreamsPath)
}

// TableWithSchemaName returns the table resource path with schema name
func (rb *ResourceBuilder) TableWithSchemaName(tableName, schemaName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, SchemasPath, schemaName, TablesPath, url.PathEscape(tableName))
}

// Functions returns the functions resource path
func (rb *ResourceBuilder) Functions() string {
	return path.Join(ProjectsPath, rb.ProjectName, RegistrationPath, FunctionsPath)
}

// Function returns the function resource path
func (rb *ResourceBuilder) Function(functionName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, RegistrationPath, FunctionsPath, url.PathEscape(functionName))
}

// XFlows returns the xflows resource path
func (rb *ResourceBuilder) XFlows() string {
	return path.Join(ProjectsPath, rb.ProjectName, XFlowsPath)
}

// XFlow returns the xflow resource path
func (rb *ResourceBuilder) XFlow(xFlowName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, XFlowsPath, url.PathEscape(xFlowName))
}

// Instances returns the instances resource path
func (rb *ResourceBuilder) Instances() string {
	return path.Join(ProjectsPath, rb.ProjectName, InstancesPath)
}

// CachedInstances returns the cached instances resource path
func (rb *ResourceBuilder) CachedInstances() string {
	return path.Join(ProjectsPath, rb.ProjectName, CachedInstancesPath)
}

// Instance returns the instance resource path
func (rb *ResourceBuilder) Instance(instanceId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, InstancesPath, url.PathEscape(instanceId))
}

// Resources returns the resources resource path
func (rb *ResourceBuilder) Resources() string {
	return path.Join(ProjectsPath, rb.ProjectName, ResourcesPath)
}

// Resource returns the resource resource path
func (rb *ResourceBuilder) Resource(resourceName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, ResourcesPath, url.PathEscape(resourceName))
}

// Volumes returns the volumes resource path
func (rb *ResourceBuilder) Volumes() string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath)
}

// Volume returns the volume resource path
func (rb *ResourceBuilder) Volume(volumeName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath, url.PathEscape(volumeName))
}

// VolumePartition returns the volume partition resource path
func (rb *ResourceBuilder) VolumePartition(volumeName, partitionKey string) string {
	return path.Join(ProjectsPath, rb.ProjectName, VolumesPath, url.PathEscape(volumeName), partitionKey)
}

// Users returns the users resource path
func (rb *ResourceBuilder) Users() string {
	return path.Join(ProjectsPath, rb.ProjectName, UsersPath)
}

// User returns the user resource path
func (rb *ResourceBuilder) User(userId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, UsersPath, url.PathEscape(userId))
}

// Roles returns the roles resource path
func (rb *ResourceBuilder) Roles() string {
	return path.Join(ProjectsPath, rb.ProjectName, RolesPath)
}

// Role returns the role resource path
func (rb *ResourceBuilder) Role(roleName string) string {
	return path.Join(ProjectsPath, rb.ProjectName, RolesPath, url.PathEscape(roleName))
}

// Auth returns the auth resource path
func (rb *ResourceBuilder) Auth() string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthPath)
}

// Authorization returns the authorization resource path
func (rb *ResourceBuilder) Authorization() string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthorizationPath)
}

// AuthorizationId returns the authorization resource path with instance id
func (rb *ResourceBuilder) AuthorizationId(instanceId string) string {
	return path.Join(ProjectsPath, rb.ProjectName, AuthorizationPath, url.PathEscape(instanceId))
}

// Tunnel returns the tunnel resource path
func (rb *ResourceBuilder) Tunnel() string {
	return path.Join(ProjectsPath, rb.ProjectName, TunnelPath)
}
