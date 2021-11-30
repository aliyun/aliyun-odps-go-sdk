package odps

import (
	"net/url"
	"path"
)

const (
	ProjectsPath        = "/projects"
	SchemasPath          = "/schemas"
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
	TunnelPath              = "/tunnel"
)

type ResourceBuilder struct {
	projectName string
}

func NewResourceBuilder(projectName string) ResourceBuilder {
	return ResourceBuilder{projectName: url.QueryEscape(projectName)}
}

func (rb *ResourceBuilder) SetProject(name string) {
	rb.projectName = url.QueryEscape(name)
}

func (rb *ResourceBuilder) Projects() string {
	return ProjectsPath
}

func (rb *ResourceBuilder) Project() string {
	return path.Join(ProjectsPath, rb.projectName)
}

func (rb *ResourceBuilder) Tables() string {
	return path.Join(ProjectsPath, rb.projectName, TablesPath)
}

func (rb *ResourceBuilder) Table(tableName string) string {
	return path.Join(ProjectsPath, rb.projectName, TablesPath, url.PathEscape(tableName))
}

func (rb *ResourceBuilder) TableWithSchemaName(tableName, schemaName string) string {
	return path.Join(ProjectsPath, rb.projectName, SchemasPath, schemaName, TablesPath, url.PathEscape(tableName))
}

func (rb *ResourceBuilder) Functions() string {
	return path.Join(ProjectsPath, rb.projectName, RegistrationPath, FunctionsPath)
}

func (rb *ResourceBuilder) Function(functionName string) string {
	return path.Join(ProjectsPath, rb.projectName, RegistrationPath, FunctionsPath, url.PathEscape(functionName))
}

func (rb *ResourceBuilder) XFlows() string {
	return path.Join(ProjectsPath, rb.projectName, XFlowsPath)
}

func (rb *ResourceBuilder) XFlow(xFlowName string) string {
	return path.Join(ProjectsPath, rb.projectName, XFlowsPath, url.PathEscape(xFlowName))
}

func (rb *ResourceBuilder) Instances() string {
	return path.Join(ProjectsPath, rb.projectName, InstancesPath)
}

func (rb *ResourceBuilder) CachedInstances() string {
	return path.Join(ProjectsPath, rb.projectName, CachedInstancesPath)
}

func (rb *ResourceBuilder) Instance(instanceId string) string {
	return path.Join(ProjectsPath, rb.projectName, InstancesPath, url.PathEscape(instanceId))
}

func (rb *ResourceBuilder) Resources() string {
	return path.Join(ProjectsPath, rb.projectName, ResourcesPath)
}

func (rb *ResourceBuilder) Resource(resourceName string) string {
	return path.Join(ProjectsPath, rb.projectName, ResourcesPath, url.PathEscape(resourceName))
}

func (rb *ResourceBuilder) Volumes() string {
	return path.Join(ProjectsPath, rb.projectName, VolumesPath)
}

func (rb *ResourceBuilder) Volume(volumeName string) string {
	return path.Join(ProjectsPath, rb.projectName, VolumesPath, url.PathEscape(volumeName))
}

func (rb *ResourceBuilder) VolumePartition(volumeName, partitionKey string) string {
	return path.Join(ProjectsPath, rb.projectName, VolumesPath, url.PathEscape(volumeName), partitionKey)
}

func (rb *ResourceBuilder) Users() string {
	return path.Join(ProjectsPath, rb.projectName, UsersPath)
}

func (rb *ResourceBuilder) User(userId string) string {
	return path.Join(ProjectsPath, rb.projectName, UsersPath, url.PathEscape(userId))
}

func (rb *ResourceBuilder) Roles() string {
	return path.Join(ProjectsPath, rb.projectName, RolesPath)
}

func (rb *ResourceBuilder) Role(roleName string) string {
	return path.Join(ProjectsPath, rb.projectName, RolesPath, url.PathEscape(roleName))
}

func (rb *ResourceBuilder) Auth() string {
	return path.Join(ProjectsPath, rb.projectName, AuthPath)
}

func (rb *ResourceBuilder) Authorization() string {
	return path.Join(ProjectsPath, rb.projectName, AuthorizationPath)
}

func (rb *ResourceBuilder) AuthorizationId(instanceId string) string {
	return path.Join(ProjectsPath, rb.projectName, AuthorizationPath, url.PathEscape(instanceId))
}

func (rb *ResourceBuilder) Tunnel() string {
	return path.Join(ProjectsPath, rb.projectName, TunnelPath)
}
