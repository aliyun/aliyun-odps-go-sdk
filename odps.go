package odps

import (
	"github.com/pkg/errors"
	"time"
)

type Odps struct {
	defaultProject string

	account    Account
	restClient RestClient
	rb         ResourceBuilder
	projects   Projects
}

func NewOdps(account Account, endpoint string) *Odps {
	ins := Odps{
		account:    account,
		restClient: NewOdpsRestClient(account, endpoint),
	}

	ins.projects = NewProjects(&ins)

	return &ins
}

func (odps *Odps) Account() Account {
	return odps.account
}

func (odps *Odps) RestClient() RestClient {
	return odps.restClient
}

func (odps *Odps) SetTcpConnectTimeout(t time.Duration) {
	odps.restClient.TcpConnectionTimeout = t
}

func (odps *Odps) SetHttpTimeout(t time.Duration) {
	odps.restClient.HttpTimeout = t
}

func (odps *Odps) DefaultProject() Project {
	return NewProject(odps.defaultProject, odps)
}

func (odps *Odps) DefaultProjectName() string {
	return odps.defaultProject
}

func (odps *Odps) SetDefaultProjectName(projectName string) {
	odps.defaultProject = projectName
	odps.rb.SetProject(projectName)
	odps.restClient.setDefaultProject(projectName)
}

func (odps *Odps) Projects() Projects {
	return odps.projects
}

func (odps *Odps) Project(name string) Project {
	return NewProject(name, odps)
}

func (odps *Odps) RunSQl(sql string) (*Instance, error) {
	task := NewSqlTask("execute_sql", sql, "", nil)
	Instances := NewInstances(odps, odps.defaultProject)
	i, err := Instances.CreateTask(odps.defaultProject, &task)
	return i, errors.WithStack(err)
}
