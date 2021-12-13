package odps

import (
	account2 "github.com/aliyun/aliyun-odps-go-sdk/account"
	"github.com/aliyun/aliyun-odps-go-sdk/rest_client"
	"github.com/pkg/errors"
	"time"
)

type Odps struct {
	defaultProject string

	account    account2.Account
	restClient rest_client.RestClient
	rb         ResourceBuilder
	projects   Projects
}

func NewOdps(account account2.Account, endpoint string) *Odps {
	ins := Odps{
		account:    account,
		restClient: rest_client.NewOdpsRestClient(account, endpoint),
	}

	ins.projects = NewProjects(&ins)

	return &ins
}

func (odps *Odps) Account() account2.Account {
	return odps.account
}

func (odps *Odps) RestClient() rest_client.RestClient {
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
	odps.restClient.SetDefaultProject(projectName)
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
