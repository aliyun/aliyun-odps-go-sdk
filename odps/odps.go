package odps

import (
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
	"time"
)

type Odps struct {
	defaultProject string

	account    account2.Account
	restClient restclient.RestClient
	rb         common.ResourceBuilder
	projects   Projects
}

func NewOdps(account account2.Account, endpoint string) *Odps {
	ins := Odps{
		account:    account,
		restClient: restclient.NewOdpsRestClient(account, endpoint),
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

func (odps *Odps) Tables() Tables {
	return NewTables(odps)
}

func (odps *Odps) Table(name string) Table {
	return NewTable(odps, odps.DefaultProjectName(), name)
}

func (odps *Odps) Instances() Instances {
	return NewInstances(odps)
}

func (odps *Odps) Instance(instanceId string) Instance {
	return NewInstance(odps, odps.defaultProject, instanceId)
}

func (odps *Odps) RunSQl(sql string) (*Instance, error) {
	task := NewSqlTask("execute_sql", sql, "", nil)
	Instances := NewInstances(odps, odps.defaultProject)
	i, err := Instances.CreateTask(odps.defaultProject, &task)
	return i, errors.WithStack(err)
}
