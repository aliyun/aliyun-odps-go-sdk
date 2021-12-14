package security

import (
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
)

type Role struct {
	restClient  restclient.RestClient
	projectName string
	model       roleModel
}

type roleModel struct {
	XMLName xml.Name `xml:"Role"`
	Name    string
	Comment string
}

func NewRole(name string, restClient restclient.RestClient, projectName string) Role {
	return Role{
		restClient:  restClient,
		projectName: projectName,
		model:       roleModel{Name: name},
	}
}

func (role *Role) Load() error {
	rb := common.NewResourceBuilder(role.projectName)
	resource := rb.Role(role.model.Name)
	client := role.restClient

	return errors.WithStack(client.GetWithModel(resource, nil, &role.model))
}

func (role *Role) Name() string {
	return role.model.Name
}

func (role *Role) Comment() string {
	return role.model.Comment
}
