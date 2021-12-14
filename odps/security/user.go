package security

import (
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

type User struct {
	restClient  restclient.RestClient
	projectName string
	model       userModel
}

type userModel struct {
	XMLName     xml.Name `xml:"User"`
	ID          string
	DisplayName string
	Comment     string
}

func NewUser(userId string, restClient restclient.RestClient, projectName string) User {
	return User{
		restClient:  restClient,
		projectName: projectName,
		model:       userModel{ID: userId},
	}
}

func (user *User) Load() error {
	rb := common.NewResourceBuilder(user.projectName)
	resource := rb.User(user.model.ID)
	client := user.restClient

	return client.GetWithModel(resource, nil, &user.model)
}

func (user *User) ID() string {
	return user.model.ID
}

func (user *User) DisplayName() string {
	return user.model.DisplayName
}

func (user *User) Comment() string {
	return user.model.Comment
}
