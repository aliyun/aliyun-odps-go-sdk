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
