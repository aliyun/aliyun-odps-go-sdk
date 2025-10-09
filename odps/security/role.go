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

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
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

	return errors.WithStack(client.GetWithModel(resource, nil, nil, &role.model))
}

func (role *Role) Name() string {
	return role.model.Name
}

func (role *Role) Comment() string {
	return role.model.Comment
}
