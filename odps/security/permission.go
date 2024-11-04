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
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type Permission struct {
	ProjectName string
	ObjectType  PermissionObjectType
	ObjectName  string
	ActionType  PermissionActionType
	Params      map[string]string
}

type PermissionCheckResult struct {
	Result  string
	Message string
}

func NewPermission(
	projectName string,
	objectType PermissionObjectType,
	objectName string,
	actionType PermissionActionType) Permission {

	return Permission{
		ProjectName: projectName,
		ObjectType:  objectType,
		ObjectName:  objectName,
		ActionType:  actionType,
		Params:      make(map[string]string),
	}
}

func (perm *Permission) SetColumns(columns []string) {
	j, _ := json.Marshal(columns)
	perm.Params["odps:SelectColumn"] = string(j)
}

func (perm Permission) MarshalJSON() ([]byte, error) {
	m := make(map[string]string, len(perm.Params)+2)
	m["Action"] = perm.ActionType.String()
	m["Resource"] = perm.Resource()

	for key, value := range perm.Params {
		m[key] = value
	}

	r := []map[string]string{m}
	b, err := json.Marshal(r)
	return b, errors.WithStack(err)
}

func (perm *Permission) Resource() string {
	rb := common.NewResourceBuilder(perm.ProjectName)

	switch perm.ObjectType {
	case ObjectTypeProject:
		return rb.Project()
	case ObjectTypeTable:
		return rb.Table(perm.ObjectName)
	case ObjectTypeFunction:
		return rb.Function(perm.ObjectName)
	case ObjectTypeInstance:
		return rb.Function(perm.ProjectName)
	case ObjectTypeResource:
		return rb.Resource(perm.ObjectName)
	default:
		return ""
	}
}
