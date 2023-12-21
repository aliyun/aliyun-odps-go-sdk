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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type Manager struct {
	restClient     restclient.RestClient
	projectName    string
	securityConfig Config
}

func NewSecurityManager(restClient restclient.RestClient, projectName string) Manager {
	return Manager{
		restClient:     restClient,
		projectName:    projectName,
		securityConfig: NewSecurityConfig(restClient, false, projectName),
	}
}

func (sm *Manager) GetSecurityConfig(withoutExceptionPolicy bool) (Config, error) {
	if sm.securityConfig.BeLoaded() {
		return sm.securityConfig, nil
	}

	sm.securityConfig.withoutExceptionPolicy = withoutExceptionPolicy
	err := sm.securityConfig.Load()

	return sm.securityConfig, errors.WithStack(err)
}

func (sm *Manager) SetSecurityConfig(config Config, supervisionToken string) error {
	err := config.Update(supervisionToken)
	sm.securityConfig = config
	return errors.WithStack(err)
}

func (sm *Manager) CheckPermissionV1(p Permission) (*PermissionCheckResult, error) {
	rb := sm.rb()
	resource := rb.Auth()
	client := sm.restClient

	type ResModel struct {
		XMLName xml.Name `xml:"Auth"`
		PermissionCheckResult
	}

	body, err := json.Marshal(p)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	req, err := client.NewRequest(common.HttpMethod.PostMethod, resource, bytes.NewReader(body))
	req.Header.Set(common.HttpHeaderContentType, "application/json")
	var resModel ResModel
	err = client.DoWithModel(req, &resModel)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &resModel.PermissionCheckResult, nil
}

func (sm *Manager) CheckPermissionV0(
	objectType PermissionObjectType,
	objectName string,
	actionType PermissionActionType,
	columns []string,
) (*PermissionCheckResult, error) {

	rb := sm.rb()
	resource := rb.Auth()
	client := sm.restClient

	queryArgs := make(url.Values, 4)
	queryArgs.Set("type", objectType.String())
	queryArgs.Set("name", objectName)
	queryArgs.Set("grantee", actionType.String())

	if len(columns) > 0 {
		queryArgs.Set("columns", strings.Join(columns, ","))
	}

	type ResModel struct {
		XMLName xml.Name `xml:"Auth"`
		PermissionCheckResult
	}

	var resModel ResModel
	err := client.GetWithModel(resource, queryArgs, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &resModel.PermissionCheckResult, nil
}

func (sm *Manager) getPolicy(resource, policyType string) ([]byte, error) {
	queryArgs := make(url.Values, 1)
	queryArgs.Set(policyType, "")
	client := sm.restClient
	var body []byte

	err := client.GetWithParseFunc(resource, queryArgs, func(res *http.Response) error {
		var err error
		body, err = ioutil.ReadAll(res.Body)
		return errors.WithStack(err)
	})

	return body, errors.WithStack(err)
}

func (sm *Manager) setPolicy(resource, policyType string, policy string) error {
	queryArgs := make(url.Values, 1)
	queryArgs.Set(policyType, "")
	client := sm.restClient
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.PutMethod, resource, strings.NewReader(policy), queryArgs)

	if err != nil {
		return errors.WithStack(err)
	}

	if policyType == "security_policy" {
		req.Header.Set(common.HttpHeaderContentType, "application/json")
	}

	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

func (sm *Manager) GetPolicy() ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Project(), "policy")
}

func (sm *Manager) SetPolicy(policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Project(), "policy", policy)
}

func (sm *Manager) GetSecurityPolicy() ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Project(), "security_policy")
}

func (sm *Manager) SetSecurityPolicy(policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Project(), "security_policy", policy)
}

func (sm *Manager) GetRolePolicy(roleName string) ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Role(roleName), "policy")
}

func (sm *Manager) SetRolePolicy(roleName, policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Role(roleName), "policy", policy)
}

func (sm *Manager) ListUsers() ([]User, error) {
	rb := sm.rb()
	resource := rb.Users()
	client := sm.restClient

	type ResModel struct {
		XMLName xml.Name    `xml:"Users"`
		User    []userModel `xml:"User"`
	}

	var resModel ResModel

	err := client.GetWithModel(resource, nil, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	users := make([]User, len(resModel.User))
	for i, model := range resModel.User {
		user := NewUser(model.ID, sm.restClient, sm.projectName)
		user.model = model
		users[i] = user
	}

	return users, nil
}

func (sm *Manager) ListRoles() ([]Role, error) {
	rb := sm.rb()
	resource := rb.Roles()
	client := sm.restClient

	type ResModel struct {
		XMLName xml.Name    `xml:"Roles"`
		Role    []roleModel `xml:"Role"`
	}

	var resModel ResModel

	err := client.GetWithModel(resource, nil, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	roles := make([]Role, len(resModel.Role))
	for i, model := range resModel.Role {
		role := NewRole(model.Name, sm.restClient, sm.projectName)
		role.model = model
		roles[i] = role
	}

	return roles, nil
}

func (sm *Manager) listRolesForUser(userIdOrName, _type string) ([]Role, error) {
	rb := sm.rb()
	resource := rb.User(userIdOrName)
	client := sm.restClient
	queryArgs := make(url.Values, 2)
	queryArgs.Set("role", "")
	if _type != "" {
		queryArgs.Set("type", _type)
	}

	type ResModel struct {
		XMLName xml.Name    `xml:"Roles"`
		Role    []roleModel `xml:"Role"`
	}

	var resModel ResModel

	err := client.GetWithModel(resource, nil, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	roles := make([]Role, len(resModel.Role))
	for i, model := range resModel.Role {
		role := NewRole(model.Name, sm.restClient, sm.projectName)
		role.model = model
		roles[i] = role
	}

	return roles, nil
}

func (sm *Manager) ListRolesForUserWithName(userName string) ([]Role, error) {
	return sm.listRolesForUser(userName, "displayname")
}

func (sm *Manager) ListRolesForUserWithId(userId, _type string) ([]Role, error) {
	return sm.listRolesForUser(userId, "")
}

func (sm *Manager) ListUsersForRole(roleName string) ([]User, error) {
	rb := sm.rb()
	resource := rb.Role(roleName)
	client := sm.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("usesrs", "")

	type ResModel struct {
		XMLName xml.Name    `xml:"Users"`
		User    []userModel `xml:"User"`
	}

	var resModel ResModel

	err := client.GetWithModel(resource, queryArgs, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	users := make([]User, len(resModel.User))
	for i, model := range resModel.User {
		user := NewUser(model.ID, sm.restClient, sm.projectName)
		user.model = model
		users[i] = user
	}

	return users, nil
}

func (sm *Manager) RunQuery(query string, jsonOutput bool, supervisionToken string) (string, error) {
	authIns, err := sm.Run(query, jsonOutput, supervisionToken)

	if err != nil {
		return "", err
	}

	return authIns.WaitForSuccess()
}

func (sm *Manager) Run(query string, jsonOutput bool, supervisionToken string) (*AuthQueryInstance, error) {
	rb := sm.rb()
	resource := rb.Authorization()
	client := sm.restClient

	type ReqBody struct {
		XMLName              xml.Name `xml:"Authorization"`
		Query                string
		ResponseInJsonFormat bool
	}

	type ResModel struct {
		XMLName xml.Name `xml:"Authorization"`
		Result  string
	}

	reqBody := ReqBody{
		Query:                query,
		ResponseInJsonFormat: jsonOutput,
	}
	var resModel ResModel
	var isAsync bool
	headers := make(map[string]string)
	if supervisionToken != "" {
		headers["odps-x-supervision-token"] = supervisionToken
	}

	err := client.DoXmlWithParseRes(common.HttpMethod.PostMethod, resource, nil, nil, reqBody, func(res *http.Response) error {
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return errors.WithStack(restclient.NewHttpNotOk(res))
		}

		isAsync = res.StatusCode != 200
		decoder := xml.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if _, ok := err.(restclient.HttpNotOk); ok {
		return nil, errors.WithStack(err)
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !isAsync {
		return newAuthQueryInstanceWithResult(resModel.Result), nil
	}

	return newAuthQueryInstance(sm, resModel.Result), nil
}

func (sm *Manager) GenerateAuthorizationToken(policy string) (string, error) {
	rb := sm.rb()
	resource := rb.Authorization()
	client := sm.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("sign_bearer_token", "")
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, strings.NewReader(policy), queryArgs)
	if err != nil {
		return "", errors.WithStack(err)
	}
	req.Header.Set(common.HttpHeaderContentType, "application/json")

	type ResModel struct {
		XMLName xml.Name `xml:"Authorization"`
		Result  string
	}
	var resModel ResModel

	err = client.DoWithModel(req, &resModel)
	if err != nil {
		return "", errors.WithStack(err)
	}

	return resModel.Result, nil
}

func (sm *Manager) rb() common.ResourceBuilder {
	return common.NewResourceBuilder(sm.projectName)
}
