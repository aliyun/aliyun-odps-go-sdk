package odps

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type SecurityManager struct {
	odpsIns        *Odps
	projectName    string
	securityConfig SecurityConfig
}

func NewSecurityManager(odpsIns *Odps, projectName ...string) SecurityManager {
	var _projectName string

	if len(projectName) > 0 {
		_projectName = projectName[0]
	} else {
		_projectName = odpsIns.DefaultProjectName()
	}

	return SecurityManager{
		odpsIns:        odpsIns,
		projectName:    _projectName,
		securityConfig: NewSecurityConfig(odpsIns, false, _projectName),
	}
}

func (sm *SecurityManager) GetSecurityConfig(withoutExceptionPolicy bool) (SecurityConfig, error) {
	if sm.securityConfig.BeLoaded() {
		return sm.securityConfig, nil
	}

	sm.securityConfig.withoutExceptionPolicy = withoutExceptionPolicy
	err := sm.securityConfig.Load()

	return sm.securityConfig, errors.WithStack(err)
}

func (sm *SecurityManager) SetSecurityConfig(config SecurityConfig, supervisionToken string) error {
	err := config.Update(supervisionToken)
	sm.securityConfig = config
	return errors.WithStack(err)
}

func (sm *SecurityManager) CheckPermissionV1(p Permission) (*PermissionCheckResult, error) {
	rb := sm.rb()
	resource := rb.Auth()
	client := sm.odpsIns.restClient

	type ResModel struct {
		XMLName xml.Name `xml:"Auth"`
		PermissionCheckResult
	}

	body, err := json.Marshal(p)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	req, err := client.NewRequest(HttpMethod.PostMethod, resource, bytes.NewReader(body))
	req.Header.Set(HttpHeaderContentType, "application/json")
	var resModel ResModel
	err = client.DoWithModel(req, &resModel)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &resModel.PermissionCheckResult, nil
}

func (sm *SecurityManager) CheckPermissionV0(
	objectType PermissionObjectType,
	objectName string,
	actionType PermissionActionType,
	columns []string,
) (*PermissionCheckResult, error) {

	rb := sm.rb()
	resource := rb.Auth()
	client := sm.odpsIns.restClient

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

func (sm *SecurityManager) getPolicy(resource, policyType string) ([]byte, error) {
	queryArgs := make(url.Values, 1)
	queryArgs.Set(policyType, "")
	client := sm.odpsIns.restClient
	var body []byte

	err := client.GetWithParseFunc(resource, queryArgs, func(res *http.Response) error {
		var err error
		body, err = ioutil.ReadAll(res.Body)
		return errors.WithStack(err)
	})

	return body, errors.WithStack(err)
}

func (sm *SecurityManager) setPolicy(resource, policyType string, policy string) error {
	queryArgs := make(url.Values, 1)
	queryArgs.Set(policyType, "")
	client := sm.odpsIns.restClient
	req, err := client.NewRequestWithUrlQuery(HttpMethod.PutMethod, resource, strings.NewReader(policy), queryArgs)

	if err != nil {
		return errors.WithStack(err)
	}

	if policyType == "security_policy" {
		req.Header.Set(HttpHeaderContentType, "application/json")
	}

	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

func (sm *SecurityManager) GetPolicy() ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Project(), "policy")
}

func (sm *SecurityManager) SetPolicy(policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Project(), "policy", policy)
}

func (sm *SecurityManager) GetSecurityPolicy() ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Project(), "security_policy")
}

func (sm *SecurityManager) SetSecurityPolicy(policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Project(), "security_policy", policy)
}

func (sm *SecurityManager) GetRolePolicy(roleName string) ([]byte, error) {
	rb := sm.rb()
	return sm.getPolicy(rb.Role(roleName), "policy")
}

func (sm *SecurityManager) SetRolePolicy(roleName, policy string) error {
	rb := sm.rb()
	return sm.setPolicy(rb.Role(roleName), "policy", policy)
}

func (sm *SecurityManager) ListUsers() ([]User, error) {
	rb := sm.rb()
	resource := rb.Users()
	client := sm.odpsIns.restClient

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
		user := NewUser(model.ID, sm.odpsIns, sm.projectName)
		user.model = model
		users[i] = user
	}

	return users, nil
}

func (sm *SecurityManager) ListRoles() ([]Role, error) {
	rb := sm.rb()
	resource := rb.Roles()
	client := sm.odpsIns.restClient

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
		role := NewRole(model.Name, sm.odpsIns, sm.projectName)
		role.model = model
		roles[i] = role
	}

	return roles, nil
}

func (sm *SecurityManager) listRolesForUser(userIdOrName, _type string) ([]Role, error) {
	rb := sm.rb()
	resource := rb.User(userIdOrName)
	client := sm.odpsIns.restClient
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
		role := NewRole(model.Name, sm.odpsIns, sm.projectName)
		role.model = model
		roles[i] = role
	}

	return roles, nil
}

func (sm *SecurityManager) ListRolesForUserWithName(userName, _type string) ([]Role, error) {
	return sm.listRolesForUser(userName, "displayname")
}

func (sm *SecurityManager) ListRolesForUserWithId(userId, _type string) ([]Role, error) {
	return sm.listRolesForUser(userId, "")
}

func (sm *SecurityManager) ListUsersForRole(roleName string) ([]User, error) {
	rb := sm.rb()
	resource := rb.Role(roleName)
	client := sm.odpsIns.restClient

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
		user := NewUser(model.ID, sm.odpsIns, sm.projectName)
		user.model = model
		users[i] = user
	}

	return users, nil
}

func (sm *SecurityManager) RunQuery(query string, jsonOutput bool, supervisionToken string) (string, error) {
	rb := sm.rb()
	resource := rb.Authorization()
	client := sm.odpsIns.restClient

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

	err := client.DoXmlWithParseRes(HttpMethod.PostMethod, resource, nil, reqBody, func(res *http.Response) error {
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return errors.WithStack(NewHttpNotOk(res))
		}

		isAsync = res.StatusCode != 200
		decoder := xml.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if httpNodeOk, ok := err.(HttpNotOk); ok {
		return string(httpNodeOk.Body), errors.WithStack(err)
	}

	if err != nil {
		return "", errors.WithStack(err)
	}

	if !isAsync {
		return resModel.Result, nil
	}

	authResource := rb.AuthorizationId(resModel.Result)
	var resModel1 ResModel
	err = client.GetWithModel(authResource, nil, &resModel1)
	return resModel1.Result, errors.WithStack(err)
}

func (sm *SecurityManager) GenerateAuthorizationToken(policy string) (string, error) {
	rb := sm.rb()
	resource := rb.Authorization()
	client := sm.odpsIns.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("sign_bearer_token", "")
	req, err := client.NewRequest(HttpMethod.PostMethod, resource, strings.NewReader(policy))
	if err != nil {
		return "", errors.WithStack(err)
	}
	req.Header.Set(HttpHeaderContentType, "application/json")

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

func (sm *SecurityManager) rb() ResourceBuilder {
	return NewResourceBuilder(sm.projectName)
}
