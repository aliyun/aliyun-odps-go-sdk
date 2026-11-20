// Licensed under the Apache License, Version 2.0.

package security_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
)

func testManager(t *testing.T, handler http.HandlerFunc) security.Manager {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := restclient.NewOdpsRestClient(account2.NewAliyunAccount("test-ak", "test-sk"), server.URL)
	t.Cleanup(client.CloseIdleConnections)
	return security.NewSecurityManager(client, "fixture")
}

func TestSecurityManagerPolicyRoundTrip(t *testing.T) {
	const policy = `{"Version":"1","Statement":[]}`
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		requireKey := r.URL.Query().Has("policy") || r.URL.Query().Has("security_policy")
		assert.True(t, requireKey)
		assert.Contains(t, r.Header.Get("Authorization"), "test-ak")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, policy)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.JSONEq(t, policy, string(body))
			if r.URL.Query().Has("security_policy") {
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	projectPolicy, err := manager.GetPolicy()
	require.NoError(t, err)
	require.JSONEq(t, policy, string(projectPolicy))
	require.NoError(t, manager.SetPolicy(policy))
	securityPolicy, err := manager.GetSecurityPolicy()
	require.NoError(t, err)
	require.JSONEq(t, policy, string(securityPolicy))
	require.NoError(t, manager.SetSecurityPolicy(policy))
	rolePolicy, err := manager.GetRolePolicy("reader")
	require.NoError(t, err)
	require.JSONEq(t, policy, string(rolePolicy))
	require.NoError(t, manager.SetRolePolicy("reader", policy))
}

func TestSecurityManagerLoadsAndUpdatesConfig(t *testing.T) {
	var fullReads, redactedReads atomic.Int32
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/projects/fixture", r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Has("security_configuration_without_exception_policy") {
				redactedReads.Add(1)
				fmt.Fprint(w, `<SecurityConfiguration><SupportACL>true</SupportACL><AuthorizationVersion>2</AuthorizationVersion></SecurityConfiguration>`)
				return
			}
			assert.True(t, r.URL.Query().Has("security_configuration"))
			if fullReads.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, `<SecurityConfiguration><ProjectProtection><Protected>true</Protected><Exceptions>fixture</Exceptions></ProjectProtection></SecurityConfiguration>`)
		case http.MethodPut:
			assert.True(t, r.URL.Query().Has("security_configuration"))
			assert.Equal(t, "fixture-supervision", r.Header.Get("odps-x-supervision-token"))
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Contains(t, string(body), "<EnableDownloadPrivilege>true</EnableDownloadPrivilege>")
			assert.Contains(t, string(body), "<SecurityConfiguration>")
			assert.Contains(t, string(body), "<ProjectProtection Protected=\"false\">")
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	config, err := manager.GetSecurityConfig(true)
	require.NoError(t, err)
	require.True(t, config.BeLoaded())
	require.True(t, config.SupportAcl())
	require.Equal(t, "2", config.GetAuthorizationVersion())
	cached, err := manager.GetSecurityConfig(true)
	require.NoError(t, err)
	require.True(t, cached.BeLoaded())
	require.Equal(t, int32(1), redactedReads.Load())

	config, err = manager.GetSecurityConfig(false)
	require.Error(t, err)
	require.False(t, config.BeLoaded())
	config, err = manager.GetSecurityConfig(false)
	require.NoError(t, err)
	require.True(t, config.ProjectProtection())
	require.Equal(t, "fixture", config.ProjectProtectionExceptionPolicy())
	require.Equal(t, int32(2), fullReads.Load())

	config.DisableProjectProtection()
	require.False(t, config.ProjectProtection())
	require.Empty(t, config.ProjectProtectionExceptionPolicy())
	require.False(t, config.CheckDownloadPrivilege())
	config.EnableDownloadPrivilege()
	require.True(t, config.CheckDownloadPrivilege())
	require.NoError(t, manager.SetSecurityConfig(config, "fixture-supervision"))
	updated, err := manager.GetSecurityConfig(false)
	require.NoError(t, err)
	require.False(t, updated.ProjectProtection())
	require.True(t, updated.CheckDownloadPrivilege())
	require.Equal(t, int32(2), fullReads.Load())
}

func TestSecurityConfigurationUsesProtectedAttribute(t *testing.T) {
	var updates atomic.Int32
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, r.URL.Query().Has("security_configuration"))
		if r.Method == http.MethodGet {
			fmt.Fprint(w, `<SecurityConfiguration><ProjectProtection Protected="true"><Exceptions>policy</Exceptions></ProjectProtection></SecurityConfiguration>`)
			return
		}
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Empty(t, r.Header.Get("odps-x-supervision-token"))
		var body struct {
			XMLName           xml.Name `xml:"SecurityConfiguration"`
			ProjectProtection struct {
				Protected  string `xml:"Protected,attr"`
				Exceptions string
			}
		}
		assert.NoError(t, xml.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "false", body.ProjectProtection.Protected)
		assert.Empty(t, body.ProjectProtection.Exceptions)
		if updates.Add(1) == 2 {
			w.WriteHeader(http.StatusForbidden)
		}
	})
	config, err := manager.GetSecurityConfig(false)
	require.NoError(t, err)
	require.True(t, config.ProjectProtection())
	require.Equal(t, "policy", config.ProjectProtectionExceptionPolicy())
	config.DisableProjectProtection()
	require.NoError(t, manager.SetSecurityConfig(config, ""))
	require.Error(t, manager.SetSecurityConfig(config, ""))
}

func TestSecurityRequestsPropagateInvalidEndpointAndTokenRejection(t *testing.T) {
	client := restclient.NewOdpsRestClient(account2.NewAliyunAccount("id", "secret"), "http://%invalid")
	manager := security.NewSecurityManager(client, "fixture")
	permission := security.Permission{ProjectName: "fixture", ObjectType: security.ObjectTypeTable,
		ObjectName: "sample", ActionType: security.ActionTypeSelect}
	permission.SetColumns([]string{"id"})
	require.Equal(t, `["id"]`, permission.Params["odps:SelectColumn"])
	_, err := manager.CheckPermissionV1(permission)
	require.ErrorContains(t, err, "invalid URL escape")
	require.ErrorContains(t, manager.SetPolicy("{}"), "invalid URL escape")
	_, err = manager.GenerateAuthorizationTokenContext(context.Background(), "{}")
	require.ErrorContains(t, err, "invalid URL escape")
	config := security.NewSecurityConfig(client, false, "fixture")
	require.ErrorContains(t, config.Update(""), "invalid URL escape")

	rejecting := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, r.URL.Query().Has("sign_bearer_token"))
		w.WriteHeader(http.StatusForbidden)
	})
	_, err = rejecting.GenerateAuthorizationToken("{}")
	var response restclient.HttpError
	require.ErrorAs(t, err, &response)
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func TestSecurityManagerListsUsersAndRoles(t *testing.T) {
	const usersXML = `<Users><User><ID>42</ID><DisplayName>role-owner</DisplayName><Comment>first user</Comment></User></Users>`
	const rolesXML = `<Roles><Role><Name>reader</Name><Comment>first role</Comment></Role></Roles>`
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		q := r.URL.Query()
		switch r.URL.Path {
		case "/projects/fixture/users":
			fmt.Fprint(w, usersXML)
		case "/projects/fixture/roles":
			fmt.Fprint(w, rolesXML)
		case "/projects/fixture/users/role-owner":
			assert.True(t, q.Has("roles"))
			assert.Equal(t, "displayname", q.Get("type"))
			fmt.Fprint(w, rolesXML)
		case "/projects/fixture/users/42":
			if q.Has("roles") {
				assert.Empty(t, q.Get("type"))
				fmt.Fprint(w, rolesXML)
			} else {
				fmt.Fprint(w, `<User><ID>42</ID><DisplayName>role-owner</DisplayName><Comment>updated user</Comment></User>`)
			}
		case "/projects/fixture/roles/reader":
			if q.Has("users") {
				fmt.Fprint(w, usersXML)
			} else {
				fmt.Fprint(w, `<Role><Name>reader</Name><Comment>updated role</Comment></Role>`)
			}
		case "/projects/fixture/roles/missing", "/projects/fixture/users/missing":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})

	users, err := manager.ListUsers()
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Equal(t, "42", users[0].ID())
	require.Equal(t, "role-owner", users[0].DisplayName())
	require.Equal(t, "first user", users[0].Comment())
	roles, err := manager.ListRoles()
	require.NoError(t, err)
	require.Len(t, roles, 1)
	require.Equal(t, "reader", roles[0].Name())
	require.Equal(t, "first role", roles[0].Comment())
	require.NoError(t, roles[0].Load())
	require.Equal(t, "updated role", roles[0].Comment())
	require.NoError(t, users[0].Load())
	require.Equal(t, "updated user", users[0].Comment())

	byName, err := manager.ListRolesForUserWithName("role-owner")
	require.NoError(t, err)
	require.Len(t, byName, 1)
	require.Equal(t, "reader", byName[0].Name())
	byID, err := manager.ListRolesForUserWithId("42", "")
	require.NoError(t, err)
	require.Len(t, byID, 1)
	require.Equal(t, "reader", byID[0].Name())
	members, err := manager.ListUsersForRole("reader")
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "42", members[0].ID())
	_, err = manager.ListRolesForUserWithName("missing")
	require.Error(t, err)
	_, err = manager.ListUsersForRole("missing")
	require.Error(t, err)
}

func TestAuthorizationQueryLifecycle(t *testing.T) {
	var polls atomic.Int32
	const policy = `{"Version":"1","Statement":[]}`
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/projects/fixture/authorization":
			assert.Equal(t, http.MethodPost, r.Method)
			if r.URL.Query().Has("sign_bearer_token") {
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, policy, string(body))
				fmt.Fprint(w, `<Authorization><Result>fixture-token</Result></Authorization>`)
				return
			}
			var request struct {
				Query                string
				ResponseInJsonFormat bool
			}
			assert.NoError(t, xml.NewDecoder(r.Body).Decode(&request))
			switch request.Query {
			case "fixture synchronous":
				assert.True(t, request.ResponseInJsonFormat)
				assert.Equal(t, "fixture-supervision", r.Header.Get("odps-x-supervision-token"))
				fmt.Fprint(w, `<Authorization><Result>{"authorized":true}</Result></Authorization>`)
			case "fixture asynchronous":
				assert.False(t, request.ResponseInJsonFormat)
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, `<Authorization><Result>pending</Result></Authorization>`)
			case "fixture unsuccessful":
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, `<Authorization><Result>failed</Result></Authorization>`)
			case "fixture missing":
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, `<Authorization><Result>missing</Result></Authorization>`)
			default:
				w.WriteHeader(http.StatusForbidden)
			}
		case "/projects/fixture/authorization/pending":
			assert.Equal(t, http.MethodGet, r.Method)
			if polls.Add(1) == 1 {
				fmt.Fprint(w, `<AuthorizationQuery><Status>Running</Status></AuthorizationQuery>`)
			} else {
				fmt.Fprint(w, `<AuthorizationQuery><Status>Terminated</Status><Result>complete</Result></AuthorizationQuery>`)
			}
		case "/projects/fixture/authorization/failed":
			fmt.Fprint(w, `<AuthorizationQuery><Status>Failed</Status><Result>fixture denied</Result></AuthorizationQuery>`)
		case "/projects/fixture/authorization/missing":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})
	result, err := manager.RunQuery("fixture synchronous", true, "fixture-supervision")
	require.NoError(t, err)
	require.JSONEq(t, `{"authorized":true}`, result)
	result, err = manager.RunQuery("fixture asynchronous", false, "")
	require.NoError(t, err)
	require.Equal(t, "complete", result)
	require.Equal(t, int32(2), polls.Load())
	_, err = manager.RunQuery("fixture unsuccessful", false, "")
	require.ErrorContains(t, err, "fixture denied")
	_, err = manager.RunQuery("fixture missing", false, "")
	require.Error(t, err)
	_, err = manager.RunQuery("fixture forbidden", false, "")
	require.Error(t, err)
	token, err := manager.GenerateAuthorizationToken(policy)
	require.NoError(t, err)
	require.Equal(t, "fixture-token", token)
}

func TestSecurityManagerPreservesServiceErrors(t *testing.T) {
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `<Error><Code>NoPermission</Code><Message>denied</Message></Error>`)
	})
	_, configErr := manager.GetSecurityConfig(false)
	_, rolesErr := manager.ListRoles()
	_, usersErr := manager.ListUsers()
	_, policyErr := manager.GetPolicy()
	for _, err := range []error{configErr, rolesErr, usersErr, policyErr, manager.SetPolicy("{}")} {
		var response restclient.HttpError
		require.ErrorAs(t, err, &response)
		require.Equal(t, http.StatusForbidden, response.StatusCode)
		require.NotNil(t, response.ErrorMessage)
		require.Equal(t, "NoPermission", response.ErrorMessage.ErrorCode)
	}
}
