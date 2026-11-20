// Licensed under the Apache License, Version 2.0.

package security_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/projects/fixture", r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			assert.True(t, r.URL.Query().Has("security_configuration_without_exception_policy"))
			fmt.Fprint(w, `<SecurityConfiguration><SupportACL>true</SupportACL><AuthorizationVersion>2</AuthorizationVersion></SecurityConfiguration>`)
		case http.MethodPut:
			assert.True(t, r.URL.Query().Has("security_configuration"))
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Contains(t, string(body), "<EnableDownloadPrivilege>true</EnableDownloadPrivilege>")
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	config, err := manager.GetSecurityConfig(true)
	require.NoError(t, err)
	require.True(t, config.SupportAcl())
	require.Equal(t, "2", config.GetAuthorizationVersion())
	require.False(t, config.CheckDownloadPrivilege())
	config.EnableDownloadPrivilege()
	require.True(t, config.CheckDownloadPrivilege())
	require.NoError(t, manager.SetSecurityConfig(config, ""))
}

func TestSecurityManagerListsUsersAndRoles(t *testing.T) {
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		switch r.URL.Path {
		case "/projects/fixture/users":
			fmt.Fprint(w, `<Users><User><ID>42</ID><DisplayName>role-owner</DisplayName><Comment>first user</Comment></User></Users>`)
		case "/projects/fixture/roles":
			fmt.Fprint(w, `<Roles><Role><Name>reader</Name><Comment>first role</Comment></Role></Roles>`)
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
