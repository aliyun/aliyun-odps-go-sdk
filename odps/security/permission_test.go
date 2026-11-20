// Licensed under the Apache License, Version 2.0.

package security_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
)

func TestPermissionWireNames(t *testing.T) {
	for _, tc := range []struct {
		typ  security.PermissionObjectType
		name string
		path string
	}{
		{security.ObjectTypeProject, "Project", "/projects/fixture"},
		{security.ObjectTypeTable, "Table", "/projects/fixture/tables/sample"},
		{security.ObjectTypeFunction, "Function", "/projects/fixture/registration/functions/sample"},
		{security.ObjectTypeResource, "Resource", "/projects/fixture/resources/sample"},
		{security.ObjectTypeInstance, "Instance", "/projects/fixture/instances/sample"},
		{security.PermissionObjectType(-1), "UnknownObjectType", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.name, tc.typ.String())
			permission := security.NewPermission("fixture", tc.typ, "sample", security.ActionTypeRead)
			require.Equal(t, tc.path, permission.Resource())
		})
	}

	for action, want := range map[security.PermissionActionType]string{
		security.ActionTypeRead: "Read", security.ActionTypeWrite: "Write", security.ActionTypeList: "List",
		security.ActionTypeCreateTable: "CreateTable", security.ActionTypeCreateInstance: "CreateInstance",
		security.ActionTypeCreateFunction: "CreateFunction", security.ActionTypeCreateResource: "CreateResource",
		security.ActionTypeAll: "All", security.ActionTypeDescribe: "Describe", security.ActionTypeSelect: "Select",
		security.ActionTypeAlter: "Alter", security.ActionTypeUpdate: "Update", security.ActionTypeDrop: "Drop",
		security.ActionTypeExecute: "Execute", security.ActionTypeDelete: "Delete", security.ActionTypeDownload: "Download",
		security.PermissionActionType(-1): "UnknownActionType",
	} {
		require.Equal(t, want, action.String())
	}
	require.Equal(t, "Allow", security.EffectAllow.String())
	require.Equal(t, "Deny", security.EffectDeny.String())
	require.Equal(t, "UnknownEffect", security.PermissionEffect(-1).String())
}

func TestPermissionCheckPayloadAndErrors(t *testing.T) {
	manager := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/projects/fixture/auth", r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			q := r.URL.Query()
			assert.Equal(t, "Table", q.Get("type"))
			assert.Equal(t, "Select", q.Get("grantee"))
			if q.Get("name") == "absent" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `<Error><Code>NoSuchObject</Code><Message>unknown table</Message></Error>`)
				return
			}
			assert.Equal(t, "id,note", q.Get("columns"))
		case http.MethodPost:
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			var body []map[string]string
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if assert.Len(t, body, 1) {
				assert.Equal(t, "Select", body[0]["Action"])
				if strings.HasSuffix(body[0]["Resource"], "/absent") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				assert.Equal(t, "/projects/fixture/tables/sample", body[0]["Resource"])
				assert.JSONEq(t, `["id","note"]`, body[0]["odps:SelectColumn"])
			}
		default:
			t.Errorf("unexpected permission method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = fmt.Fprint(w, "<Auth><Result>Allow</Result><Message>fixture</Message></Auth>")
	})

	columns := []string{"id", "note"}
	permission := security.NewPermission("fixture", security.ObjectTypeTable, "sample", security.ActionTypeSelect)
	permission.SetColumns(columns)
	v1, err := manager.CheckPermissionV1(permission)
	require.NoError(t, err)
	require.Equal(t, &security.PermissionCheckResult{Result: "Allow", Message: "fixture"}, v1)
	v0, err := manager.CheckPermissionV0(security.ObjectTypeTable, "sample", security.ActionTypeSelect, columns)
	require.NoError(t, err)
	require.Equal(t, v1, v0)

	permission.ObjectName = "absent"
	_, err = manager.CheckPermissionV1(permission)
	var response restclient.HttpError
	require.ErrorAs(t, err, &response)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	_, err = manager.CheckPermissionV0(security.ObjectTypeTable, "absent", security.ActionTypeSelect, nil)
	require.ErrorAs(t, err, &response)
	require.Equal(t, http.StatusNotFound, response.StatusCode)

	client := restclient.NewOdpsRestClient(account2.NewAliyunAccount("test-ak", "test-sk"), "http://%")
	unusable := security.NewSecurityManager(client, "fixture")
	_, err = unusable.CheckPermissionV1(permission)
	require.ErrorContains(t, err, "invalid URL")
}
