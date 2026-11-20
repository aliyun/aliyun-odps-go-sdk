// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestTenantMetadataWireContract(t *testing.T) {
	for _, test := range []struct {
		name  string
		body  string
		state odps.TenantState
		err   string
	}{
		{name: "active", body: `{"Tenant":{"Name":"fixture","State":"normal","OwnerId":"owner","TenantId":"123","CreateTime":60,"UpdateTime":120,"Parameters":{"flag":"on"}}}`, state: odps.StateNormal},
		{name: "suspended", body: `{"Tenant":{"State":"SUSPENDED"}}`, state: odps.StateSuspended},
		{name: "deleting", body: `{"Tenant":{"State":"DELETING"}}`, state: odps.StateDeleting},
		{name: "deleted", body: `{"Tenant":{"State":"DELETED"}}`, state: odps.StateDeleted},
		{name: "unknown state", body: `{"Tenant":{"State":"PENDING"}}`, err: "invalid tenant state"},
		{name: "wrong state type", body: `{"Tenant":{"State":5}}`, err: "cannot unmarshal number"},
		{name: "missing tenant", body: `{"Tenant":null}`, err: "missing Tenant"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/api/tenants", r.URL.Path)
				_, _ = io.WriteString(w, test.body)
			})
			tenant := service.Tenant()
			if test.err != "" {
				require.ErrorContains(t, tenant.Load(), test.err)
				require.Equal(t, "", tenant.GetName())
				return
			}
			require.NoError(t, tenant.Load())
			require.Equal(t, test.state, tenant.GetState())
			if test.name == "active" {
				require.Equal(t, "fixture", tenant.GetName())
				require.Equal(t, "owner", tenant.GetOwnerId())
				require.Equal(t, "123", tenant.GetTenantId())
				require.Equal(t, int64(60), tenant.GetCreationTime().Unix())
				require.Equal(t, int64(120), tenant.GetLastModifiedTime().Unix())
				require.Equal(t, "on", tenant.GetProperty("flag"))
				require.Empty(t, tenant.GetProperty("unknown"))
			}
		})
	}
}

func TestTenantRolePolicyRequests(t *testing.T) {
	const policy = `{"Version":"1","Statement":[]}`
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, r.URL.Query().Has("policy"))
		switch r.URL.Path {
		case "/api/tenants/_empty_tenant_/authorization/roles/fixture":
			switch r.Method {
			case http.MethodPut:
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, policy, string(body))
				w.WriteHeader(http.StatusNoContent)
			case http.MethodGet:
				assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"Policy": policy}))
			default:
				t.Errorf("unexpected method %s", r.Method)
			}
		case "/api/tenants/_empty_tenant_/authorization/roles/missing":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected role path %s", r.URL.Path)
		}
	})
	tenant := service.Tenant()
	require.NoError(t, tenant.PutTenantRolePolicy("fixture", policy))
	got, err := tenant.GetTenantRolePolicy("fixture")
	require.NoError(t, err)
	require.JSONEq(t, policy, got)
	_, err = tenant.GetTenantRolePolicy("missing")
	require.ErrorContains(t, err, "get tenant role policy failed")
	_, err = tenant.GetTenantRolePolicy("")
	require.ErrorContains(t, err, "roleName")
	require.ErrorContains(t, tenant.PutTenantRolePolicy("", policy), "roleName")
	require.ErrorContains(t, tenant.PutTenantRolePolicy("fixture", ""), "policy")
}
