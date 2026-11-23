package offline_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

type testAccount struct{}

func (testAccount) SignRequest(*http.Request, string) error { return nil }
func (testAccount) GetType() account.Provider               { return account.Aliyun }

func TestTenantLoadAndRolePolicy(t *testing.T) {
	policy := `{"Version":"1","Statement":[]}`
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch {
		case r.URL.Path == "/tenants" && r.Method == http.MethodGet:
			io.WriteString(w, `{"Tenant":{"Name":"test","TenantId":"tenant-test","State":"NORMAL","Parameters":{"key":"value"}}}`)
		case r.URL.Path == "/tenants/_empty_tenant_/authorization/roles/reader" && r.Method == http.MethodGet:
			if _, ok := r.URL.Query()["policy"]; !ok {
				t.Error("policy query missing")
			}
			io.WriteString(w, `{"Policy":"policy-text"}`)
		case r.URL.Path == "/tenants/_empty_tenant_/authorization/roles/reader" && r.Method == http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			if string(b) != policy {
				t.Errorf("policy changed: %s", b)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	tenant := odps.NewOdps(testAccount{}, server.URL).Tenant()
	if err := tenant.Load(); err != nil {
		t.Fatal(err)
	}
	if tenant.GetName() != "test" || tenant.GetTenantId() != "tenant-test" || tenant.GetProperty("key") != "value" || tenant.GetState() != odps.StateNormal {
		t.Fatal("tenant properties not decoded")
	}
	p, err := tenant.GetTenantRolePolicy("reader")
	if err != nil || p != "policy-text" {
		t.Fatalf("get policy: %q %v", p, err)
	}
	if err := tenant.PutTenantRolePolicy("reader", policy); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("requests=%d", calls)
	}
}
