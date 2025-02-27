package odps_test

import "testing"

func TestTenant_Load(t *testing.T) {
	tenant := odpsIns.Tenant()
	err := tenant.Load()
	if err != nil {
		t.Error(err)
	}
	println(tenant.GetName())
	println(tenant.GetCreationTime().String())
	println(tenant.GetLastModifiedTime().String())
}

func TestTenant_TenantRolePolicy(t *testing.T) {
	t.Skip("skip this tests, the impact of tenant test may be relatively large")
	manager := odpsIns.Projects().GetDefaultProject().SecurityManager()
	res, err := manager.Run("LIST TENANT ROLES", true, "")
	if err != nil {
		t.Error(err)
	}
	result, err := res.WaitForSuccess()
	if err != nil {
		t.Error(err)
	}
	println(result)

	policy, err := odpsIns.Tenant().GetTenantRolePolicy("aaa")
	if err != nil {
		t.Error(err)
	}
	println(policy)

	POLICY := "{\n" + "    \"Statement\": [{\n" + "            \"Action\": [\"odps:CreateQuota\",\n" + "                \"odps:Usage\"],\n" + "            \"Effect\": \"Allow\",\n" + "            \"Resource\": [\"acs:odps:*:quotas/*\"]},\n" + "        {\n" + "            \"Action\": [\"odps:CreateNetworkLink\",\n" + "                \"odps:Usage\"],\n" + "            \"Effect\": \"Allow\",\n" + "            \"Resource\": [\"acs:odps:*:networklinks/*\"]},\n" + "        {\n" + "            \"Action\": [\"odps:*\"],\n" + "            \"Effect\": \"Allow\",\n" + "            \"Resource\": [\"acs:odps:*:projects/*\"]}],\n" + "    \"Version\": \"1\"}"
	err = odpsIns.Tenant().PutTenantRolePolicy("aaa", POLICY)
	if err != nil {
		t.Error(err)
	}
	policy, err = odpsIns.Tenant().GetTenantRolePolicy("aaa")
	if err != nil {
		t.Error(err)
	}
	println(policy)
}
