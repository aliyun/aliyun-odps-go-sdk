package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
)

func ExampleSecurityManager_GetSecurityConfig() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")
	sc, err := sm.GetSecurityConfig(true)
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%+v", sc))
	}

	// Output:
}

func ExampleSecurityManager_CheckPermissionV1() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")
	p := odps.NewPermission("project_1", odps.ObjectTypeTable, "sale_detail", odps.ActionTypeAll)
	p.Params["User"] = "Aliyun$odpstest1@aliyun.com;"

	r, err := sm.CheckPermissionV1(p)
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%v", r))
	}

	// Output:

}

func ExampleSecurityManager_CheckPermissionV0() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")

	r, err := sm.CheckPermissionV0(odps.ObjectTypeTable, "sale_detail", odps.ActionTypeAll, nil)
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%v", r))
	}

	// Output:
}

func ExampleSecurityManager_GetPolicy() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")
	policy, err := sm.GetPolicy()
	if err != nil {
		println(err.Error())
	} else {
		println(policy)
	}

	// Output:
}

func ExampleSecurityManager_ListUsers() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")
	users, err := sm.ListUsers()
	if err != nil {
		println(err.Error())
	} else {
		for _, user := range users {
			println(fmt.Sprintf("id=%s, name=%s", user.ID(), user.DisplayName()))
		}
	}

	// Output:
}

func ExampleSecurityManager_RunQuery() {
	sm := odps.NewSecurityManager(odpsIns, "project_1")
	result, err := sm.RunQuery("show grants for aliyun$odpstest1@aliyun.com;", true, "")
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("ok: %s", result))
	}

	// Output:

}