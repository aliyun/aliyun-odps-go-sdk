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

package security_test

import (
	"fmt"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
	"log"
)

var account = account2.AliyunAccountFromEnv()
var endpoint = restclient.LoadEndpointFromEnv()
var restClient = restclient.NewOdpsRestClient(account, endpoint)

func ExampleManager_GetSecurityConfig() {
	sm := security.NewSecurityManager(restClient, "project_1")
	sc, err := sm.GetSecurityConfig(true)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%+v", sc))

	// Output:
}

func ExampleManager_CheckPermissionV1() {
	sm := security.NewSecurityManager(restClient, "project_1")
	p := security.NewPermission(
		"project_1",
		security.ObjectTypeTable,
		"sale_detail",
		security.ActionTypeAll,
	)
	p.Params["User"] = "Aliyun$odpstest1@aliyun.com;"

	r, err := sm.CheckPermissionV1(p)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%v", r))
	// Output:

}

func ExampleManager_CheckPermissionV0() {
	sm := security.NewSecurityManager(restClient, "project_1")

	r, err := sm.CheckPermissionV0(
		security.ObjectTypeTable,
		"sale_detail",
		security.ActionTypeAll,
		nil,
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%v", r))
	// Output:
}

func ExampleManager_GetPolicy() {
	sm := security.NewSecurityManager(restClient, "project_1")
	policy, err := sm.GetPolicy()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(policy)
	// Output:
}

func ExampleManager_ListUsers() {
	sm := security.NewSecurityManager(restClient, "project_1")
	users, err := sm.ListUsers()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, user := range users {
		println(fmt.Sprintf("id=%s, name=%s", user.ID(), user.DisplayName()))
	}

	// Output:
}

func ExampleManager_RunQuery() {
	sm := security.NewSecurityManager(restClient, "project_1")
	result, err := sm.RunQuery("show grants for aliyun$odpstest1@aliyun.com;", true, "")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("ok: %s", result))

	// Output:
}
