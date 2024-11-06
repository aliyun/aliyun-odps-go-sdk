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
	"log"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
)

var (
	account     = account2.AccountFromEnv()
	endpoint    = restclient.LoadEndpointFromEnv()
	restClient  = restclient.NewOdpsRestClient(account, endpoint)
	projectName = "go_sdk_regression_testing"
)

func ExampleManager_GetSecurityConfig() {
	sm := security.NewSecurityManager(restClient, projectName)
	sc, err := sm.GetSecurityConfig(true)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%+v", sc))

	// Output:
}

func ExampleManager_CheckPermissionV1() {
	sm := security.NewSecurityManager(restClient, projectName)
	p := security.NewPermission(
		projectName,
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
	sm := security.NewSecurityManager(restClient, projectName)

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
	sm := security.NewSecurityManager(restClient, projectName)
	policy, err := sm.GetPolicy()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(policy)
	// Output:
}

func ExampleManager_ListUsers() {
	sm := security.NewSecurityManager(restClient, projectName)
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
	sm := security.NewSecurityManager(restClient, projectName)
	result, err := sm.RunQuery("whoami;", true, "")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("ok: %s", result))

	// Output:
}
