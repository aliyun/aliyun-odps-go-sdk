package odps_test

import (
	account2 "github.com/aliyun/aliyun-odps-go-sdk/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/rest_client"
)

var account = account2.AliyunAccountFromEnv()
var endpoint = rest_client.LoadEndpointFromEnv()
var odpsIns = odps.NewOdps(account, endpoint)

func init() {
	if account.AccessId() == "" {
		panic("account environments are not set")
	}
	odpsIns.SetDefaultProjectName("odps_smoke_test")
	//odpsIns.SetDefaultProjectName("project_1")
}
