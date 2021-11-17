package odps_test

import odps "github.com/aliyun/aliyun-odps-go-sdk"

var account = odps.AliyunAccountFromEnv()
var endpoint = odps.LoadEndpointFromEnv()
var odpsIns = odps.NewOdps(&account, endpoint)

func init()  {
	if account.AccessId() == "" {
		panic("account environments are not set")
	}
	odpsIns.SetDefaultProjectName("odps_smoke_test")
}