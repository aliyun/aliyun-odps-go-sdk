package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
)

func main() {
	config := new(credentials.Config).SetType("credentials_uri").SetURLCredential("http://127.0.0.1")
	credential, err := credentials.NewCredential(config)
	if err != nil {
		return
	}

	stsAccount := account.NewStsAccountWithCredential(credential)

	// Get the endpoint of a specific region from https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints
	// 获取具体的endpoint请参考 https://help.aliyun.com/zh/maxcompute/user-guide/endpoints
	// The following an example endpoint of cn-hangzhou
	endpoint := "http://service.cn-hangzhou.maxcompute.aliyun.com/api"

	// The Default Maxcompute project used by Odps instance
	defaultProject := ""

	odpsIns := odps.NewOdps(stsAccount, endpoint)
	odpsIns.SetDefaultProjectName(defaultProject)

	fmt.Printf("odps:%#v\n", odpsIns)
}
