package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
)

func main() {
	config := new(credentials.Config).
		// Which type of credential you want
		SetType("ecs_ram_role").
		// `roleName` is optional. It will be retrieved automatically if not set. It is highly recommended to set it up to reduce requests
		SetRoleName("RoleName").
		// `DisableIMDSv1` is optional and is recommended to be turned on. It can be replaced by setting environment variable: ALIBABA_CLOUD_IMDSV1_DISABLED
		SetDisableIMDSv1(true)

	credential, err := credentials.NewCredential(config)
	if err != nil {
		log.Fatalf("%+v", err)
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
