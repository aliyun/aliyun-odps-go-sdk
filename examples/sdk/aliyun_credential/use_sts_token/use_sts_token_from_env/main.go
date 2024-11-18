package main

import (
	"fmt"

	"github.com/aliyun/credentials-go/credentials"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	// Read ak from environment variables: "ALIBABA_CLOUD_ACCESS_KEY_ID",
	// "ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABA_CLOUD_SECURITY_TOKEN"
	credential, err := credentials.NewCredential(nil)
	if err != nil {
		return
	}

	aliyunAccount := account.NewStsAccountWithCredential(credential)

	// Get the endpoint of a specific region from https://www.alibabacloud.com/help/en/maxcompute/user-guide/endpoints
	// 获取具体的endpoint请参考 https://help.aliyun.com/zh/maxcompute/user-guide/endpoints
	// The following an example endpoint of cn-hangzhou
	endpoint := "http://service.cn-hangzhou.maxcompute.aliyun.com/api"

	// The Default Maxcompute project used by Odps instance
	defaultProject := ""

	odpsIns := odps.NewOdps(aliyunAccount, endpoint)
	odpsIns.SetDefaultProjectName(defaultProject)

	fmt.Printf("odps:%#v\n", odpsIns)
}
