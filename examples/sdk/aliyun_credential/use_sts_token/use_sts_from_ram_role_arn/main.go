package main

import (
	"fmt"
	"log"

	"github.com/aliyun/credentials-go/credentials"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	// Get your AK frist
	accessKeyId := ""
	accessKeySecret := ""

	config := new(credentials.Config).
		// Which type of credential you want
		SetType("ram_role_arn").
		// AccessKeyId of your account
		SetAccessKeyId(accessKeyId).
		// AccessKeySecret of your account
		SetAccessKeySecret(accessKeySecret).
		// Format: acs:ram::USER_Id:role/ROLE_NAME
		SetRoleArn("RoleArn").
		// Role Session Name
		SetRoleSessionName("RoleSessionName").
		// Not required, limit the permissions of STS Token
		SetPolicy("Policy").
		// Not required, limit the Valid time of STS Token
		SetRoleSessionExpiration(3600)

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
