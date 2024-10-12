package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
)

type CustomCredentialProvider struct {
}

func (cp *CustomCredentialProvider) GetType() (*string, error) {
	s := "CustomProvider"
	return &s, nil
}

func (cp *CustomCredentialProvider) GetCredential() (*credentials.CredentialModel, error) {

	accessKeyId := ""
	accessKeySecurity := ""
	accessKeyToken := ""

	return &credentials.CredentialModel{
		AccessKeyId:     &accessKeyId,
		AccessKeySecret: &accessKeyToken,
		SecurityToken:   &accessKeySecurity,
	}, nil
}

func main() {
	provider := &CustomCredentialProvider{}

	stsAccount := account.NewStsAccountWithProvider(provider)

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
