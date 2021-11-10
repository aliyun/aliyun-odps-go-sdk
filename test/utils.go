package testu

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
)


var DefaultAccount odps.Account
var DefaultOdpsHttpClient odps.RestClient

func init() {
	a := odps.AliyunAccountFromEnv()

	if a.Endpoint() == "" {
		panic("environment odps_endpoint is not set")
	}

	if a.AccessKey() == "" {
		panic("environment odps_accessKey is not set")
	}

	if a.AccessId() == "" {
		panic("environment odps_accessId is not set")
	}

	DefaultAccount = &a

	DefaultOdpsHttpClient = odps.NewOdpsHttpClient(DefaultAccount)
}