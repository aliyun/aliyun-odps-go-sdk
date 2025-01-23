package odps

import (
	"testing"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

var (
	account            = account2.AccountFromEnv()
	endpoint           = restclient.LoadEndpointFromEnv()
	odpsIns            = NewOdps(account, endpoint)
	defaultProjectName = "go_sdk_regression_testing"
)

func TestLogView_GenerateLogViewV2(t *testing.T) {
	odpsIns.SetDefaultProjectName(defaultProjectName)
	instance := NewInstance(odpsIns, defaultProjectName, "20221027160000000001000001")
	logView := NewLogView(odpsIns)
	url, err := logView.GenerateLogViewV2(instance, "cn-shanghai")
	if err != nil {
		t.Error(err)
	}
	println(url)
	expect := "https://maxcompute.console.aliyun.com/cn-shanghai/job-insights?h=http://service.cn-shanghai.maxcompute.aliyun.com/api&p=go_sdk_regression_testing&i=20221027160000000001000001"
	if url != expect {
		t.Errorf("expect %s, but got %s", expect, url)
	}
}
