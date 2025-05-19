package odps_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

func TestLogView_GenerateLogViewAuto(t *testing.T) {
	odpsIns.SetDefaultProjectName(defaultProjectName)
	odpsIns.Options.LogViewVersion = options.Auto
	instance := odps.NewInstance(odpsIns, defaultProjectName, "20221027160000000001000001")
	logView := odps.NewLogView(odpsIns)
	url, err := logView.GenerateLogView(instance, 24)
	if err != nil {
		t.Error(err)
	}
	println(url)
}

func TestLogView_GenerateLogView(t *testing.T) {
	odpsIns.SetDefaultProjectName(defaultProjectName)
	odpsIns.Options.LogViewVersion = options.LegacyLogView
	instance := odps.NewInstance(odpsIns, defaultProjectName, "20221027160000000001000001")
	logView := odps.NewLogView(odpsIns)
	url, err := logView.GenerateLogView(instance, 24)
	if err != nil {
		t.Error(err)
	}
	println(url)
}

func TestLogView_GenerateJobInsight(t *testing.T) {
	odpsIns.SetDefaultProjectName(defaultProjectName)
	odpsIns.Options.LogViewVersion = options.JobInsight
	instance := odps.NewInstance(odpsIns, defaultProjectName, "20221027160000000001000001")
	logView := odps.NewLogView(odpsIns)
	url, err := logView.GenerateLogView(instance, 24)
	if err != nil {
		t.Error(err)
	}
	println(url)
	expect := "https://maxcompute.console.aliyun.com/cn-shanghai/job-insights?h=http%3A%2F%2Fservice.cn-shanghai.maxcompute.aliyun.com%2Fapi&i=20221027160000000001000001&p=go_sdk_regression_testing"
	if url != expect {
		t.Errorf("expect %s, but got %s", expect, url)
	}
}

func TestLogView_GenerateLogViewV2(t *testing.T) {
	odpsIns.SetDefaultProjectName(defaultProjectName)
	instance := odps.NewInstance(odpsIns, defaultProjectName, "20221027160000000001000001")
	logView := odps.NewLogView(odpsIns)
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
