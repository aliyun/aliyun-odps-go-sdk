package main

import (
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	// Specify the ini file path
	configPath := "./config.ini"
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	odpsIns.SetCurrentSchemaName("default")

	fb := odps.NewFunctionBuilder()
	fb.Name("test_sleep2")
	fb.ClassPath("com.aliyun.odps.observation.udf.jobinsight.Base64Decoder")
	fb.Resources([]string{"mc-observation-miscellaneous-1.0-SNAPSHOT.jar"})
	f := fb.Build()

	functions := odps.NewFunctions(odpsIns)
	err = functions.Update("", "", f)
	if err != nil {
		panic(err)
	}
}
