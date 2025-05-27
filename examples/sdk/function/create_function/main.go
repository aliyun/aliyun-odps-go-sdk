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

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	odpsIns.SetCurrentSchemaName("default")

	fb := odps.NewFunctionBuilder()
	fb.Name("test_sleep2")
	fb.ClassPath("org.example.Sleep")
	fb.Resources([]string{"udf-1.0-SNAPSHOT.jar"})
	f := fb.Build()

	functions := odps.NewFunctions(odpsIns)
	err = functions.Create("", "", f)
	if err != nil {
		panic(err)
	}
}
