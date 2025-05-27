package main

import (
	"fmt"
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

	functions := odps.NewFunctions(odpsIns)
	function, _ := functions.Get("test_sleep2")

	fmt.Println(function.Exist())
	err = functions.Delete("test_sleep2")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println(function.Exist())
}
