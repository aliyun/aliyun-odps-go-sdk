package main

import (
	"log"
	"os"

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
	resources := odps.NewResources(odpsIns)

	file, err := os.Open("xxxx")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fr := odps.NewFileResource("bank_data")
	fr.SetReader(file)

	err = resources.UpdateFileResource("", "", fr)
	if err != nil {
		log.Fatal(err)
	}
}
