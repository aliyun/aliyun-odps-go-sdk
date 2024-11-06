package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	// create config.ini file with content like the following
	/*
		    ; Inline comment is not allowed
		 	[odps]
			access_id =
			access_key =
			sts_token =
			endpoint =
			project =
	*/
	// Specify the ini file path
	configPath := ""
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	stsAccount := account.NewStsAccount(conf.AccessId, conf.AccessKey, conf.StsToken)
	odpsIns := odps.NewOdps(stsAccount, conf.Endpoint)
	// Set the Default Maxcompute project used by Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	fmt.Printf("odps:%#v\n", odpsIns)
}
