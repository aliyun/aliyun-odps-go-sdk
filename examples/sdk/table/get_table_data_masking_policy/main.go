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

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	project := odpsIns.Project(conf.ProjectName)
	tables := project.Tables()
	table := tables.Get("personal_info")

	err = table.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	maskInfos, err := table.ColumnMaskInfos()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, maskInfo := range maskInfos {
		fmt.Println("column name = ", maskInfo.Name)
		fmt.Println("column policy name = ", maskInfo.PolicyNameList)
	}

}
