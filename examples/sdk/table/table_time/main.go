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

	project := odpsIns.Project(conf.ProjectName)
	tables := project.Tables()
	table := tables.Get("all_types_demo")

	err = table.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Get table create time
	createTime := table.CreatedTime()
	println("create time = ", createTime)

	// Get table last ddl time
	lastDDLTime := table.LastDDLTime()
	println("last ddl time = ", lastDDLTime)

	// Get table last modified time
	lastModifiedTime := table.LastModifiedTime()
	println("last modified time = ", lastModifiedTime)
}
