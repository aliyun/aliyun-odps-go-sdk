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

	sql := "select * from all_types_demo where p1>0 or p2 > '';"

	// The flags used by sql engine, such as odps.sql.skewjoin
	var hints map[string]string = nil

	// Create a SqlTask
	sqlTask := odps.NewSqlTask("select", sql, hints)

	// Run the sql with the quota associated with a project
	project := odpsIns.DefaultProjectName()
	ins, err := sqlTask.Run(odpsIns, project)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	logView, err := odpsIns.LogView().GenerateLogView(ins, 1)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(logView)
}
