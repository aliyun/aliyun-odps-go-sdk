package main

import (
	"fmt"
	"io"
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

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	csvReader, err := sqlTask.GetSelectResultAsCsv(ins, true)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%+v", err)
		}
		fmt.Printf("%v\n", record)
	}
}
