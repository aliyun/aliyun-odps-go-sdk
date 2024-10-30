package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"log"
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

	p, err := table.GetPartition("p1=20/p2=hangzhou")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Get the basic partition information
	fmt.Printf(
		"value=%s, createTime=%s, lastDDLTime=%s, lastModifiedTime=%s, size=%d\n",
		p.Value(), p.CreatedTime(), p.LastDDLTime(), p.LastModifiedTime(), p.Size(),
	)

	// Get the extended partition information
	err = p.LoadExtended()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	fmt.Printf(
		"isArchived=%t, lifeCycle=%d, physicalSize=%d",
		p.IsArchivedEx(), p.LifeCycleEx(), p.PhysicalSizeEx(),
	)
}
