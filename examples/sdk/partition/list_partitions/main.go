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

	partitions, err := table.GetPartitions()

	if err != nil {
		log.Fatalf("%+v", err)
	}

	fmt.Printf("get %d partitions\n", len(partitions))

	for _, p := range partitions {
		fmt.Printf(
			"value=%s, createTime=%s, lastDDLTime=%s, lastModifiedTime=%s, size=%d\n",
			p.Value(), p.CreatedTime(), p.LastDDLTime(), p.LastModifiedTime(), p.Size(),
		)
	}
}
