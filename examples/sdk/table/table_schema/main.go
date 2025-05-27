package main

import (
	"fmt"
	"log"
	"strings"

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

	project := odpsIns.Project(conf.ProjectName)
	tables := project.Tables()
	table := tables.Get("test_cluster_table")

	err = table.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Get table schema
	schema := table.Schema()

	println("table name = ", schema.TableName)
	if table.LifeCycle() > 0 {
		println("table lifecycle = ", table.LifeCycle())
	}

	// Get columns
	for _, c := range schema.Columns {
		fmt.Printf("column %s %s comment '%s'\n", c.Name, c.Type, c.Comment)
	}

	// Get partition columns
	for _, c := range schema.PartitionColumns {
		fmt.Printf("partition column %s %s comment '%s'\n", c.Name, c.Type, c.Comment)
	}

	// Get cluster information
	if schema.ClusterInfo.ClusterType != "" {
		ci := schema.ClusterInfo
		println("cluster type = ", ci.ClusterType)
		println("cluster columns = ", strings.Join(ci.ClusterCols, ", "))
		println("cluster bucket num = ", ci.BucketNum)
	}
}
