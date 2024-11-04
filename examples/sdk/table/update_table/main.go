package update_table

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
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
	table := tables.Get("test_table")

	// Rename Table
	err = table.Rename("test_table_new")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change Owner
	err = table.ChangeOwner("ALIYUN$xxx@aliyun.com")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change Comment
	err = table.ChangeComment("This's comment.")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Touch (update LastModifiedTime)
	err = table.Touch()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change Cluster Info
	err = table.ChangeClusterInfo(tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Hash,
		ClusterCols: []string{"col1"},
		SortCols: []tableschema.SortColumn{
			{Name: "sort_col1", Order: tableschema.SORT_ORDER.ASC},
			{Name: "sort_col2", Order: tableschema.SORT_ORDER.DESC},
		},
		BucketNum: 10,
	})
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Add Columns
	c1 := tableschema.Column{
		Name:    "c1",
		Type:    datatype.BigIntType,
		Comment: "c1 is bigint",
	}
	c2 := tableschema.Column{
		Name:    "c2",
		Type:    datatype.StringType,
		Comment: "c2 is string",
	}
	newColumns := []tableschema.Column{c1, c2}

	err = table.AddColumns(newColumns, true)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Drop Columns
	err = table.DropColumns([]string{"c1", "c2"})
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change Column Name
	err = table.ChangeColumnName("c1", "c1_new")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change Column Type
	err = table.AlterColumnType("c1", datatype.StringType)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Truncate Table
	err = table.Truncate()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
