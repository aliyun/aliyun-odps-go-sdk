package main

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

	// build the schema of table whose ddl is
	// CREATE TABLE test_hash_clustering (a string, b string, c bigint)
	// PARTITIONED BY (dt string)
	// CLUSTERED BY (c)
	// SORTED by (c) INTO 1024 BUCKETS;

	c1 := tableschema.Column{
		Name: "a",
		Type: datatype.StringType,
	}

	c2 := tableschema.Column{
		Name: "b",
		Type: datatype.StringType,
	}

	c3 := tableschema.Column{
		Name: "c",
		Type: datatype.BigIntType,
	}

	// partition column
	pc := tableschema.Column{
		Name: "dt",
		Type: datatype.StringType,
	}

	sb := tableschema.NewSchemaBuilder()

	sb.Name("test_hash_clustering"). // table name
						Columns(c1, c2, c3).                        // columns
						PartitionColumns(pc).                       // partition columns
						ClusterType(tableschema.CLUSTER_TYPE.Hash). // hash clustering, required for cluster table
						ClusterColumns([]string{c3.Name}).          // cluster by these columns, required for cluster table
		// sort by these columns, optional for cluster table
		ClusterSortColumns([]tableschema.SortColumn{{Name: c3.Name, Order: tableschema.SORT_ORDER.ASC}}).
		ClusterBucketNum(1024) // bucket num, required for hash clustering

	tablesIns := odpsIns.Tables()

	schema := sb.Build()

	// print sql for debugging
	println(schema.ToSQLString("test_cluster", "", true))

	err = tablesIns.Create(schema, true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

}
