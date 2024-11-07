package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
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

// 	CREATE MATERIALIZED VIEW [IF NOT EXISTS][project_name.]<mv_name>
// [LIFECYCLE <days>]    --指定生命周期
// [BUILD DEFERRED]    -- 指定是在创建时只生成表结构，不生成数据
// [(<col_name> [COMMENT <col_comment>],...)]    --列注释
// [DISABLE REWRITE]    --指定是否用于改写
// [COMMENT 'table comment']    --表注释
// [PARTITIONED ON/BY (<col_name> [, <col_name>, ...])    --创建物化视图表为分区表
// [CLUSTERED BY|RANGE CLUSTERED BY (<col_name> [, <col_name>, ...])
//      [SORTED BY (<col_name> [ASC | DESC] [, <col_name> [ASC | DESC] ...])]
//                  INTO <number_of_buckets> BUCKETS]    --用于创建聚簇表时设置表的Shuffle和Sort属性
// [TBLPROPERTIES("compressionstrategy"="normal/high/extreme",    --指定表数据存储压缩策略
//                 "enable_auto_substitute"="true",    --指定当分区不存在时是否转化视图来查询
//                 "enable_auto_refresh"="true",    --指定是否开启自动刷新
//                 "refresh_interval_minutes"="120",    --指定刷新时间间隔
//                 "only_refresh_max_pt"="true"    --针对分区物化视图，只自动刷新源表最新分区
//                 )]
//    AS <select_statement>;

	viewName := "testCreateMaterializedView"

	pc:=[]tableschema.Column{
		{
			Name: "p1",
			Type: datatype.BigIntType,
		},{
			Name: "p2",
			Type: datatype.StringType,
		},
	}

	sb := tableschema.NewSchemaBuilder()

	schema := sb.Name(viewName). // table name
		Comment("create Materialized view").
		Lifecycle(10).
		PartitionColumns(pc...).
		IsMaterializedView(true).
		ViewText("select string_type,date_type,int_type,p1,p2 from all_types_demo where p1=20").
		MvProperty("enable_auto_refresh","true").
		MvProperty("refresh_interval_minutes","120").
		ClusterType(tableschema.CLUSTER_TYPE.Hash).
		ClusterColumns([]string{"string_type"}).
		ClusterSortColumns([]tableschema.SortColumn{{Name: "date_type", Order: "asc"}}).
		ClusterBucketNum(1024).
		Build()


	tablesIns := odpsIns.Tables()

	sql, _ := schema.ToViewSQLString(odpsIns.DefaultProjectName(), odpsIns.CurrentSchemaName(), true, true, false)
	fmt.Println(sql)

	err = tablesIns.CreateView(schema, true, true, false)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
