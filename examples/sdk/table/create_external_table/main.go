package main

import (
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

	// create external table if not exists go_sdk_regression_testing.`testCreateExternalTableWithUserDefinedStorageHandler` (
	//    `a` STRING ,
	//    `b` STRING ,
	//    `c` BIGINT
	//)
	//	comment 'External table using user defined TextStorageHandler'
	//	partitioned by (`dt` STRING)
	//	stored by 'com.aliyun.odps.udf.example.text.TextStorageHandler'
	//	with serdeproperties('odps.text.option.delimiter'='|', 'my.own.option'='value')
	//	location 'MOCKoss://full/uri/path/to/oss/directory/'
	//	using 'odps-udf-example.jar, another.jar'
	//	lifecycle 10;

	tableName := "testCreateExternalTableWithUserDefinedStorageHandler"

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

	sb.Name(tableName). // table name
				Columns(c1, c2, c3).  // columns
				PartitionColumns(pc). // partition columns
				Location("MOCKoss://full/uri/path/to/oss/directory/").
				StorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler").
				Comment("External table using user defined TextStorageHandler").
				Lifecycle(10)

	tablesIns := odpsIns.Tables()

	schema := sb.Build()

	// 定义 jars
	jars := []string{
		"odps-udf-example.jar",
		"another.jar",
	}

	// 定义 properties 映射
	serDeProperties := map[string]string{
		"odps.text.option.delimiter": "|",
		"my.own.option":              "value",
	}

	// 定义 hints 映射
	hints := map[string]string{
		"odps.sql.preparse.odps2":       "lot",
		"odps.sql.planner.mode":         "lot",
		"odps.sql.planner.parser.odps2": "true",
		"odps.sql.ddl.odps2":            "true",
		"odps.compiler.output.format":   "lot,pot",
	}

	sql, err := schema.ToExternalSQLString(odpsIns.DefaultProjectName(), "", true, serDeProperties, jars)
	print(sql)

	err = tablesIns.CreateExternal(schema, true, serDeProperties, jars, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
