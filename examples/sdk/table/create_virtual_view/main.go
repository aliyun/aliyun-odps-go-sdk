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

	// create [or replace] view [if not exists] <view_name>
	// [(<col_name> [comment <col_comment>], ...)]
	// [comment <view_comment>]
	// as <select_statement>;

	viewName := "testCreateVirtualView"

	c1 := tableschema.Column{
		Name: "string_type",
		Type: datatype.StringType,
	}

	c2 := tableschema.Column{
		Name: "date_type",
		Type: datatype.DateType,
	}

	c3 := tableschema.Column{
		Name: "int_type",
		Type: datatype.IntType,
	}

	sb := tableschema.NewSchemaBuilder()

	schema := sb.Name(viewName). // table name
		Columns(c1, c2, c3). // columns
		Comment("create Virtual view").
		Lifecycle(10).
		IsVirtualView(true).
		ViewText("select string_type,date_type,int_type from all_types_demo").
		Build()


	tablesIns := odpsIns.Tables()

	sql, _ := schema.ToViewSQLString(odpsIns.DefaultProjectName(), odpsIns.CurrentSchemaName(), true, true, false)
	fmt.Println(sql)

	err = tablesIns.CreateView(schema, true, true, false)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
