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

	project := odpsIns.Project(conf.ProjectName)
	tables := project.Tables()
	table := tables.Get("testcreatevirtualview")

	err = table.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Rename View
	err = table.Rename("test_view_new")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Change View Owner
	err = table.ChangeOwner("ALIYUN$xxx@aliyun.com")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	//  You can also use create or replace view to change the view
	// create [or replace] view [if not exists] <view_name>
	// [(<col_name> [comment <col_comment>], ...)]
	// [comment <view_comment>]
	// as <select_statement>;

	viewName := "test_view_new"

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

	c4 := tableschema.Column{
		Name: "tiny_int_type",
		Type: datatype.TinyIntType,
	}

	c5 := tableschema.Column{
		Name: "bigint_type",
		Type: datatype.BigIntType,
	}

	sb := tableschema.NewSchemaBuilder()

	schema := sb.Name(viewName). // table name
					Columns(c1, c2, c3, c4, c5). // columns
					Comment("create Virtual view").
					Lifecycle(10).
					IsVirtualView(true).
					ViewText("select string_type,date_type,int_type,tiny_int_type,bigint_type from all_types_demo").
					Build()

	tablesIns := odpsIns.Tables()

	odpsIns.SetCurrentSchemaName("default")

	sql, _ := schema.ToViewSQLString(odpsIns.DefaultProjectName(), odpsIns.CurrentSchemaName(), true, true, false)
	fmt.Println(sql)

	err = tablesIns.CreateView(schema, true, true, false)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
