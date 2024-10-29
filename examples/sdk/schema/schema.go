package schema

import (
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

	// schemas means all Schema in default project
	schemas := odpsIns.Schemas()
	// show all schemas in default project
	schemas.List(func(schema *odps.Schema, err error) {
		print(schema.Name() + "\n")
	})

	// You can set the current schema
	// Direct operations on the table, if no schema is specified, this schema will be used
	odpsIns.SetCurrentSchemaName("default_schema")
	table := odpsIns.Table("table") // actually, the table name is "project.default_schema.table"
	print(table.SchemaName())

	// You can get the tables in the specified schema
	tablesInSchemaA := odps.NewTables(odpsIns, conf.ProjectName, "schema_A")

	// show all tables in "schema_A"
	tablesInSchemaA.List(func(table *odps.Table, err error) {
		print(table.Name() + "\n")
	})

	// create schema
	schemas.Create("new_schema", false, "comment")

	// delete schema
	schemas.Delete("to_delete_schema")

	// get schema meta, remember load first
	schema := schemas.Get("new_schema")
	schema.Load()

	// all schema meta
	schema.Name()
	schema.ProjectName()
	schema.Type()
	schema.Owner()
	schema.Comment()
	schema.CreateTime()
	schema.ModifiedTime()
}
