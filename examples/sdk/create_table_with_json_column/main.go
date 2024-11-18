package main

import (
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func main() {
	conf, err := odps.NewConfigFromIni("./config.ini")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	c1 := tableschema.Column{
		Name: "name",
		Type: datatype.NewJsonType(),
	}

	println(c1.Type.Name())

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Name("test_json").
		Columns(c1).
		Comment("test for table creation with golang sdk").
		Lifecycle(2)

	schema := schemaBuilder.Build()
	tablesIns := odpsIns.Tables()
	err = tablesIns.Create(schema, false, conf.Hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
