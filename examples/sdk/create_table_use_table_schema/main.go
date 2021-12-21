package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"log"
	"os"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	c1 := tableschema.Column{
		Name: "name",
		Type: datatype.StringType,
	}

	c2 := tableschema.Column{
		Name: "score",
		Type: datatype.IntType,
	}

	arrayType := datatype.NewArrayType(datatype.StringType)
	structType := datatype.NewStructType(
		datatype.NewStructFieldType("Address", arrayType),
		datatype.NewStructFieldType("Hobby", datatype.StringType),
	)
	// or
	//structTypeStr := "struct<Address:array<string>, Hobby:string>"
	//structType, _ := datatype.ParseDataType(structTypeStr)

	c3 := tableschema.Column{
		Name: "birthday",
		Type: datatype.DateTimeType,
	}

	c4 := tableschema.Column{
		Name: "extra",
		Type: structType,
	}

	c5 := tableschema.Column{
		Name: "age",
		Type: datatype.IntType,
	}

	c6 := tableschema.Column{
		Name: "hometown",
		Type: datatype.StringType,
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Name("user_test").
		Columns(c1, c2, c3, c4).
		PartitionColumns(c5, c6).
		Lifecycle(2)

	schema := schemaBuilder.Build()
	tablesIns := odpsIns.Tables()
	hints := make(map[string]string)
	hints["odps.sql.type.system.odps2"] = "true"
	hints["odps.sql.decimal.odps2"] = "true"

	err = tablesIns.CreateAndWait(schema, true, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
