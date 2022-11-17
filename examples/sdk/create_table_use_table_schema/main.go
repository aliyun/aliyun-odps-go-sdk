package main

import (
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
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
		Name: "ti",
		Type: datatype.TinyIntType,
	}

	c2 := tableschema.Column{
		Name: "si",
		Type: datatype.SmallIntType,
	}

	c3 := tableschema.Column{
		Name: "i",
		Type: datatype.IntType,
	}

	c4 := tableschema.Column{
		Name: "bi",
		Type: datatype.BigIntType,
	}

	c5 := tableschema.Column{
		Name: "b",
		Type: datatype.BinaryType,
	}

	c6 := tableschema.Column{
		Name: "f",
		Type: datatype.FloatType,
	}

	c7 := tableschema.Column{
		Name: "d",
		Type: datatype.DoubleType,
	}

	c8 := tableschema.Column{
		Name: "dc",
		Type: datatype.NewDecimalType(38, 18),
	}

	c9 := tableschema.Column{
		Name: "vc",
		Type: datatype.NewVarcharType(1000),
	}

	c10 := tableschema.Column{
		Name: "c",
		Type: datatype.NewCharType(100),
	}

	c11 := tableschema.Column{
		Name: "s",
		Type: datatype.StringType,
	}

	c12 := tableschema.Column{
		Name: "da",
		Type: datatype.DateType,
	}

	c13 := tableschema.Column{
		Name: "dat",
		Type: datatype.DateTimeType,
	}

	c14 := tableschema.Column{
		Name: "t",
		Type: datatype.TimestampType,
	}

	c15 := tableschema.Column{
		Name: "bl",
		Type: datatype.BooleanType,
	}

	arrayType := datatype.NewArrayType(datatype.StringType)
	structType := datatype.NewStructType(
		datatype.NewStructFieldType("arr", arrayType),
		datatype.NewStructFieldType("name", datatype.StringType),
	)

	c16 := tableschema.Column{
		Name: "st",
		Type: structType,
	}

	p1 := tableschema.Column{
		Name: "p1",
		Type: datatype.IntType,
	}

	p2 := tableschema.Column{
		Name: "p2",
		Type: datatype.StringType,
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Name("data_type_demo").
		Columns(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16).
		PartitionColumns(p1, p2).
		Lifecycle(2) // 单位: 天

	schema := schemaBuilder.Build()
	tablesIns := odpsIns.Tables()
	// 如果project的数据类型版本是1.0，需要通过下面的hints使用mc 2.0数据类型
	hints := make(map[string]string)
	hints["odps.sql.type.system.odps2"] = "true"
	hints["odps.sql.decimal.odps2"] = "true"

	err = tablesIns.Create(schema, true, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
