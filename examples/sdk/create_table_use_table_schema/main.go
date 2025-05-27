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

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	c1 := tableschema.Column{
		Name: "tiny_int_type",
		Type: datatype.TinyIntType,
	}

	c2 := tableschema.Column{
		Name: "small_int_type",
		Type: datatype.SmallIntType,
	}

	c3 := tableschema.Column{
		Name: "int_type",
		Type: datatype.IntType,
	}

	c4 := tableschema.Column{
		Name: "bigint_type",
		Type: datatype.BigIntType,
	}

	c5 := tableschema.Column{
		Name: "binary_type",
		Type: datatype.BinaryType,
	}

	c6 := tableschema.Column{
		Name: "float_type",
		Type: datatype.FloatType,
	}

	c7 := tableschema.Column{
		Name: "double_type",
		Type: datatype.DoubleType,
	}

	c8 := tableschema.Column{
		Name: "decimal_type",
		Type: datatype.NewDecimalType(10, 8),
	}

	c9 := tableschema.Column{
		Name: "varchar_type",
		Type: datatype.NewVarcharType(500),
	}

	c10 := tableschema.Column{
		Name: "char_type",
		Type: datatype.NewCharType(254),
	}

	c11 := tableschema.Column{
		Name: "string_type",
		Type: datatype.StringType,
	}

	c12 := tableschema.Column{
		Name: "date_type",
		Type: datatype.DateType,
	}

	c13 := tableschema.Column{
		Name: "datetime_type",
		Type: datatype.DateTimeType,
	}

	c14 := tableschema.Column{
		Name: "timestamp_type",
		Type: datatype.TimestampType,
	}

	c15 := tableschema.Column{
		Name: "timestamp_ntz_type",
		Type: datatype.TimestampNtzType,
	}

	c16 := tableschema.Column{
		Name: "boolean_type",
		Type: datatype.BooleanType,
	}

	mapType := datatype.NewMapType(datatype.StringType, datatype.BigIntType)
	arrayType := datatype.NewArrayType(datatype.StringType)
	structType := datatype.NewStructType(
		datatype.NewStructFieldType("arr", arrayType),
		datatype.NewStructFieldType("name", datatype.StringType),
	)
	jsonType := datatype.NewJsonType()

	c17 := tableschema.Column{
		Name: "map_type",
		Type: mapType,
	}

	c18 := tableschema.Column{
		Name: "array_type",
		Type: arrayType,
	}

	c19 := tableschema.Column{
		Name: "struct_type",
		Type: structType,
	}

	c20 := tableschema.Column{
		Name: "json_type",
		Type: jsonType,
	}

	p1 := tableschema.Column{
		Name: "p1",
		Type: datatype.BigIntType,
	}

	p2 := tableschema.Column{
		Name: "p2",
		Type: datatype.StringType,
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Name("all_types_demo").
		Columns(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20).
		PartitionColumns(p1, p2).
		Lifecycle(2).
		TblProperties(map[string]string{"transactional": "true"}) // 创建 Transactional 表

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
