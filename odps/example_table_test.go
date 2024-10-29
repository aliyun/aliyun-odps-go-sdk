// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps_test

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func ExampleTableSchema_ToSQLString() {
	c1 := tableschema.Column{
		Name:    "name",
		Type:    datatype.StringType,
		Comment: "name of user",
	}

	c2 := tableschema.Column{
		Name:    "age",
		Type:    datatype.IntType,
		Comment: "how old is the user",
	}

	p1 := tableschema.Column{
		Name:    "region",
		Type:    datatype.StringType,
		Comment: "居住区域",
	}

	p2 := tableschema.Column{
		Name: "code",
		Type: datatype.IntType,
	}

	serdeProperties := make(map[string]string)
	serdeProperties["odps.sql.preparse.odps"] = "lot"
	serdeProperties["odps.sql.planner.mode"] = "lot"
	serdeProperties["odps.sql.planner.parser.odps"] = "true"
	serdeProperties["odps.sql.ddl.odps"] = "true"
	serdeProperties["odps.compiler.output.format"] = "lot,pot"

	jars := []string{"odps-udf-example.jar", "another.jar"}

	builder := tableschema.NewSchemaBuilder()
	builder.Name("user").
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2).
		StorageHandler("com.aliyun.odps.CsvStorageHandler").
		Location("MOCKoss://full/uri/path/to/oss/directory/")

	schema := builder.Build()

	sql, _ := schema.ToSQLString("go_sdk_regression_testing", "schema", true)
	println("sql of create table:")
	println(sql)
	println()

	externalSql, err := schema.ToExternalSQLString(
		"go_sdk_regression_testing",
		"",
		true,
		serdeProperties,
		jars,
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	println("sql of create external table:")
	println(externalSql)

	// Output:
}

func ExampleTable_Load() {
	table := odpsIns.Table("has_struct")
	err := table.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	schema := table.Schema()
	println(fmt.Sprintf("%+v", schema.Columns))
	// Output:
}

func ExampleTable_AddPartition() {
	table := odpsIns.Table("sale_detail")
	err := table.AddPartition(true, "sale_date=202111/region=hangzhou")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func ExampleTable_GetPartitions() {
	table := odpsIns.Table("sale_detail")
	partitions, err := table.GetPartitions()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, p := range partitions {
		println(fmt.Sprintf("Value: %s", p.Value()))
		println(fmt.Sprintf("Create time: %s", p.CreatedTime()))
		println(fmt.Sprintf("Last DDL time: %s", p.LastDDLTime()))
		println(fmt.Sprintf("Last Modified time: %s", p.LastModifiedTime()))
		println("")
	}

	// Output:
}

func ExampleTable_ExecSql() {
	//table := odps.NewTable(odpsIns, "go_sdk_regression_testing", "sale_detail")
	table := odpsIns.Table("has_struct")
	//instance, err := table.ExecSql("SelectSale_detail", "select * from sale_detail;")
	instance, err := table.ExecSql("Select_has_struct", "select * from has_struct;")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = instance.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	results, err := instance.GetResult()
	if err != nil {
		log.Fatalf("%+v", err)
	} else if len(results) == 0 {
		log.Fatalf("should get at least one result")
	}

	println(fmt.Sprintf("%+v", results[0].Result))

	// Output:
}

//func ExampleTable_Read() {
//	table := odps.NewTable(odpsIns, defaultProjectName, "sale_detail")
//	columns := []string{
//		"shop_name", "customer_id", "total_price", "sale_date", "region",
//	}
//	reader, err := table.Read("", columns, -1, "")
//	if err != nil {
//		log.Fatalf("%+v", err)
//	}
//
//	for {
//		record, err := reader.Read()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		println(fmt.Sprintf("%+v", record))
//	}
//
//	// Output:
//}
