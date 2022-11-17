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
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"log"
)

var account = account2.AliyunAccountFromEnv()
var endpoint = restclient.LoadEndpointFromEnv()
var odpsIns = odps.NewOdps(account, endpoint)
var defaultProjectName = "project_1"

func init() {
	if account.AccessId() == "" {
		panic("account environments are not set")
	}

	//odpsIns.SetDefaultProjectName("odps_smoke_test")
	odpsIns.SetDefaultProjectName(defaultProjectName)
	// 在这里初始化表、分区等
	createUserTable("user")
	createUserTable("user_temp")
	createTableWithComplexData()
	createSaleDetailTable()
}

func createUserTable(tableName string) {
	c1 := tableschema.Column{
		Name:    "name",
		Type:    datatype2.StringType,
		Comment: "name of user",
	}

	c2 := tableschema.Column{
		Name:    "age",
		Type:    datatype2.IntType,
		Comment: "how old is the user",
	}

	p1 := tableschema.Column{
		Name:    "region",
		Type:    datatype2.StringType,
		Comment: "居住区域",
	}

	p2 := tableschema.Column{
		Name: "code",
		Type: datatype2.IntType,
	}

	hints := make(map[string]string)
	hints["odps.sql.preparse.odps"] = "lot"
	hints["odps.sql.planner.mode"] = "lot"
	hints["odps.sql.planner.parser.odps"] = "true"
	hints["odps.sql.ddl.odps"] = "true"
	hints["odps.compiler.output.format"] = "lot,pot"

	builder := tableschema.NewSchemaBuilder()
	builder.Name(tableName).
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	tables := odps.NewTables(odpsIns, defaultProjectName)
	err := tables.Create(schema, true, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func createTableWithComplexData() {
	columnType, _ := datatype2.ParseDataType("struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>")
	column := tableschema.Column{
		Name: "struct_field",
		Type: columnType,
	}

	builder := tableschema.NewSchemaBuilder()
	builder.Name("has_struct").Columns(column)
	schema := builder.Build()

	tables := odps.NewTables(odpsIns, defaultProjectName)
	err := tables.Create(schema, true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func createSaleDetailTable() {
	c1 := tableschema.Column{
		Name: "shop_name",
		Type: datatype2.StringType,
	}

	c2 := tableschema.Column{
		Name: "custom_id",
		Type: datatype2.StringType,
	}

	c3 := tableschema.Column{
		Name: "total_price",
		Type: datatype2.DoubleType,
	}

	p1 := tableschema.Column{
		Name: "sale_date",
		Type: datatype2.StringType,
	}

	p2 := tableschema.Column{
		Name: "region",
		Type: datatype2.StringType,
	}

	builder := tableschema.NewSchemaBuilder()
	builder.Name("sale_detail").
		Columns(c1, c2, c3).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	tables := odps.NewTables(odpsIns, defaultProjectName)
	err := tables.Create(schema, true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
