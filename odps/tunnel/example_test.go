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

package tunnel_test

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
	"os"
)

var tunnelIns tunnel.Tunnel
var odpsIns *odps.Odps
var ProjectName = "test_new_console_gcc"

func init() {
	accessId := os.Getenv("tunnel_odps_accessId")
	accessKey := os.Getenv("tunnel_odps_accessKey")
	odpsEndpoint := os.Getenv("odps_endpoint")
	tunnelEndpoint := os.Getenv("tunnel_odps_endpoint")

	account := account2.NewAliyunAccount(accessId, accessKey)
	odpsIns = odps.NewOdps(account, odpsEndpoint)
	tunnelIns = tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	//createTableWithComplexData()
	//createSaleDetailTable()
	///createUploadSampleArrowTable()
}

func createTableWithComplexData() {
	columnType, _ := datatype.ParseDataType("struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>")
	column := tableschema.Column{
		Name: "struct_field",
		Type: columnType,
	}

	builder := tableschema.NewSchemaBuilder()
	builder.Name("has_struct").Columns(column)
	schema := builder.Build()

	tables := odps.NewTables(odpsIns, ProjectName, "")
	err := tables.Create(schema, true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func createSaleDetailTable() {
	c1 := tableschema.Column{
		Name: "shop_name",
		Type: datatype.StringType,
	}

	c2 := tableschema.Column{
		Name: "custom_id",
		Type: datatype.StringType,
	}

	c3 := tableschema.Column{
		Name: "total_price",
		Type: datatype.DoubleType,
	}

	p1 := tableschema.Column{
		Name: "sale_date",
		Type: datatype.StringType,
	}

	p2 := tableschema.Column{
		Name: "region",
		Type: datatype.StringType,
	}

	builder := tableschema.NewSchemaBuilder()
	builder.Name("sale_detail").
		Columns(c1, c2, c3).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	tables := odps.NewTables(odpsIns, ProjectName, "")
	err := tables.Create(schema, true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func createUploadSampleArrowTable() {
	ins, err := odpsIns.ExecSQl("CREATE TABLE IF NOT EXISTS project_1.upload_sample_arrow(payload STRING);")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
