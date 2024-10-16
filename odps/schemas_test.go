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

package odps

import (
	"fmt"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"log"
	"testing"
)

var (
	account  account2.Account
	endpoint string
	odpsIns  *Odps
	project  string
)

func setup() {
	account = account2.AliyunAccountFromEnv()
	endpoint = restclient.LoadEndpointFromEnv()
	odpsIns = NewOdps(account, endpoint)
	project = "dingxin"

	schemas := NewSchemas(odpsIns, project)
	err := schemas.Create("exist_schema", true, "create by ut")
	if err != nil {
		log.Fatalf("%+v", err)
		return
	}

	// create some tables in exist_schema
	tables := NewTables(odpsIns, project, "exist_schema")
	c := tableschema.Column{
		Name: "name",
		Type: datatype.StringType,
	}
	schemaBuilder := tableschema.NewSchemaBuilder()
	err = tables.Create(schemaBuilder.Name("table1").Column(c).Build(), true, nil, nil)
	err = tables.Create(schemaBuilder.Name("table2").Build(), true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
		return
	}
}

func TestMain(m *testing.M) {
	// 在运行所有测试前进行设置
	setup()
	m.Run()
}

func TestSchemas_List(t *testing.T) {
	schemas := NewSchemas(odpsIns, project)
	schemas.List(func(schema *Schema, err error) {
		print(schema.Name() + "\n")
	})
}

func TestSchemas_GetSchema(t *testing.T) {
	schema := NewSchema(odpsIns, project, "exist_schema")
	schema.Load()
	print(schema.ModifiedTime().String())
}

func TestSchemas_ListTableBySchema(t *testing.T) {
	ts := NewTables(odpsIns, project, "exist_schema")
	var f = func(t *Table, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
	}
	ts.List(f, nil)
}

func TestSchemas_CheckExists(t *testing.T) {
	schema := NewSchema(odpsIns, project, "not_exists")
	exists, err := schema.Exists()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	print(exists)

	schema = NewSchema(odpsIns, project, "exist_schema")
	exists, err = schema.Exists()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	print(exists)
}

func TestSchemas_CreateSchema(t *testing.T) {
	schemas := NewSchemas(odpsIns, project)
	err := schemas.Create("new_schema", true, "new comment")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	schema := NewSchema(odpsIns, project, "new_schema")
	schema.Load()
	println(schema.Comment())
}

func TestSchemas_DeleteSchema(t *testing.T) {
	schemas := NewSchemas(odpsIns, project)

	err := schemas.Create("to_delete_schema", true, "to delete")

	schema := NewSchema(odpsIns, project, "to_delete_schema")
	exists, err := schema.Exists()
	print(exists)

	err = schemas.Delete("to_delete_schema")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	exists, err = schema.Exists()
	print(exists)
}
