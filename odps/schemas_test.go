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
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestSchemas_List(t *testing.T) {
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	schemas.List(func(schema *odps.Schema, err error) {
		print(schema.Name() + "\n")
	})
}

func TestSchemas_GetSchema(t *testing.T) {
	schema := odps.NewSchema(odpsIns, defaultProjectName, "exist_schema")
	err := schema.Load()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	print(schema.ModifiedTime().String())
}

func TestSchemas_ListTableBySchema(te *testing.T) {
	ts := odps.NewTables(odpsIns, defaultProjectName, "exist_schema")
	var f = func(t *odps.Table, err error) {
		if err != nil {
			te.Fatalf("%+v", err)
		}

		println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
	}
	ts.List(f, nil)
}

func TestSchemas_CheckExists(t *testing.T) {
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	schema := schemas.Get("not_exists")
	exists, err := schema.Exists()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	schema = odps.NewSchema(odpsIns, defaultProjectName, "exist_schema")
	exists, err = schema.Exists()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	print(exists)
}

func TestSchemas_CreateSchema(t *testing.T) {
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	err := schemas.Create("new_schema", true, "new comment")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	schema := odps.NewSchema(odpsIns, defaultProjectName, "new_schema")
	schema.Load()
	println(schema.Comment())
}

func TestSchemas_DeleteSchema(t *testing.T) {
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)

	err := schemas.Create("to_delete_schema", true, "to delete")

	schema := odps.NewSchema(odpsIns, defaultProjectName, "to_delete_schema")
	exists, err := schema.Exists()
	print(exists)

	err = schemas.Delete("to_delete_schema")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	exists, err = schema.Exists()
	print(exists)
}
