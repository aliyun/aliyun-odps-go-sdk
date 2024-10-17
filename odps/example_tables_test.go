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

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func ExampleTables_List() {
	ts := odps.NewTables(odpsIns, "", "")
	var f = func(t *odps.Table, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
	}
	ts.List(f, odps.TableFilter.Extended())

	// Output:
}

func ExampleTables_BatchLoadTables() {
	tablesIns := odps.NewTables(odpsIns, "", "")
	tableNames := []string{
		"jet_mr_input",
		"jet_smode_test",
		"odps_smoke_table",
		"user",
	}

	tables, err := tablesIns.BatchLoadTables(tableNames)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, table := range tables {
		println(fmt.Sprintf("%s, %s, %s", table.Name(), table.TableID(), table.Type()))
	}

	schema := tables[len(tables)-1].Schema()

	for _, c := range schema.Columns {
		println(fmt.Sprintf("%s, %s, %t, %s", c.Name, c.Type, c.IsNullable, c.Comment))
	}

	// Output:
}

func ExampleTables_Create() {
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

	hints := make(map[string]string)
	hints["odps.sql.preparse.odps"] = "lot"
	hints["odps.sql.planner.mode"] = "lot"
	hints["odps.sql.planner.parser.odps"] = "true"
	hints["odps.sql.ddl.odps"] = "true"
	hints["odps.compiler.output.format"] = "lot,pot"

	builder := tableschema.NewSchemaBuilder()
	builder.Name("user_temp").
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	sql, _ := schema.ToSQLString(defaultProjectName, "", false)
	println(sql)

	tables := odps.NewTables(odpsIns, odpsIns.DefaultProjectName(), "")
	err := tables.Create(schema, true, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func ExampleTables_Delete() {
	tables := odps.NewTables(odpsIns, odpsIns.DefaultProjectName(), "")
	err := tables.Delete("user_temp", false)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
