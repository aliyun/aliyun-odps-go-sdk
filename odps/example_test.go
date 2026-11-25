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
	"log"
	"os"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// Every object this suite creates is named through livetest.Fixture, so the
// creator, the readers in the other test files and the cleanup below all agree
// on one name per run. With MAXCOMPUTE_LIVE_RUN_ID unset the names are the
// historical ones and dropRunScopedFixtures does nothing, which leaves a
// fixture a human created alone.
var (
	account            = account2.AccountFromEnv()
	endpoint           = restclient.LoadEndpointFromEnv()
	odpsIns            = odps.NewOdps(account, endpoint)
	defaultProjectName = "go_sdk_regression_testing"
)

func init() {
	// These fixtures create tables and a schema on the live service.
	if !liveServiceEnabled() {
		return
	}

	// odpsIns.SetDefaultProjectName("odps_smoke_test")
	odpsIns.SetDefaultProjectName(defaultProjectName)
	// 在这里初始化表、分区等
	createUserTable(livetest.Fixture("user"))
	createUserTable(livetest.Fixture("user_temp"))
	createTableWithComplexData()
	createSaleDetailTable()
	createTestSchema()
}

func requireLiveService(t *testing.T) {
	t.Helper()
	if !liveServiceEnabled() {
		t.Skip("requires MAXCOMPUTE_LIVE_TESTS=1, a live endpoint, and credentials")
	}
}

func liveServiceEnabled() bool {
	return os.Getenv("MAXCOMPUTE_LIVE_TESTS") == "1" && account != nil && endpoint != ""
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
	hints["odps.namespace.schema"] = "false"

	builder := tableschema.NewSchemaBuilder()
	builder.Name(tableName).
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	tables := odps.NewTables(odpsIns, defaultProjectName, "")
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
	builder.Name(livetest.Fixture("has_struct")).Columns(column)
	schema := builder.Build()

	hints := make(map[string]string)
	hints["odps.namespace.schema"] = "false"
	tables := odps.NewTables(odpsIns, defaultProjectName, "")
	err := tables.Create(schema, true, hints, nil)
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
	builder.Name(livetest.Fixture("sale_detail")).
		Columns(c1, c2, c3).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	tables := odps.NewTables(odpsIns, defaultProjectName, "")

	hints := make(map[string]string)
	hints["odps.namespace.schema"] = "false"
	err := tables.Create(schema, true, hints, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func createTestSchema() {
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	err := schemas.Create(livetest.Fixture("exist_schema"), true, "create by ut")
	if err != nil {
		log.Fatalf("%+v", err)
		return
	}

	// create some tables in exist_schema
	table := livetest.Fixture("exist_schema")
	tables := odps.NewTables(odpsIns, defaultProjectName, table)
	c := tableschema.Column{
		Name: "name",
		Type: datatype2.StringType,
	}
	schemaBuilder := tableschema.NewSchemaBuilder()
	err = tables.Create(schemaBuilder.Name(livetest.Fixture("table1")).Column(c).Build(), true, nil, nil)
	err = tables.Create(schemaBuilder.Name(livetest.Fixture("table2")).Build(), true, nil, nil)
	if err != nil {
		log.Fatalf("%+v", err)
		return
	}
}

// dropRunScopedFixtures removes the objects created above once the run is
// done. It only ever drops namespaced names: without a namespace the names
// point at the shared fixtures other people's runs may still be using, and
// deleting those would be the very hazard this file is about.
func dropRunScopedFixtures() []string {
	if livetest.RunID() == "" {
		return nil
	}

	failures := []string{}
	defaultTables := odps.NewTables(odpsIns, defaultProjectName, "")
	for _, name := range []string{
		livetest.Fixture("user"),
		livetest.Fixture("user_temp"),
		livetest.Fixture("has_struct"),
		livetest.Fixture("sale_detail"),
	} {
		if err := defaultTables.Delete(name, true); err != nil {
			failures = append(failures, "drop table "+name+": "+err.Error())
		}
	}

	schemaTables := odps.NewTables(odpsIns, defaultProjectName, livetest.Fixture("exist_schema"))
	for _, name := range []string{livetest.Fixture("table1"), livetest.Fixture("table2")} {
		if err := schemaTables.Delete(name, true); err != nil {
			failures = append(failures, "drop table "+livetest.Fixture("exist_schema")+"."+name+": "+err.Error())
		}
	}

	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	if err := schemas.Delete(livetest.Fixture("exist_schema")); err != nil {
		failures = append(failures, "drop schema "+livetest.Fixture("exist_schema")+": "+err.Error())
	}
	return failures
}

// TestMain keeps the offline behaviour untouched: with the live gate closed
// nothing here contacts a service, so it is m.Run() and its exit code exactly
// as before. With the gate open, the fixtures this package created in init are
// dropped once the tests finish.
//
// A failed drop is reported on stdout but does not change the exit code. The
// run that just executed its assertions is the thing this suite grades; a
// leftover namespaced table is noise someone can read, not evidence that the
// SDK misbehaved.
func TestMain(m *testing.M) {
	code := m.Run()

	if liveServiceEnabled() && os.Getenv(livetest.EnvKeepFixtures) != "1" {
		for _, failure := range dropRunScopedFixtures() {
			println("live fixture cleanup: " + failure)
		}
	}

	os.Exit(code)
}
