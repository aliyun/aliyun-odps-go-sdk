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

package sqldriver_test

import (
	"database/sql"
	"fmt"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
	"time"
)

func Example() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	config.AccessId = account.AccessId()
	config.AccessKey = account.AccessKey()
	config.ProjectName = "project_1"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	rows, err := db.Query("select ti, si, i, bi, b, f, d from data_type_demo;", nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	var ti int8
	var si int16
	var i int32
	var bi int64
	var b []byte
	var f float32
	var d float64

	for rows.Next() {
		err = rows.Scan(&ti, &si, &i, &bi, &b, &f, &d)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(ti, si, i, bi, b, f, d)
	}

	// Output:
}

func ExampleStructField() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	config.AccessId = account.AccessId()
	config.AccessKey = account.AccessKey()
	config.ProjectName = "project_1"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	rows, err := db.Query("select struct_field from has_struct;", nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	odpsStruct := data.NewStruct()

	for rows.Next() {
		err = rows.Scan(&odpsStruct)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	// Output:
}

func ExampleInsert() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	config.AccessId = account.AccessId()
	config.AccessKey = account.AccessKey()
	config.ProjectName = "project_1"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	type SimpleStruct struct {
		A int32
		B struct {
			B1 string
		}
	}

	simpleStruct := SimpleStruct{
		A: 10,
		B: struct{ B1 string }{B1: time.Now().Format("2006-01-02 15:04:05")},
	}

	odpsStruct, err := data.StructFromGoStruct(simpleStruct)
	fmt.Println(odpsStruct)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	_, err = db.Exec("insert into simple_struct values (?);", sql.Named("", odpsStruct.Sql()))
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func ExampleCreateTable() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	config.AccessId = account.AccessId()
	config.AccessKey = account.AccessKey()
	config.ProjectName = "project_1"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	structType := datatype.NewStructType(
		datatype.NewStructFieldType("A", datatype.IntType),
		datatype.NewStructFieldType("B", datatype.NewStructType(
			datatype.NewStructFieldType("B1", datatype.StringType),
		)),
	)

	_, err = db.Exec("create table simple_struct (struct_field @f);", sql.Named("f", structType.String()))
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
