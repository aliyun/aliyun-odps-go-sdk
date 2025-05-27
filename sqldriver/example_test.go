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
	"log"
	"time"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func Example() {
	account := account2.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint

	if account.GetType() == account2.STS {
		stsAccount, _ := account.(*account2.StsAccount)
		config.AccessId = stsAccount.AccessId()
		config.AccessKey = stsAccount.AccessKey()
		config.StsToken = stsAccount.StsToken()

	} else if account.GetType() == account2.Aliyun {
		akAccount, _ := account.(*account2.ApsaraAccount)
		config.AccessId = akAccount.AccessId()
		config.AccessKey = akAccount.AccessKey()
	} else {
		errMsg := "unknown account type: %s" + account.GetType().String()
		panic(errMsg)
	}
	config.ProjectName = "go_sdk_regression_testing"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	_, err = db.Exec("create table if not exists data_type_demo(ti tinyint, si smallint, i int, bi bigint, b binary, f float, d double);", nil)
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

		log.Println(ti, si, i, bi, b, f, d)
	}

	// Output:
}

func ExampleStructField() {
	account := account2.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	if account.GetType() == account2.STS {
		stsAccount, _ := account.(*account2.StsAccount)
		config.AccessId = stsAccount.AccessId()
		config.AccessKey = stsAccount.AccessKey()
		config.StsToken = stsAccount.StsToken()
	} else if account.GetType() == account2.Aliyun {
		akAccount, _ := account.(*account2.ApsaraAccount)
		config.AccessId = akAccount.AccessId()
		config.AccessKey = akAccount.AccessKey()
	} else {
		log.Fatalf("unknown account type: %s", account.GetType())
	}
	config.ProjectName = "go_sdk_regression_testing"

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
	account := account2.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	if account.GetType() == account2.STS {
		stsAccount, _ := account.(*account2.StsAccount)
		config.AccessId = stsAccount.AccessId()
		config.AccessKey = stsAccount.AccessKey()
		config.StsToken = stsAccount.StsToken()
	} else if account.GetType() == account2.Aliyun {
		akAccount, _ := account.(*account2.ApsaraAccount)
		config.AccessId = akAccount.AccessId()
		config.AccessKey = akAccount.AccessKey()
	} else {
		log.Fatalf("unknown account type: %s", account.GetType())
	}
	config.ProjectName = "go_sdk_regression_testing"

	dsn := config.FormatDsn()
	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	db.Exec("drop table if exists simple_struct;")
	db.Exec("create table if not exists simple_struct (col struct<a:int, b:struct<b1:STRING>>);")

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
	log.Println(odpsStruct)

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
	account := account2.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	if account.GetType() == account2.STS {
		stsAccount, _ := account.(*account2.StsAccount)
		config.AccessId = stsAccount.AccessId()
		config.AccessKey = stsAccount.AccessKey()
		config.StsToken = stsAccount.StsToken()
	} else if account.GetType() == account2.Aliyun {
		akAccount, _ := account.(*account2.ApsaraAccount)
		config.AccessId = akAccount.AccessId()
		config.AccessKey = akAccount.AccessKey()
	} else {
		log.Fatalf("unknown account type: %s", account.GetType())
	}
	config.ProjectName = "go_sdk_regression_testing"

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

	_, err = db.Exec("create table if not exists simple_struct (struct_field @f);", sql.Named("f", structType.String()))
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
