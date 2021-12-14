package sqldriver_test

import (
	"database/sql"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	odps "github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
	"time"
)

func Example() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = odps.LoadEndpointFromEnv()

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
	var endpoint = odps.LoadEndpointFromEnv()

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

		println(odpsStruct.Sql())
	}

	// Output:
}

func ExampleInsert() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = odps.LoadEndpointFromEnv()

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
	var endpoint = odps.LoadEndpointFromEnv()

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
