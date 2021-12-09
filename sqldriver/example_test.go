package sqldriver_test

import (
	"database/sql"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/data"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
)

func Example() {
	var account = odps.AliyunAccountFromEnv()
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
	var account = odps.AliyunAccountFromEnv()
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

func ExampleExecSql() {
	var account = odps.AliyunAccountFromEnv()
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

	date, _ := data.NewDate("2021-12-09")

	_, err = db.Exec("insert into has_date values (@date);", sql.Named("date", date.Sql()))
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
