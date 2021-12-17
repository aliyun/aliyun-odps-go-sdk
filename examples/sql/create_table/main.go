package main

import (
	"database/sql"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
)

func main() {
	accessId := ""
	accessKey := ""
	endpoint := ""
	projectName := ""

	config := sqldriver.NewConfig()
	config.Endpoint = endpoint
	config.AccessId = accessId
	config.AccessKey = accessKey
	config.ProjectName = projectName

	dsn := config.FormatDsn()
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	sqlStr := "create table if not exists user_test (" +
		"name string,score int,birthday date,addresses array<string>" +
		") " +
		"partitioned by (age int,hometown string) " +
		"lifecycle 2;"

	_, err = db.Exec(sqlStr)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
