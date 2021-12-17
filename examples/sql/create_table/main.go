package main

import (
	"database/sql"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
	"os"
)

func main() {
	config, err := sqldriver.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	dsn := config.FormatDsn()
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	sqlStr := "create table if not exists user_test (" +
		"name string,score int,birthday datetime,addresses array<string>" +
		") " +
		"partitioned by (age int,hometown string) " +
		"lifecycle 2;"

	_, err = db.Exec(sqlStr)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
