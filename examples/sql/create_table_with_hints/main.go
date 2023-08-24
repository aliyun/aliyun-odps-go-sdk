package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func main() {
	config, err := sqldriver.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	dsn := config.FormatDsn()
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>&odps.sql.type.system.odps2=true&odps.sql.decimal.odps2=true"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	sqlStr := `create table table_with_date (
				 date_col DATE);
				`
	_, err = db.Exec(sqlStr)
	if err != nil {
		log.Fatalf("%+v", err)
	}

}
