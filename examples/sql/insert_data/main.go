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

	dsn, err := config.FormatDSN()
	if err != nil {
		log.Fatal(err)
	}
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	insertSQL := "insert into user_test partition (age=20, hometown='hangzhou') values (" +
		"@name, @score, cast(@birthday as datetime), named_struct('address', array(@first, @second), 'hobby', @hobby));"

	_, err = db.Exec(
		insertSQL,
		sql.Named("name", "xiaoming"),
		sql.Named("score", 99),
		sql.Named("birthday", "2010-11-11 15:20:00"),
		sql.Named("first", "apsaras"),
		sql.Named("second", "efc"),
		sql.Named("hobby", "swimming"),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
