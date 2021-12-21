package main

import (
	"database/sql"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
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

	insertSql := "insert into user_test partition (age=20, hometown='hangzhou') values (" +
		"@name, @score, @birthday, @extra);"

	birthday, _ := data.NewDateTime("2010-11-11 15:20:00")
	addresses := data.NewArray()
	_ = addresses.Append("apsaras", "efc")
	extra := data.NewStruct()
	_ = extra.SetField("address", addresses)
	_ = extra.SetField("hobby", "swimming")

	_, err = db.Exec(
		insertSql,
		sql.Named("name", "'xiaoming'"),
		sql.Named("score", 99),
		sql.Named("birthday", birthday.Sql()), // datetime'2010-11-11 15:20:00'
		sql.Named("extra", extra.Sql()),       // named_struct('address', array('apsaras', 'efc'), 'hobby', 'swimming')
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}
}
