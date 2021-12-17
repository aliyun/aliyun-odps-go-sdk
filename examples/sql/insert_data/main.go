package main

import (
	"database/sql"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
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

	insertSql := "insert into user_test partition (age=20, hometown='hangzhou') values (" +
		"@name, @score, @birthday, @addresses);"

	birthday, _ := data.NewDateTime("2010-11-11 15:20:00")
	addresses := data.NewArray()
	_ = addresses.Append("apsaras", "efc")

	_, err = db.Exec(
		insertSql,
		sql.Named("name", "'xiaoming'"),
		sql.Named("score", 99),
		sql.Named("birthday", birthday.Sql()),   // datetime'2010-11-11 15:20:00'
		sql.Named("addresses", addresses.Sql()), // array('apsaras', 'efc')
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}
}
