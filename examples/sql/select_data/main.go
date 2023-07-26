package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
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

	selectSql := "select * from user_test where name=@name and age=20 and hometown='hangzhou';"

	rows, err := db.Query(
		selectSql,
		sql.Named("name", "'xiaoming'"),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	var name sql.NullString
	var score sql.NullInt64
	var birthday sql.NullTime
	extra := data.NewStruct()
	var age sql.NullInt64
	var hometown sql.NullString

	for rows.Next() {
		err = rows.Scan(&name, &score, &birthday, &extra, &age, &hometown)
		if err != nil {
			log.Fatalf("%+v", err)
		}
		fmt.Printf(
			"name:%s, score:%d, birthday:%s, extra:%s, age:%d, hometown: %s",
			name, score, birthday, extra, age, hometown,
		)
	}
}
