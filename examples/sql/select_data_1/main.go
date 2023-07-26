package main

import (
	"database/sql"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"log"
	"os"
	"reflect"
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

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	values := make([]interface{}, len(columnTypes))

	for i, columnType := range columnTypes {
		fmt.Println(columnType.ScanType())

		values[i] = reflect.New(columnType.ScanType()).Interface()
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		fmt.Printf(
			"name:%v, score:%v, birthday:%v, extra:%v, age:%v, hometown: %v\n",
			values[0], values[1], values[2], values[3], values[4], values[5],
		)
	}
}
