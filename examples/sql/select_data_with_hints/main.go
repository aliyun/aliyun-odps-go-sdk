package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func main() {
	dsn := "http://<accessId>:<accessKey>@<endpoint>" +
		"?project=<project>&odps.namespace.schema=true&odps.default.schema=<schema_name>&other.odps.flag=<flag>&enableLogview=true"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	selectSql := ` select date_col 
 				   from @table_name;
				`

	rows, err := db.Query(
		selectSql,
		sql.Named("table_name", "table1"),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	var date sqldriver.NullDate
	for rows.Next() {
		err = rows.Scan(&date)
		if err != nil {
			log.Fatalf("%+v", err)
		}
		fmt.Println(date)
	}
}
