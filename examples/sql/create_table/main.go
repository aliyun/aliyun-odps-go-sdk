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
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	sqlStr := "create table if not exists all_types_demo (" +
		"    tiny_int_type         tinyint," +
		"    small_int_type        smallint," +
		"    int_type              int," +
		"    bigint_type           bigint," +
		"    binary_type           binary," +
		"    float_type            float," +
		"    double_type           double," +
		"    decimal_type          decimal(10, 8)," +
		"    varchar_type          varchar(500)," +
		"    char_type             varchar(254)," +
		"    string_type           string," +
		"    date_type             date," +
		"    datetime_type         datetime," +
		"    timestamp_type        timestamp," +
		"    boolean_type          boolean," +
		"    map_type              map<string, bigint>," +
		"    array_type            array< string>," +
		"    struct_type           struct<arr:ARRAY<STRING>, name:STRING>" +
		") " +
		"partitioned by (p1 bigint, p2 string);"

	_, err = db.Exec(sqlStr)
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
