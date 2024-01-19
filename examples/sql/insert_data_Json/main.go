package main

import (
	"database/sql"
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

	insertSql := "insert into json_table values (" +
		"@json);"
	// struct type
	s := struct {
		Typ   string
		Value int
	}{
		Typ:   "asfdsahfh",
		Value: 10000,
	}
	jsonIns := data.NewJson(s)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// slice type
	a := []string{"abc", "edf", "ghj"}
	jsonIns = data.NewJson(a)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// number type
	i := 10000
	jsonIns = data.NewJson(i)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// string type
	str := "abcdfghijklmn"
	jsonIns = data.NewJson(str)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// bool type
	b := true
	jsonIns = data.NewJson(b)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// double type
	d := 123456.789
	jsonIns = data.NewJson(d)
	_, err = db.Exec(
		insertSql,
		sql.Named("json", jsonIns.Sql()),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	//
}
