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

	dsn, err := config.FormatDSN()
	if err != nil {
		log.Fatal(err)
	}
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	createJson := func(value interface{}) *data.Json {
		jsonObj, err := data.NewJson(value)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		return jsonObj
	}

	// sql of creating table json_demo :
	// CREATE TABLE IF NOT EXISTS json_demo(id int, json JSON);
	insertSql := "insert into json_table values (@id, json_parse(@json));"

	records := [][]data.Data{
		{
			data.Int(1),
			createJson(nil),
		},
		{
			data.Int(2),
			createJson(true),
		},
		{
			data.Int(3),
			createJson([]interface{}{"abc", "dfg"}),
		},
		{
			data.Int(4),
			createJson(
				struct {
					Age  int
					Name string
				}{
					Age:  20,
					Name: "Ali",
				}),
		},
		{
			data.Int(5),
			createJson("I am a string"),
		},
		{
			data.Int(6),
			createJson(""),
		},
		{
			data.Int(7),
			createJson(123),
		},
		{
			data.Int(8),
			createJson(123.467),
		},
		{
			data.Int(9),
			nil,
		},
	}

	for _, record := range records {
		var jsonText any
		if value, ok := record[1].(*data.Json); ok && value != nil {
			jsonText = value.GetData()
		}
		_, err = db.Exec(
			insertSql,
			sql.Named("id", int64(record[0].(data.Int))),
			sql.Named("json", jsonText),
		)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}
}
