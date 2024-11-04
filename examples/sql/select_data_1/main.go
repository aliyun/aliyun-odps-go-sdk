package main

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func main() {
	config, err := sqldriver.NewConfigFromIni("./config.ini")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	dsn := config.FormatDsn()
	// or dsn := "http://<accessId>:<accessKey>@<endpoint>?project=<project>"

	db, err := sql.Open("odps", dsn)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	selectSql := "select * from all_types_demo where bigint_type=@bigint_type and p1=@p1 and p2='@p2';"

	rows, err := db.Query(
		selectSql,
		sql.Named("bigint_type", 100000000000),
		sql.Named("p1", 20),
		sql.Named("p2", "hangzhou"),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	record := make([]interface{}, len(columnTypes))

	for i, columnType := range columnTypes {
		record[i] = reflect.New(columnType.ScanType()).Interface()
		t := reflect.TypeOf(record[i])

		fmt.Printf("kind=%s, name=%s\n", t.Kind(), t.String())
	}

	columns, err := rows.Columns()

	for rows.Next() {
		err = rows.Scan(record...)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, r := range record {
			rr := r.(sqldriver.NullAble)

			if rr.IsNull() {
				fmt.Printf("%s=NULL", columns[i])
			} else {
				switch r.(type) {
				case *sqldriver.NullInt8:
					fmt.Printf("%s=%d", columns[i], r.(*sqldriver.NullInt8).Int8)
				case *sqldriver.NullInt16:
					fmt.Printf("%s=%d", columns[i], r.(*sqldriver.NullInt16).Int16)
				case *sqldriver.NullInt32:
					fmt.Printf("%s=%d", columns[i], r.(*sqldriver.NullInt32).Int32)
				case *sqldriver.NullInt64:
					fmt.Printf("%s=%d", columns[i], r.(*sqldriver.NullInt64).Int64)
				case *sqldriver.Binary:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullFloat32:
					fmt.Printf("%s=%f", columns[i], r.(*sqldriver.NullFloat32).Float32)
				case *sqldriver.NullFloat64:
					fmt.Printf("%s=%f", columns[i], r.(*sqldriver.NullFloat64).Float64)
				case *sqldriver.Decimal:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullString:
					fmt.Printf("%s=%s", columns[i], r.(*sqldriver.NullString).String)
				case *sqldriver.NullDate:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullDateTime:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullTimeStamp:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullTimeStampNtz:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.NullBool:
					fmt.Printf("%s=%v", columns[i], r.(*sqldriver.NullBool).Bool)
				case *sqldriver.Map:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.Array:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.Struct:
					fmt.Printf("%s=%s", columns[i], r)
				case *sqldriver.Json:
					fmt.Printf("%s=%s", columns[i], r)
				}
			}

			if i < len(record)-1 {
				fmt.Printf(", ")
			} else {
				fmt.Print("\n\n")
			}
		}
	}
}
