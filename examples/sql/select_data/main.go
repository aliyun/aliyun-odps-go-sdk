package main

import (
	"database/sql"
	"fmt"
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

	selectSql := "select * from all_types_demo where bigint_type=@bigint_type and p1=20 and p2='hangzhou';"

	rows, err := db.Query(
		selectSql,
		sql.Named("bigint_type", 100000000000),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	var tinyInt sqldriver.NullInt8        // if the column is not nullable, int8 is ok too
	var smallInt sqldriver.NullInt16      // if the column is not nullable, int16 is ok too
	var intData sqldriver.NullInt32       // if the column is not nullable, int is ok too
	var bigInt sqldriver.NullInt64        // if the column is not nullable, int64 is ok too
	var binaryData sqldriver.Binary       // if the column is not nullable, []byte or database/sql.RawBytes is ok too
	var floatData sqldriver.NullFloat32   // if the column is not nullable, float32 is ok too
	var doubleData sqldriver.NullFloat64  // if the column is not nullable, float64 is ok too
	var decimal sqldriver.Decimal         // if the column is not nullable
	var varchar sqldriver.NullString      // if the column is not nullable, string is ok too
	var char sqldriver.NullString         // if the column is not nullable, string is ok too
	var stringData sqldriver.NullString   // if the column is not nullable, string is ok too
	var date sqldriver.NullDate           // if the column is not nullable, odps/data.Date is ok too
	var dateTime sqldriver.NullDateTime   // if the column is not nullable, odps/data.Datetime is ok too
	var timestamp sqldriver.NullTimeStamp // if the column is not nullable, odps/data.TimeStamp is ok too
	var timestampNtz sqldriver.NullTimeStampNtz // if the column is not nullable, odps/data.TimeStamp is ok too
	var boolData sqldriver.NullBool       // if the column is not nullable, bool is ok too
	var mapData sqldriver.Map
	var arrayData sqldriver.Array
	var structType sqldriver.Struct
	var p1 sqldriver.NullInt64
	var p2 sqldriver.NullString

	record := []interface{}{
		&tinyInt, &smallInt, &intData, &bigInt, &binaryData, &floatData, &doubleData, &decimal, &varchar,
		&char, &stringData, &date, &dateTime, &timestamp, &timestampNtz, &boolData, &mapData, &arrayData, &structType,
		&p1, &p2,
	}

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	if len(columns) != len(record) {
		log.Fatalf("columns length is %d, but reocrd length is %d\n", len(columns), len(record))
	}

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
				fmt.Printf("%s=%v", columns[i], record[i])
			}

			if i < len(record)-1 {
				fmt.Printf(", ")
			} else {
				fmt.Print("\n\n")
			}
		}
	}
}
