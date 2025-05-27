package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateDownloadSession(
		project.Name(),
		"user_test",
		tunnel.SessionCfg.WithPartitionKey("age=20,hometown='hangzhou'"),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	fmt.Printf("record count is %d", recordCount)

	reader, err := session.OpenRecordArrowReader(0, recordCount, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	totalRows := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("Read Record Failed: %v", err)
		}
		defer rec.Release()

		numRows := int(rec.NumRows())
		for row := 0; row < numRows; row++ {
			for colIdx, col := range rec.Columns() {
				if col.IsNull(row) {
					fmt.Printf("Row %d, Column %d: NULL\n", row, colIdx)
				} else {
					var value interface{}
					switch arr := col.(type) {
					case *array.Int32:
						value = arr.Value(row)
					case *array.Int64:
						value = arr.Value(row)
					case *array.String:
						value = arr.Value(row)
					case *array.Boolean:
						value = arr.Value(row)
					case *array.Float64:
						value = arr.Value(row)
					default:
						value = "Other Type, Not Implemented in this example."
					}
					fmt.Printf("Row %d, Column %d: %v\n", row, colIdx, value)
				}
			}
		}
		totalRows += numRows
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
