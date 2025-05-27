package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
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

	maxqaQuotaName := "maxqa_quota_nick"
	sql := "select 1;"
	taskOptions := options.NewSQLTaskOptions(
		options.WithInstanceOption(options.WithMaxQAOptions("", maxqaQuotaName)))

	instance, err := odpsIns.ExecSQlWithOption(sql, taskOptions)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(instance.Id())

	err = instance.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Get CSV Result
	result, err := instance.GetResult()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(result[0].Content())

	// Get Result By Instance Tunnel
	tunnelIns := tunnel.NewTunnel(odpsIns)
	session, err := tunnelIns.CreateInstanceResultDownloadSession(conf.ProjectName, instance.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	start := 0
	step := 200000
	recordCount := session.RecordCount()
	schema := session.Schema()
	total := 0

	// Iterate the reader step by step
	for start < recordCount {
		reader, err := session.OpenRecordReader(start, step, 0, nil)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		count := 0
		err = reader.Iterator(func(record data.Record, _err error) {
			count += 1

			if _err != nil {
				return
			}

			for i, d := range record {
				if d == nil {
					fmt.Printf("%s=null", schema.Columns[i].Name)
				} else {
					fmt.Printf("%s=%s", schema.Columns[i].Name, d.Sql())
				}

				if i < record.Len()-1 {
					fmt.Printf(", ")
				} else {
					fmt.Println()
				}
			}
		})
		if err != nil {
			log.Fatalf("%+v", err)
		}

		start += count
		total += count
		log.Println(count)
		if err = reader.Close(); err != nil {
			log.Fatalf("%+v", err)
		}
	}

	println("total count ", total)
}
