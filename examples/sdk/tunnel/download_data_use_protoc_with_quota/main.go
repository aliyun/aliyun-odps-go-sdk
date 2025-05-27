package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	tunnel := tunnel2.NewTunnel(odpsIns)
	tunnel.SetQuotaName(conf.TunnelQuotaName)

	session, err := tunnel.CreateDownloadSession(
		conf.ProjectName,
		"mf_test",
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Printf("tunnel endpoint: %+v\n", tunnel.GetEndpoint())

	schema := session.Schema()
	recordCount := session.RecordCount()

	recordReader, err := session.OpenRecordReader(0, recordCount, nil)

	recordReader.Iterator(func(record data.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
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

	if err = recordReader.Close(); err != nil {
		log.Fatalf("%+v", err)
	}
}
