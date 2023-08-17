package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
	"os"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint(conf.QuotaName)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("endpoint: " + tunnelEndpoint)

	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnel.CreateDownloadSession(
		project.Name(),
		"mf_test",
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
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
