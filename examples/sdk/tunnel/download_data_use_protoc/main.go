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

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: ", tunnelEndpoint)

	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnel.CreateDownloadSession(
		project.Name(),
		"all_types_demo",
		tunnel2.SessionCfg.WithPartitionKey("p1=20,p2='hangzhou'"),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	fmt.Printf("record count is %d\n", recordCount)

	reader, err := session.OpenRecordReader(0, recordCount, nil)
	schema := session.Schema()

	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader.Iterator(func(record data.Record, err error) {
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

	if err = reader.Close(); err != nil {
		log.Fatalf("%+v", err)
	}
}
