package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	// Get config from ini file
	conf, err := odps.NewConfigFromIni("./config.ini")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Initialize Odps
	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()

	// Get the endpoint of tunnel service
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: " + tunnelEndpoint)

	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)

	// Create a download session,  specify the table/partition to be read
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

	start := 0
	step := 100001
	total := 0
	schema := session.Schema()

	// Iterate the reader step by step
	for start < recordCount {
		reader, err := session.OpenRecordReader(start, step, nil)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		count := 0
		err = reader.Iterator(func(record data.Record, _err error) {
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
