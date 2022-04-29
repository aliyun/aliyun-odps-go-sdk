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

	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnel.CreateDownloadSession(
		project.Name(),
		"user_test",
		tunnel2.SessionCfg.WithPartitionKey("age=20,hometown='hangzhou'"),
		tunnel2.SessionCfg.DisableArrow(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	fmt.Printf("record count is %d", recordCount)

	reader, err := session.OpenRecordReader(0, recordCount, nil)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader.Iterator(func(record data.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}
		if record.Len() != 3 {
			log.Fatalf("only select 3 columns, but get %d", record.Len())
		}

		fmt.Printf("name:%s, birthday:%s, extra:%s\n", record[0], record[1], record[2])
	})
}
