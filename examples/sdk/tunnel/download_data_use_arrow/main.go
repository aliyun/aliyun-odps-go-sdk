package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v9/arrow/array"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
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

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
