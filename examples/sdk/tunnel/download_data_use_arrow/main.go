package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
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
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnel.CreateDownloadSession(
		project.Name(),
		"user_test",
		tunnel2.SessionCfg.WithPartitionKey("age=20,hometown='hangzhou'"),
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

	n := 0
	for rec := range reader.Iterator() {
		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	}

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

}
