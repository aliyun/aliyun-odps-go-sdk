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
	sql := "select name, birthday, extra from user_test;"

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	tunnel := tunnel2.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnel.CreateInstanceResultDownloadSession(project.Name(), ins.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader, err := session.OpenRecordReader(0, session.RecordCount(), 1000, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for recordOrErr := range reader.Iterator() {
		if recordOrErr.IsErr() {
			log.Fatalf("%+v", recordOrErr.Error)
		}

		record := recordOrErr.Data.(data.Record)
		if record.Len() != 3 {
			log.Fatalf("only select 3 columns, but get %d", record.Len())
		}

		fmt.Printf("name:%s, birthday:%s, extra:%s\n", record[0], record[1], record[2])
	}
}
