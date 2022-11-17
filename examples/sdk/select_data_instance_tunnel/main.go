package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
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
	sql := "select * from data_type_demo where p1=20 and p2='hangzhou';"

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	lv := odpsIns.LogView()
	lvUrl, err := lv.GenerateLogView(ins, 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(lvUrl)

	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateInstanceResultDownloadSession(project.Name(), ins.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader, err := session.OpenRecordReader(0, session.RecordCount(), 1000, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader.Iterator(func(record data.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}
		if record.Len() != 18 {
			log.Fatalf("only select 3 columns, but get %d", record.Len())
		}

		fmt.Printf("%v\n", record)
	})
}
