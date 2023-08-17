package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
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
	fmt.Println("tunnelEndpoint: " + tunnelEndpoint)

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"mf_test",
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordWriter, err := session.OpenRecordWriter(0)
	str := data.String("maxcompute")

	record := []data.Data{
		data.Int(100),
		str,
	}

	for i := 0; i < 10; i++ {
		err = recordWriter.Write(record)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	err = recordWriter.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = session.Commit([]int{0})
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
