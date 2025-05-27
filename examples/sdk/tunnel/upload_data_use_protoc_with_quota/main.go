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

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	tunnelIns := tunnel.NewTunnel(odpsIns)
	tunnelIns.SetQuotaName(conf.TunnelQuotaName)

	session, err := tunnelIns.CreateUploadSession(
		conf.ProjectName,
		"mf_test",
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: " + tunnelIns.GetEndpoint())

	recordWriter, err := session.OpenRecordWriter(0)
	str := data.String("maxcompute")

	record := []data.Data{
		data.Int(100),
		str,
	}

	for i := 0; i < 1; i++ {
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
