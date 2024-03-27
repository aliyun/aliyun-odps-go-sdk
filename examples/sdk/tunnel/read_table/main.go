package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
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

	tableTunnel := tunnel2.NewTunnel(odpsIns, conf.TunnelEndpoint)
	ts := odpsIns.Table("all_types_demo")
	err = ts.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	reader, err := tableTunnel.ReadTable(&ts, "", 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		for c, col := range rec.Columns() {
			fmt.Printf("rec[%d][%d]: %v\n", n, c, col)
		}
		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
