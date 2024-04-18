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
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: " + tunnelEndpoint)
	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	// sql of creating table json_demo :
	// CREATE TABLE IF NOT EXISTS json_demo(id int, json JSON);
	tableName := "json_demo"
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		tableName,
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordWriter, err := session.OpenRecordWriter(0)

	createJson := func(value interface{}) *data.Json {
		jsonObj, err := data.NewJson(value)

		if err != nil {
			log.Fatalf("%+v", err)
		}

		return jsonObj
	}

	records := [][]data.Data{
		{
			data.Int(1),
			createJson(nil),
		},
		{
			data.Int(2),
			createJson(true),
		},
		{
			data.Int(3),
			createJson([]interface{}{"abc", "dfg"}),
		},
		{
			data.Int(4),
			createJson(
				struct {
					Age  int
					Name string
				}{
					Age:  20,
					Name: "Ali",
				}),
		},
		{
			data.Int(5),
			createJson("I am a string"),
		},
		{
			data.Int(6),
			createJson(""),
		},
		{
			data.Int(7),
			createJson(123),
		},
		{
			data.Int(8),
			createJson(123.467),
		},
		{
			data.Int(9),
			nil,
		},
	}

	for _, record := range records {
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
