package tunnel_test

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
)

func Example_tunnel_download_instance_result() {
	var account = account2.AliyunAccountFromEnv()
	var endpoint = restclient.LoadEndpointFromEnv()
	var odpsIns = odps.NewOdps(account, endpoint)

	projectName := "project_1"
	odpsIns.SetDefaultProjectName(projectName)
	project := odpsIns.DefaultProject()
	tunnelIns, err := tunnel.NewTunnelFromProject(project)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	ins, err := odpsIns.ExecSQl("select * from data_type_demo;")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	session, err := tunnelIns.CreateInstanceResultDownloadSession(projectName, ins.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	//columnNames := []string {
	//	"ti", "si", "i", "bi", "b", "f", "d", "dc", "vc", "c", "s", "da", "dat", "t", "bl",
	//}

	// set columnNames=nil for get all the columns
	reader, err := session.OpenRecordReader(0, 100, 0, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// 用read()逐个读取
	//record, err := reader.Read()
	//if err != nil && err != io.EOF {
	//	println(err.Error())
	//} else {
	//	for i, n := 0, record.Len(); i < n; i ++ {
	//		f := record.Get(i)
	//		println(f.String())
	//	}
	//}

	// 或用iterator遍历读取
	for recordOrErr := range reader.Iterator() {
		if recordOrErr.IsErr() {
			log.Fatalf("%+v", recordOrErr.Error)
		}

		record := recordOrErr.Data.(data.Record)

		for i, n := 0, record.Len(); i < n; i++ {
			f := record.Get(i)
			println(f.String())
		}
	}

	// Output:
}
