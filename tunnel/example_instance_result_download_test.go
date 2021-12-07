package tunnel_test

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/tunnel"
	"io"
)

func Example_tunnel_download_instance_result()  {
	var account = odps.AliyunAccountFromEnv()
	var endpoint = odps.LoadEndpointFromEnv()
	var odpsIns = odps.NewOdps(&account, endpoint)

	projectName := "project_1"
	odpsIns.SetDefaultProjectName(projectName)
	project := odpsIns.DefaultProject()
	tunnelIns, err := tunnel.NewTunnelFromProject(project)
	if err != nil {
		println(err.Error())
		return
	}

	ins, err := odpsIns.RunSQlTask("select * from data_type_demo;")
	if err != nil {
		println(err.Error())
		return
	}

	err = ins.WaitForSuccess()
	if err != nil {
		println(err.Error())
		return
	}

	session, err := tunnelIns.CreateInstanceResultDownloadSession(projectName, ins.Id())
	if err != nil {
		println(err.Error())
		return
	}

	columnNames := []string {
		"ti", "si", "i", "bi", "b", "f", "d", "dc", "vc", "c", "s", "da", "dat", "t", "bl",
	}

	reader, err := session.OpenRecordReader(0, 100, 100, columnNames)
	if err != nil {
		println(err.Error())
	}

	record, err := reader.Read()
	if err != nil && err != io.EOF {
		println(err.Error())
	} else {
		for i, n := 0, record.Len(); i < n; i ++ {
			f := record.Get(i)
			println(f.String())
		}
	}
	// Output:
}