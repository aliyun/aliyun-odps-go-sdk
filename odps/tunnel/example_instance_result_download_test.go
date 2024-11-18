// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel_test

import (
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func Example_tunnel_download_instance_result() {
	account := account2.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()
	odpsIns := odps.NewOdps(account, endpoint)

	projectName := "go_sdk_regression_testing"
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

	// columnNames := []string {
	//	"ti", "si", "i", "bi", "b", "f", "d", "dc", "vc", "c", "s", "da", "dat", "t", "bl",
	// }

	// set columnNames=nil for get all the columns
	reader, err := session.OpenRecordReader(0, 100, 0, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// 用read()逐个读取
	// record, err := reader.Read()
	// if err != nil && err != io.EOF {
	//	println(err.Error())
	// } else {
	//	for i, n := 0, record.Len(); i < n; i ++ {
	//		f := record.Get(i)
	//		println(f.String())
	//	}
	// }

	// 或用iterator遍历读取
	err = reader.Iterator(func(record data.Record, err error) {
		if err != nil {
			return
		}

		for i, n := 0, record.Len(); i < n; i++ {
			f := record.Get(i)
			println(f.String())
		}
	})
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
