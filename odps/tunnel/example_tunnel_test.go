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

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func ExampleTunnel_Preview() {
	tableName := "ExampleTunnel_Preview"
	exists, err := odpsIns.Tables().Get(tableName).Exists()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	if !exists {
		c1 := tableschema.Column{
			Name: "c1",
			Type: datatype.StringType,
		}
		c2 := tableschema.Column{
			Name: "c2",
			Type: datatype.BigIntType,
		}
		p1 := tableschema.Column{
			Name: "p1",
			Type: datatype.StringType,
		}
		p2 := tableschema.Column{
			Name: "p2",
			Type: datatype.StringType,
		}

		tableSchema := tableschema.NewSchemaBuilder().
			Name(tableName).
			Columns(c1, c2).
			PartitionColumns(p1, p2).
			Build()

		err := odpsIns.Tables().Create(tableSchema, true, nil, nil)
		if err != nil {
			log.Fatalf("%+v", err)
		}
		ins, err := odpsIns.ExecSQl("insert into" + tableName + " partition(p1='a',p2='b') values('c1',2);")
		if err != nil {
			log.Fatalf("%+v", err)
		}
		err = ins.WaitForSuccess()
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	ts := odpsIns.Table(tableName)
	err = ts.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	records, err := tunnelIns.Preview(ts, "", 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, record := range records {
		println(record.String())
	}

	records, err = tunnelIns.Preview(ts, "p1='a'/p2='b'", 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, record := range records {
		println(record.String())
	}
	// Output:
}
