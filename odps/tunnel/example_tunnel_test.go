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

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

// The table "test" and the schema "new_Schema" below are preconditions of the
// regression project, not fixtures of a run: nothing in this repository creates
// them, so they stay spelled as they are and an unprovisioned project fails
// here rather than silently testing a name nobody made.
func ExampleTunnel_CreateUploadSession_withSchema() {
	session, err := tunnelIns.CreateUploadSession(ProjectName, "test",
		tunnel2.SessionCfg.WithSchemaName("new_Schema"),
		tunnel2.SessionCfg.WithDefaultZstdCompressor(),
	)
	println(session.Id)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	writer, err := session.OpenRecordWriter(0)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	record := data.NewRecord(1)
	record[0] = data.BigInt(1)

	for i := 0; i < 10; i++ {
		err = writer.Write(record)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = session.Commit([]int{0})
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func ExampleTunnel_CreateDownloadSession_withSchema() {
	session, err := tunnelIns.CreateDownloadSession(ProjectName, "test", tunnel2.SessionCfg.WithSchemaName("new_Schema"))
	println(session.Id)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	reader, err := session.OpenRecordReader(0, 10, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	record, err := reader.Read()
	println(record.Len())
}

func ExampleTunnel_Preview() {
	tableName := livetest.Fixture("ExampleTunnel_Preview")
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
}
