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
	"fmt"
	"log"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func Example_tunnel_upload_arrow() {
	err := odpsIns.Tables().Get("sale_detail").AddPartition(true, "sale_date=202111/region=hangzhou")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	tunnelIns.SetHttpTimeout(10 * time.Second)

	session, err := tunnelIns.CreateUploadSession(
		ProjectName,
		"sale_detail",
		tunnel2.SessionCfg.WithPartitionKey("sale_date='202111',region='hangzhou'"),
		// tunnel.SessionCfg.WithSnappyFramedCompressor(),
		// tunnel.SessionCfg.WithDeflateCompressor(tunnel.DeflateLevel.DefaultCompression),
		tunnel2.SessionCfg.WithDefaultDeflateCompressor(),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	schema := session.ArrowSchema()

	type SaleDetailData struct {
		ShopNames  []string
		CustomIDs  []string
		totalPrice []float64
	}

	rawData := []SaleDetailData{
		{
			[]string{"sun", "moon", "earth"},
			[]string{"fixed_start1", "satellite1", "planet3"},
			[]float64{10000.032, 200.00, 1500.232},
		},
		{
			[]string{"mars", "venus"},
			[]string{"planet4", "planet2"},
			[]float64{1000.1, 1232.2},
		},
		{
			[]string{"songjiang", "wusong"},
			[]string{"liangshan1", "liangshan2"},
			[]float64{100.13, 232.2},
		},
	}

	blockIds := make([]int, len(rawData))

	writeBlock := func(blockId int, data SaleDetailData) error {
		recordWriter, err := session.OpenRecordArrowWriter(blockId)
		if err != nil {
			return errors.WithStack(err)
		}

		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, schema)
		defer recordBuilder.Release()

		for i, field := range schema.Fields() {
			fieldBuilder := recordBuilder.Field(i)

			switch field.Name {
			case "shop_name":
				builder := fieldBuilder.(*array.StringBuilder)
				builder.AppendValues(data.ShopNames, nil)
			case "custom_id":
				builder := fieldBuilder.(*array.StringBuilder)
				builder.AppendValues(data.CustomIDs, nil)
			case "total_price":
				builder := fieldBuilder.(*array.Float64Builder)
				builder.AppendValues(data.totalPrice, nil)
			default:
				log.Fatalf("unknown field: %s", field.Name)
			}
		}

		record := recordBuilder.NewRecord()
		defer record.Release()

		err = recordWriter.WriteArrowRecord(record)
		if err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(recordWriter.Close())
	}

	wait := make(chan error, len(rawData))

	for i, n := 0, len(rawData); i < n; i++ {
		i := i
		blockIds[i] = i

		go func() {
			err := writeBlock(i, rawData[i])

			wait <- err
		}()
	}

	for i, n := 0, len(rawData); i < n; i++ {
		e := <-wait
		if e != nil {
			log.Fatalf("%+v", err)
			return
		}
	}

	err = session.Commit(blockIds)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func Example_tunnel_download_arrow_simple() {
	session, err := tunnelIns.CreateDownloadSession(
		ProjectName,
		// "upload_sample_arrow",
		"has_struct",
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordArrowReader(0, 2, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func Example_tunnel_download_arrow_with_partition() {
	session, err := tunnelIns.CreateDownloadSession(
		"go_sdk_regression_testing",
		"sale_detail",
		tunnel2.SessionCfg.WithPartitionKey("sale_date='202111',region='hangzhou'"),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordArrowReader(
		0, 1000,
		[]string{"shop_name", "total_price"})
	if err != nil {
		log.Fatalf("%+v", err)
	}

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func ExampleTunnel_UploadInSpecificSchema() {
	session, err := tunnelIns.CreateUploadSession(ProjectName, "test", tunnel2.SessionCfg.WithSchemaName("new_Schema"))
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
	// Output:
}

func ExampleTunnel_DownloadInSpecificSchema() {
	session, err := tunnelIns.CreateDownloadSession(ProjectName, "test", tunnel2.SessionCfg.WithSchemaName("new_Schema"))
	println(session.Id)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	reader, err := session.OpenRecordReader(0, 10, nil)
	read, err := reader.Read()
	println(read.String())
	// Output:
}

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
