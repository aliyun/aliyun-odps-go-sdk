package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	odpsdata "github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
	"log"
	"os"
	"time"
)

func main() {
	// 用到的table的schema如下
	// create table if not exists user_test (
	//   name string,score int,birthday datetime,addresses array<string>
	// )
	// partitioned by (age int,hometown string)

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

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"user_test",
		tunnel.SessionCfg.WithPartitionKey("age=20,hometown='hangzhou'"),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	schema := session.ArrowSchema()

	dataSet := [][]interface{}{
		{"n1", int64(65), "2021-12-23 10:20:00", []interface{}{[]string{"cloud_silly", "efc"}, "basketball"}},
		{"n2", int64(66), "2021-12-24 11:20:00", []interface{}{[]string{"west_river"}, "football"}},
	}

	writeBlock := func(blockId int, data [][]interface{}) error {
		recordWriter, err := session.OpenRecordArrowWriter(blockId)
		if err != nil {
			return errors.WithStack(err)
		}

		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, schema)
		defer recordBuilder.Release()

		for i, field := range schema.Fields() {
			fieldBuilder := recordBuilder.Field(i)

			for _, d := range data {
				switch field.Name {
				case "name":
					builder := fieldBuilder.(*array.StringBuilder)
					builder.Append(d[i].(string))
				case "score":
					builder := fieldBuilder.(*array.Int64Builder)
					builder.Append(d[i].(int64))
				case "birthday":
					builder := fieldBuilder.(*array.TimestampBuilder)
					l, _ := time.LoadLocation("Local")
					t, _ := time.ParseInLocation(odpsdata.DateTimeFormat, d[i].(string), l)
					builder.Append(arrow.Timestamp(t.UnixMilli()))
				case "extra":
					builder := fieldBuilder.(*array.StructBuilder)
					fb1 := builder.FieldBuilder(0).(*array.ListBuilder)
					sb := fb1.ValueBuilder().(*array.StringBuilder)
					fb2 := builder.FieldBuilder(1).(*array.StringBuilder)
					dd := d[i].([]interface{})

					builder.Append(true)
					fb1.Append(true)
					sb.AppendValues(dd[0].([]string), nil)
					fb2.Append(dd[1].(string))
				}
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

	n := len(dataSet)

	if n >= 2 {
		mid := n / 2

		c := make(chan error, 2)

		go func() {
			err := writeBlock(1, dataSet[0:mid])
			c <- err
		}()

		go func() {
			err := writeBlock(2, dataSet[mid:])
			c <- err
		}()

		for i := 0; i < 2; i++ {
			err := <-c
			if err != nil {
				log.Fatalf("%+v", err)
			}
		}
	} else {
		err := writeBlock(1, dataSet)
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	err = session.Commit([]int{1, 2})

	if err != nil {
		log.Fatalf("%+v", err)
	}
}
