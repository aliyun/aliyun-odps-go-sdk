package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
	"os"
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

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	upload := func() {
		session, err := tunnelIns.CreateStreamUploadSession(
			project.Name(),
			"data_type_demo",
			tunnel.SessionCfg.WithPartitionKey("p1=10,p2='beijing'"),
			tunnel.SessionCfg.WithCreatePartition(),
		)

		if err != nil {
			log.Fatalf("%+v", err)
		}

		packWriter := session.OpenRecordPackWriter()
		schema := session.Schema()

		varchar, _ := data.NewVarChar(1000, "varchar")
		char, _ := data.NewVarChar(100, "char")
		s := data.String("alibaba")
		date, _ := data.NewDate("2022-10-19")
		datetime, _ := data.NewDateTime("2022-10-19 17:00:00")
		timestamp, _ := data.NewTimestamp("2022-10-19 17:00:00.000")
		structType := schema.Columns[15].Type.(datatype.StructType)
		structData := data.NewStructWithTyp(&structType)
		arrayType := structType.FieldType("arr").(datatype.ArrayType)
		arr := data.NewArrayWithType(&arrayType)
		_ = arr.Append("a")
		_ = arr.Append("b")
		_ = structData.SetField("arr", arr)
		_ = structData.SetField("name", "tom")

		record := []data.Data{
			data.TinyInt(1),
			data.SmallInt(32767),
			data.Int(100),
			data.BigInt(100000000000),
			data.Binary("binary"),
			data.Float(3.14),
			data.Double(3.1415926),
			data.NewDecimal(38, 18, "3.1415926"),
			varchar,
			char,
			&s,
			date,
			datetime,
			timestamp,
			data.Bool(true),
			structData,
		}

		// 缓冲一定大小的数据后一次上传到服务器
		for packWriter.DataSize() < 64*1024 {
			err = packWriter.Append(record)
			if err != nil {
				log.Fatalf("%+v", err)
			}
		}

		traceId, err := packWriter.Flush()
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println("success upload data with traceId ", traceId)
	}

	for i := 0; i < 3; i++ {
		upload()
	}
}
