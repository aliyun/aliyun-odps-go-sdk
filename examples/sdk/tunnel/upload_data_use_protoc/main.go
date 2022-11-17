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
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"data_type_demo",
		tunnel.SessionCfg.WithPartitionKey("p1=20,p2='hangzhou'"),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordWriter, err := session.OpenRecordWriter(0)
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

	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
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
