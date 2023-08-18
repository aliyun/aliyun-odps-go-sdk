package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
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
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"all_types_demo",
		tunnel.SessionCfg.WithPartitionKey("p1=20,p2='hangzhou'"),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordWriter, err := session.OpenRecordWriter(0)
	schema := session.Schema()

	varchar, _ := data.NewVarChar(500, "varchar")
	char, _ := data.NewVarChar(254, "char")
	s := data.String("hello world")
	date, _ := data.NewDate("2022-10-19")
	datetime, _ := data.NewDateTime("2022-10-19 17:00:00")
	timestamp, _ := data.NewTimestamp("2022-10-19 17:00:00.000")

	mapType := schema.Columns[15].Type.(datatype.MapType)
	mapData := data.NewMapWithType(mapType)
	err = mapData.Set("hello", 1)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = mapData.Set("world", 2)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	arrayType := schema.Columns[16].Type.(datatype.ArrayType)
	arrayData := data.NewArrayWithType(arrayType)
	err = arrayData.Append("a")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = arrayData.Append("b")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	structType := schema.Columns[17].Type.(datatype.StructType)
	structData := data.NewStructWithTyp(structType)

	arr := data.NewArrayWithType(structType.FieldType("arr").(datatype.ArrayType))
	err = arr.Append("x")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	err = arr.Append("y")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	err = structData.SetField("arr", arr)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	err = structData.SetField("name", "tom")
	if err != nil {
		log.Fatalf("%+v", err)
	}

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
		s,
		date,
		datetime,
		timestamp,
		data.Bool(true),
		mapData,
		arrayData,
		structData,
	}

	for i := 0; i < 1; i++ {
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
