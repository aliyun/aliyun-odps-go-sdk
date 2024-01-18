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
		"json_table",
		//tunnel.SessionCfg.WithPartitionKey("p1=20,p2='hangzhou'"),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordWriter, err := session.OpenRecordWriter(0)
	//schema := session.Schema()
	nullValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.NullType))
	record := []data.Data{
		nullValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	booleanValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.BooleanType))
	booleanValue.SetData(data.Bool(true))
	record = []data.Data{
		booleanValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	objectValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.ObjectType))
	m := map[string]interface{}{"a": 1, "b": "abc"}
	objectValue.SetData(data.Object(m))
	record = []data.Data{
		objectValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	stringValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.StringType))
	stringValue.SetData(data.String("asdfghjkl"))
	record = []data.Data{
		stringValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	intValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.BigIntType))
	intValue.SetData(data.BigInt(12345))
	record = []data.Data{
		intValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	doubleValue := data.NewJsonWithTyp(datatype.NewJsonType(datatype.DoubleType))
	doubleValue.SetData(data.Double(123.456))
	record = []data.Data{
		doubleValue,
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
