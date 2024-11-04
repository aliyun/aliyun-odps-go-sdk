package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	// Get config from ini file
	conf, err := odps.NewConfigFromIni("./config.ini")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Initialize Odps
	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()

	// Get the endpoint of tunnel service
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: " + tunnelEndpoint)

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	session, err := tunnelIns.CreateStreamUploadSession(
		project.Name(),
		"all_types_demo",
		tunnel.SessionCfg.WithPartitionKey("p1=23,p2='hangzhou'"),
		tunnel.SessionCfg.WithCreatePartition(), // create new partition if the specific partition is not existed
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	packWriter := session.OpenRecordPackWriter()

	for i := 0; i < 2; i++ {
		record, err := makeRecord(session.Schema())
		if err != nil {
			log.Fatalf("%+v", err)
		}

		// Append data to packWriter until the data size reach a threshold
		for packWriter.DataSize() < 65536 {
			err = packWriter.Append(record)
			if err != nil {
				log.Fatalf("%+v", err)
			}
		}

		// Flush the data to make the data visible
		traceId, recordCount, bytesSend, err := packWriter.Flush()
		if err != nil {
			log.Fatalf("%+v", err)
		}

		fmt.Printf(
			"success to upload data with traceId=%s, record count=%d, record bytes=%d\n",
			traceId, recordCount, bytesSend,
		)
	}
}

func makeRecord(schema *tableschema.TableSchema) (data.Record, error) {
	varchar, _ := data.NewVarChar(500, "varchar")
	char, _ := data.NewVarChar(254, "char")
	s := data.String("hello world")
	date, _ := data.NewDate("2022-10-19")
	datetime, _ := data.NewDateTime("2022-10-19 17:00:00")
	timestamp, _ := data.NewTimestamp("2022-10-19 17:00:00.000")
	timestampNtz, _ := data.NewTimestampNtz("2022-10-19 17:00:00.000")

	mapType := schema.Columns[17].Type.(datatype.MapType)
	mapData := data.NewMapWithType(mapType)
	err := mapData.Set("hello", 1)
	if err != nil {
		return nil, err
	}

	err = mapData.Set("world", 2)
	if err != nil {
		return nil, err
	}

	arrayType := schema.Columns[18].Type.(datatype.ArrayType)
	arrayData := data.NewArrayWithType(arrayType)
	err = arrayData.Append("a")
	if err != nil {
		return nil, err
	}

	err = arrayData.Append("b")
	if err != nil {
		return nil, err
	}

	structType := schema.Columns[19].Type.(datatype.StructType)
	structData := data.NewStructWithTyp(structType)

	arr := data.NewArrayWithType(structType.FieldType("arr").(datatype.ArrayType))
	err = arr.Append("x")
	if err != nil {
		return nil, err
	}
	err = arr.Append("y")
	if err != nil {
		return nil, err
	}
	err = structData.SetField("arr", arr)
	if err != nil {
		return nil, err
	}
	err = structData.SetField("name", "tom")
	if err != nil {
		return nil, err
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
		timestampNtz,
		data.Bool(true),
		mapData,
		arrayData,
		structData,
	}

	return record, nil
}
