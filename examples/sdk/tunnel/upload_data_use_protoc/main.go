package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
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

	// Create an upload session, specify the table/partition to be written
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"all_types_demo",
		tunnel.SessionCfg.WithPartitionKey("p1=20,p2='hangzhou'"),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	writerNum := 3
	blockIds := make([]int, writerNum)
	for i := 0; i < writerNum; i++ {
		blockIds[i] = i
	}

	errChan := make(chan error, writerNum)

	// Upload data concurrently by multiply writers, each writer has a blockId as the identity of the data it writes
	for _, blockId := range blockIds {
		blockId := blockId
		go func() {
			schema := session.Schema()
			record, err := makeRecord(schema)
			if err != nil {
				errChan <- err
				return
			}

			recordWriter, err := session.OpenRecordWriter(blockId)
			if err != nil {
				errChan <- err
				return
			}

			for i := 0; i < 100; i++ {
				err = recordWriter.Write(record)

				if err != nil {
					_ = recordWriter.Close()
					errChan <- err
					return
				}
			}
			err = recordWriter.Close()
			if err == nil {
				fmt.Printf("success to upload %d record, %d bytes\n", recordWriter.RecordCount(), recordWriter.BytesCount())
			}

			errChan <- err
		}()
	}

	// Wait for all writers to finish uploading data
	for i := 0; i < writerNum; i++ {
		err := <-errChan

		if err != nil {
			log.Fatalf("%+v", err)
		}
	}

	// Commit all blocks to finish the upload, then the data will be visible from the table
	err = session.Commit(blockIds)
	log.Println("success to commit all blocks")
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func makeRecord(schema tableschema.TableSchema) (data.Record, error) {
	varchar, _ := data.NewVarChar(500, "varchar")
	char, _ := data.NewVarChar(254, "char")
	s := data.String("hello world")
	date, _ := data.NewDate("2022-10-19")
	datetime, _ := data.NewDateTime("2022-10-19 17:00:00")
	timestamp, _ := data.NewTimestamp("2022-10-19 17:00:00.000")
	timestampNtz, _ := data.NewTimestampNtz("2022-10-19 17:00:00.000")

	mapType := schema.Columns[15].Type.(datatype.MapType)
	mapData := data.NewMapWithType(mapType)
	err := mapData.Set("hello", 1)
	if err != nil {
		return nil, err
	}

	err = mapData.Set("world", 2)
	if err != nil {
		return nil, err
	}

	arrayType := schema.Columns[16].Type.(datatype.ArrayType)
	arrayData := data.NewArrayWithType(arrayType)
	err = arrayData.Append("a")
	if err != nil {
		return nil, err
	}

	err = arrayData.Append("b")
	if err != nil {
		return nil, err
	}

	structType := schema.Columns[17].Type.(datatype.StructType)
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
