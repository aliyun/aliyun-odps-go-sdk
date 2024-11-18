package tunnel_test

import (
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func TestStreamUploadSessionTest_SchemaEvolution(t *testing.T) {
	tableName := "TestStreamUploadSessionTest_SchemaEvolution"

	column1 := tableschema.Column{
		Name: "id",
		Type: datatype.BigIntType,
	}
	column2 := tableschema.Column{
		Name: "id2",
		Type: datatype.StringType,
	}
	tableSchema := tableschema.NewSchemaBuilder().Columns(column1, column2).Name(tableName).Build()
	err := odpsIns.Table(tableName).Delete()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err2 := odpsIns.Tables().Create(tableSchema, true, nil, nil)
	if err2 != nil {
		log.Fatalf("%+v", err2)
	}

	// create a session witch not allow schema mismatch
	// that will throw exception when found real table schema not match the session schema
	session, err2 := tunnelIns.CreateStreamUploadSession(ProjectName, tableName,
		tunnel.SessionCfg.WithAllowSchemaMismatch(false))

	if err2 != nil {
		log.Fatalf("%+v", err2)
	}

	var wg sync.WaitGroup

	// thread 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := session.OpenRecordPackWriter()
		record := data.NewRecord(len(session.Schema().Columns))
		record[0] = data.BigInt(1)
		record[1] = data.String("1")
		log.Println("Open Session.")

		for i := 0; i < 100; i++ {
			for j := 0; j < 100; j++ {
				err := writer.Append(record)
				if err != nil {
					log.Fatal(err)
					return
				}
			}
			_, _, _, err := writer.Flush()
			if err != nil {
				var httpError restclient.HttpError
				errors.As(err, &httpError)

				if httpError.StatusCode == 412 && httpError.ErrorMessage.ErrorCode == "SchemaModified" {
					latestSchemaVersion := httpError.Response.Header.Get("x-odps-tunnel-latest-schema-version")
					log.Println("Schema Mismatch, the latest schema version is " + latestSchemaVersion)
					return
				}
				log.Fatal(err)
				return
			}
			log.Println("Flush Success.")
			time.Sleep(time.Second * 3)
		}
	}()

	// thread 2
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 5)
		ins, err := odpsIns.ExecSQl("alter table " + tableName + " add column id3 string;")
		if err != nil {
			log.Fatalf("%+v", err)
		}
		log.Println(odpsIns.LogView().GenerateLogView(ins, 24))
		err = ins.WaitForSuccess()
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}()

	// 等待两个线程完成
	wg.Wait()
	log.Println("Both threads have completed.")
}
