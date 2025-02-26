package tunnel_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func TestUploadSession_Overwrite(t *testing.T) {
	tableName := "overwrite_test"
	tableSchema := tableschema.NewSchemaBuilder().Name(tableName).Column(
		tableschema.Column{Name: "c1", Type: datatype.BigIntType},
	).Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).Build()

	err := odpsIns.Schema(SchemaName).Tables().Create(tableSchema, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	session, err := tunnelIns.CreateUploadSession(ProjectName, tableName, tunnel.SessionCfg.WithSchemaName(SchemaName), tunnel.SessionCfg.Overwrite())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := session.OpenRecordWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	record := data.NewRecord(2)
	record[0] = data.BigInt(1)
	record[1] = data.String("1")

	err = writer.Write(record)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = session.Commit([]int{0})
	if err != nil {
		t.Fatal(err)
	}
	ins, err := odpsIns.ExecSQl("select count(*) from " + ProjectName + "." + SchemaName + "." + tableName + ";")
	if err != nil {
		t.Fatal(err)
	}
	err = ins.WaitForSuccess()
	if err != nil {
		t.Fatal(err)
	}
	result, err := ins.GetResult()
	if result[0].Content() != "\"_c0\"\n1\n" {
		t.Fatal("Expected 1, got " + result[0].Content())
	}
}

func TestUploadSession_CreatePartition(t *testing.T) {
	t.Skip("跳过此测试：服务端尚未发布该功能")

	tableName := "upload_create_partition_test"
	tableSchema := tableschema.NewSchemaBuilder().Name(tableName).Column(
		tableschema.Column{Name: "c1", Type: datatype.BigIntType},
	).Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).PartitionColumn(tableschema.Column{Name: "p1", Type: datatype.StringType}).Build()

	err := odpsIns.Tables().Create(tableSchema, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	session, err := tunnelIns.CreateUploadSession(ProjectName, tableName, tunnel.SessionCfg.WithPartitionKey("p1='hello'"), tunnel.SessionCfg.WithCreatePartition())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := session.OpenRecordWriter(0)
	if err != nil {
		t.Fatal(err)
	}
	record := data.NewRecord(2)
	record[0] = data.BigInt(1)
	record[1] = data.String("1")

	err = writer.Write(record)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = session.Commit([]int{0})
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := odpsIns.Table(tableName).GetPartitions()
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 {
		t.Fatal("Expected 1, got " + string(rune(len(partitions))))
	}
}
