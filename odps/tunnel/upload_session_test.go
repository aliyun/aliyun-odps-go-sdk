package tunnel_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func TestUploadSession_Overwrite(t *testing.T) {
	requireLiveTunnel(t)
	tableName := livetest.Fixture("overwrite_test")
	tableSchema := tableschema.NewSchemaBuilder().Name(tableName).Column(
		tableschema.Column{Name: "c1", Type: datatype.BigIntType},
	).Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).Build()

	tables := odpsIns.Schema(SchemaName).Tables()
	err := tables.Create(tableSchema, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// This test commits an Overwrite() session and then asserts the table holds
	// exactly one row. Without dropping the table on the way out, the next run
	// that shares the name decides what "one row" means.
	t.Cleanup(func() {
		if err := tables.Delete(tableName, true); err != nil {
			println("live fixture cleanup: drop table " + SchemaName + "." + tableName + ": " + err.Error())
		}
	})

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
	requireLiveTunnel(t)
	t.Skip("跳过此测试：服务端尚未发布该功能")

	tableName := livetest.Fixture("upload_create_partition_test")
	tableSchema := tableschema.NewSchemaBuilder().Name(tableName).Column(
		tableschema.Column{Name: "c1", Type: datatype.BigIntType},
	).Column(tableschema.Column{Name: "c2", Type: datatype.StringType}).PartitionColumn(tableschema.Column{Name: "p1", Type: datatype.StringType}).Build()

	err := odpsIns.Tables().Create(tableSchema, true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := odpsIns.Tables().Delete(tableName, true); err != nil {
			println("live fixture cleanup: drop table " + tableName + ": " + err.Error())
		}
	})

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

type mockRestClient struct {
	mockDo func(req *http.Request) (*http.Response, error)
}

func (m *mockRestClient) Do(req *http.Request) (*http.Response, error) {
	return m.mockDo(req)
}

func TestCommit_RetrySuccess(t *testing.T) {
	attempts := 0
	mockClient := &mockRestClient{
		mockDo: func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return &http.Response{StatusCode: 500}, nil
			}
			return &http.Response{StatusCode: 200}, nil
		},
	}

	err := tunnel.Retry(func() error {
		res, err := mockClient.Do(nil)
		if err != nil {
			return err
		}
		if res.StatusCode/100 != 2 {
			return restclient.NewHttpNotOk(res)
		} else {
			if res.Body != nil {
				_ = res.Body.Close()
			}
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)
}

func TestCommit_RetryFail(t *testing.T) {
	mockClient := &mockRestClient{
		mockDo: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 500}, nil
		},
	}

	err := tunnel.Retry(func() error {
		res, err := mockClient.Do(nil)
		if err != nil {
			return err
		}
		if res.StatusCode/100 != 2 {
			return restclient.NewHttpNotOk(res)
		} else {
			if res.Body != nil {
				_ = res.Body.Close()
			}
		}
		return nil
	})

	if httpError, ok := err.(restclient.HttpError); ok {
		if httpError.StatusCode == 500 {
			t.Log("Success catch error")
		}
	}
	assert.Error(t, err)
}
