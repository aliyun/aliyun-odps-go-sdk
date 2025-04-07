package tunnel_test

import (
	"net/http"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/stretchr/testify/assert"
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
