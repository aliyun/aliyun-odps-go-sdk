package restclient

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

// MockAccount 实现 account.Account 接口
type MockAccount struct{}

func (a MockAccount) SignRequest(req *http.Request, endpoint string) error {
	req.Header.Set("Authorization", "MockToken")
	return nil
}

func (a MockAccount) GetType() account.Provider {
	return account.Aliyun
}

// 测试结构体
type TestResponse struct {
	Message string `xml:"Message"`
}

func TestRestClient_GetWithModel_Success(t *testing.T) {
	// 创建模拟服务器
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "MockToken", r.Header.Get("Authorization"))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<TestResponse><Message>Hello</Message></TestResponse>`))
	}))
	defer ts.Close()

	client := NewOdpsRestClient(MockAccount{}, ts.URL)
	var resp TestResponse

	err := client.GetWithModel("test", url.Values{}, nil, &resp)
	assert.NoError(t, err)
	assert.Equal(t, "Hello", resp.Message)
}
