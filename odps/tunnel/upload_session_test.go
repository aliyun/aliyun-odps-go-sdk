package tunnel_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/stretchr/testify/assert"
)

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

	var httpError restclient.HttpNotOk
	if errors.As(err, &httpError) {
		if httpError.StatusCode == 500 {
			t.Log("Success catch error")
		}
	}
	assert.Error(t, err)
}
