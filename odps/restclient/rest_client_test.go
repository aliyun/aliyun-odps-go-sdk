package restclient

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestRestClientPreservesRequestProject(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query url.Values
		want  url.Values
	}{
		{"default", nil, url.Values{"curr_project": {"default"}}},
		{"explicit", url.Values{"curr_project": {"other"}}, url.Values{"curr_project": {"other"}}},
		{"legacy alias", url.Values{"current_project": {"legacy"}}, url.Values{"current_project": {"legacy"}}},
		{"explicit empty", url.Values{"curr_project": {""}}, url.Values{"curr_project": {""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tc.want, r.URL.Query())
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(server.Close)
			client := NewOdpsRestClient(MockAccount{}, server.URL)
			client.SetDefaultProject("default")
			require.NoError(t, client.GetWithParseFunc("/resource", tc.query, nil, nil))
		})
	}
}

func TestRestClientRejectsInvalidRequestsBeforeSending(t *testing.T) {
	client := NewOdpsRestClient(MockAccount{}, "http://[::1")
	body := TestResponse{Message: "request"}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"xml fetch", func() error { return client.GetWithModel("/resource", nil, nil, &TestResponse{}) }},
		{"raw fetch", func() error { return client.GetWithParseFunc("/resource", nil, nil, nil) }},
		{"raw put", func() error { return client.PutWithParseFunc("/resource", nil, nil, nil) }},
		{"xml put", func() error { return client.DoXmlWithParseRes(http.MethodPut, "/resource", nil, nil, body, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.call(), "missing ']'")
		})
	}
	err := client.DoXmlWithParseFunc(http.MethodPost, "/resource", nil, nil, make(chan int), nil)
	var unsupported *xml.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupported)
}

type rejectingSigner struct {
	MockAccount
	err error
}

func (s rejectingSigner) SignRequest(*http.Request, string) error { return s.err }

func TestRestClientPropagatesSigningFailure(t *testing.T) {
	signErr := errors.New("credential renewal failed")
	client := NewOdpsRestClient(rejectingSigner{err: signErr}, "http://127.0.0.1:1")
	err := client.GetWithParseFunc("/resource", nil, nil, nil)
	require.ErrorIs(t, err, signErr)
}

func TestRestClientParsesAndReleasesXMLMutations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "fixture-agent", r.Header.Get("x-odps-user-agent"))
		var body TestResponse
		assert.NoError(t, xml.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "request", body.Message)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<TestResponse><Message>accepted</Message></TestResponse>")
	}))
	t.Cleanup(server.Close)
	client := NewOdpsRestClient(MockAccount{}, server.URL)
	client.SetUserAgent("fixture-agent")
	client.CloseIdleConnections()
	var response TestResponse
	body := TestResponse{Message: "request"}
	require.NoError(t, client.DoXmlWithModel(http.MethodPut, "/resource", nil, body, &response))
	require.Equal(t, "accepted", response.Message)
	require.NoError(t, client.DoXmlWithModel(http.MethodPut, "/resource", nil, body, nil))
	client.CloseIdleConnections()
}

func TestRestClientAcceptsDirectRequestWithoutConstructor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client := RestClient{Account: MockAccount{}, HttpTimeout: time.Second}
	client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	require.NoError(t, client.DoWithParseRes(req, nil))
	client.CloseIdleConnections()
}

func TestRestClientPropagatesHeadersAndStreamsPutBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			assert.Equal(t, "fixture", r.Header.Get("x-odps-fixture"))
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPut:
			assert.Equal(t, "write", r.URL.Query().Get("operation"))
			payload, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, "payload", string(payload))
			w.Header().Set("X-Odps-Request-Id", "accepted")
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	client := NewOdpsRestClient(MockAccount{}, server.URL)
	require.NoError(t, client.GetWithParseFunc("/resource", nil, map[string]string{"x-odps-fixture": "fixture"}, nil))
	var receipt string
	err := client.PutWithParseFunc("/resource", url.Values{"operation": {"write"}}, strings.NewReader("payload"), func(res *http.Response) error {
		receipt = res.Header.Get("X-Odps-Request-Id")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "accepted", receipt)
}
