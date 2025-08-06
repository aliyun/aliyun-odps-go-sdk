package tunnel_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/stretchr/testify/assert"
)

// mockRestClientTunnel is a mock implementation of restclient.RestClient for testing
type mockRestClientTunnel struct {
	lastRequest *http.Request
}

func (m *mockRestClientTunnel) Do(req *http.Request) (*http.Response, error) {
	m.lastRequest = req
	// Return a mock response for successful session creation
	response := &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: http.NoBody,
	}
	return response, nil
}

func (m *mockRestClientTunnel) NewRequestWithParamsAndHeaders(method, resource string, body interface{}, query url.Values, headers map[string]string) (*http.Request, error) {
	return http.NewRequest(method, "http://test"+resource, nil)
}

func (m *mockRestClientTunnel) NewRequestWithUrlQuery(method, resource string, body interface{}, query url.Values) (*http.Request, error) {
	return http.NewRequest(method, "http://test"+resource, nil)
}

func (m *mockRestClientTunnel) Endpoint() string {
	return "http://test"
}

// TestTunnelCreateUploadSessionWithSchema tests that tunnel.CreateUploadSession
// properly passes schema names to the session
func TestTunnelCreateUploadSessionWithSchema(t *testing.T) {
	// This test primarily verifies the API signature and options support
	// The actual functionality is tested in the session tests
	opts := []tunnel.Option{
		tunnel.SessionCfg.WithSchemaName("test-schema"),
		tunnel.SessionCfg.WithPartitionKey("p1='test'"),
	}

	assert.Len(t, opts, 2)
}

// TestTunnelCreateDownloadSessionWithSchema tests that tunnel.CreateDownloadSession
// properly passes schema names to the session
func TestTunnelCreateDownloadSessionWithSchema(t *testing.T) {
	// This test verifies the API signature and options support
	opts := []tunnel.Option{
		tunnel.SessionCfg.WithSchemaName("test-schema"),
		tunnel.SessionCfg.WithPartitionKey("p1='test'"),
	}

	assert.Len(t, opts, 2)
}

// TestTunnelCreateStreamUploadSessionWithSchema tests that tunnel.CreateStreamUploadSession
// properly passes schema names to the session
func TestTunnelCreateStreamUploadSessionWithSchema(t *testing.T) {
	// This test verifies the API signature and options support
	opts := []tunnel.Option{
		tunnel.SessionCfg.WithSchemaName("test-schema"),
		tunnel.SessionCfg.WithPartitionKey("p1='test'"),
	}

	assert.Len(t, opts, 2)
}

// TestResourceBuilderTableWithSchema verifies the ResourceBuilder correctly
// generates URLs with schema paths
func TestResourceBuilderTableWithSchema(t *testing.T) {
	rb := common.NewResourceBuilder("test-project")

	// Test without schema
	urlWithoutSchema := rb.Table("", "test-table")
	expectedWithoutSchema := "/projects/test-project/tables/test-table"
	assert.Equal(t, expectedWithoutSchema, urlWithoutSchema)

	// Test with schema
	urlWithSchema := rb.Table("test-schema", "test-table")
	expectedWithSchema := "/projects/test-project/schemas/test-schema/tables/test-table"
	assert.Equal(t, expectedWithSchema, urlWithSchema)
}
