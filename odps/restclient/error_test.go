package restclient

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

// 测试NewHttpNotOk函数
func TestNewHttpNotOk(t *testing.T) {
	testBody := `{"Code":"NotFound","Message":"Resource not found","RequestId":"12345","HostId":"test.host"}`

	testCases := []struct {
		name           string
		response       *http.Response
		expectedStatus string
		expectedCode   int
	}{
		{
			name: "ValidResponse",
			response: &http.Response{
				Status:     "404 Not Found",
				StatusCode: 404,
				Header: http.Header{
					"x-odps-request-id": []string{"test-req-id"},
				},
				Body: http.NoBody,
			},
			expectedStatus: "404 Not Found",
			expectedCode:   404,
		},
		{
			name: "WithBody",
			response: &http.Response{
				Status:     "500 Internal Server Error",
				StatusCode: 500,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(testBody)),
			},
			expectedStatus: "500 Internal Server Error",
			expectedCode:   500,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewHttpNotOk(tc.response)
			if err.Status != tc.expectedStatus {
				t.Errorf("Expected status %s, got %s", tc.expectedStatus, err.Status)
			}
			if err.StatusCode != tc.expectedCode {
				t.Errorf("Expected code %d, got %d", tc.expectedCode, err.StatusCode)
			}
		})
	}
}

// 测试NewErrorMessage函数
func TestNewErrorMessage(t *testing.T) {
	testCases := []struct {
		name        string
		contentType string
		body        []byte
		expected    *ErrorMessage
	}{
		{
			name:        "ValidJSON",
			contentType: "application/json",
			body:        []byte(`{"Code":"Invalid","Message":"Bad request","RequestId":"req-123","HostId":"host-1"}`),
			expected: &ErrorMessage{
				ErrorCode: "Invalid",
				Message:   "Bad request",
				RequestId: "req-123",
				HostId:    "host-1",
			},
		},
		{
			name:        "ValidXML",
			contentType: "application/xml",
			body:        []byte(`<Error><Code>Invalid</Code><Message>Bad request</Message><RequestId>req-123</RequestId><HostId>host-1</HostId></Error>`),
			expected: &ErrorMessage{
				ErrorCode: "Invalid",
				Message:   "Bad request",
				RequestId: "req-123",
				HostId:    "host-1",
			},
		},
		{
			name:        "InvalidFormat",
			contentType: "text/plain",
			body:        []byte("invalid content"),
			expected:    nil,
		},
		{
			name:        "EmptyBody",
			contentType: "application/json",
			body:        nil,
			expected:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := NewErrorMessage(tc.body)
			if tc.expected == nil {
				if msg != nil {
					t.Errorf("Expected nil, got %v", msg)
				}
				return
			}

			if msg.ErrorCode != tc.expected.ErrorCode {
				t.Errorf("Code mismatch: %s != %s", msg.ErrorCode, tc.expected.ErrorCode)
			}
			if msg.Message != tc.expected.Message {
				t.Errorf("Message mismatch: %s != %s", msg.Message, tc.expected.Message)
			}
		})
	}
}

// 测试Error()方法输出格式
func TestHttpError_Error(t *testing.T) {
	testCases := []struct {
		name     string
		err      HttpError
		expected string
	}{
		{
			name: "WithRequestId",
			err: HttpError{
				Status:     "404 Not Found",
				StatusCode: 404,
				RequestId:  "test-req-id",
				Body:       []byte("Test error body"),
			},
			expected: "requestId=test-req-id\nstatus=404 Not Found\nTest error body",
		},
		{
			name: "WithoutRequestId",
			err: HttpError{
				Status:     "500 Internal Server Error",
				StatusCode: 500,
				Body:       []byte("Server error"),
			},
			expected: "500 Internal Server Error\nServer error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.err.Error()
			if result != tc.expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", tc.expected, result)
			}
		})
	}
}

// 集成测试：模拟完整HTTP响应
func TestIntegration(t *testing.T) {
	xmlBody := `
    <Error>
        <Code>AccessDenied</Code>
        <Message>Permission denied</Message>
        <RequestId>req-xml-123</RequestId>
        <HostId>host-xml</HostId>
    </Error>`

	jsonBody := `
    {
        "Code": "BadRequest",
        "Message": "Invalid parameters",
        "RequestId": "req-json-456",
        "HostId": "host-json"
    }`

	testCases := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{"XML", "application/xml", []byte(xmlBody)},
		{"JSON", "application/json", []byte(jsonBody)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{
				Status:     "403 Forbidden",
				StatusCode: 403,
				Header: http.Header{
					"Content-Type":      []string{tc.contentType},
					"x-odps-request-id": []string{"header-req-id"},
					"X-Odps-Request-Id": []string{"header-req-id"},
				},
				Body: http.NoBody,
			}

			// 模拟响应体
			resp.Body = ioutil.NopCloser(bytes.NewReader(tc.body))

			httpErr := NewHttpNotOk(resp)
			if httpErr.RequestId != "header-req-id" {
				t.Errorf("RequestId mismatch: %s", httpErr.RequestId)
			}

			if httpErr.ErrorMessage == nil {
				t.Fatal("ErrorMessage should not be nil")
			}

			// 验证解析内容
			switch tc.name {
			case "XML":
				if httpErr.ErrorMessage.ErrorCode != "AccessDenied" {
					t.Errorf("XML Code mismatch: %s", httpErr.ErrorMessage.ErrorCode)
				}
			case "JSON":
				if httpErr.ErrorMessage.ErrorCode != "BadRequest" {
					t.Errorf("JSON Code mismatch: %s", httpErr.ErrorMessage.ErrorCode)
				}
			}
		})
	}
}
