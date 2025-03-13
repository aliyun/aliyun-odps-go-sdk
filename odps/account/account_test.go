package account

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/stretchr/testify/assert"
)

func TestBuildCanonicalString(t *testing.T) {
	acct := NewAliyunAccount("id", "secret")

	req := &http.Request{
		Method: "GET",
		URL:    &url.URL{},
		Header: http.Header{
			common.HttpHeaderDate:        []string{"Mon, 01 Jan 2020 00:00:00 GMT"},
			common.HttpHeaderContentMD5:  []string{"content-md5"},
			common.HttpHeaderContentType: []string{"application/json"},
			"X-Odps-Test-Header":         []string{"value1", "value2"},
			"X-Odps-Another-Header":      []string{"valueA"},
			"Non-ODPS-Header":            []string{"ignored"},
		},
	}

	canonicalString := acct.buildCanonicalString(req, "https://odps.example.com/api")

	println(canonicalString.String())

	expected := "GET\n" +
		"content-md5\n" +
		"application/json\n" +
		"Mon, 01 Jan 2020 00:00:00 GMT\n" +
		"x-odps-another-header:valueA\n" +
		"x-odps-test-header:value1,value2\n"

	assert.Equal(t, expected, canonicalString.String())
}

func TestBuildCanonicalResource(t *testing.T) {
	acct := NewAliyunAccount("id", "secret")

	tests := []struct {
		name     string
		reqPath  string
		endpoint string
		query    url.Values
		want     string
	}{
		{
			name:     "path normalization",
			reqPath:  "/api/v1/resource",
			endpoint: "https://odps.example.com/api",
			want:     "/v1/resource",
		},
		{
			name:     "query parameters",
			reqPath:  "/resource",
			endpoint: "https://odps.example.com",
			query: url.Values{
				"action":  []string{"list"},
				"orderby": []string{"name"},
				"limit":   []string{"10"},
			},
			want: "/resource?action=list&limit=10&orderby=name",
		},
		{
			name:     "mixed parameters",
			reqPath:  "/path",
			endpoint: "https://odps.example.com",
			query: url.Values{
				"b": []string{"2"},
				"a": []string{"1"},
				"c": []string{""},
			},
			want: "/path?a=1&b=2&c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				URL: &url.URL{
					Path: tt.reqPath,
				},
			}
			req.URL.RawQuery = tt.query.Encode()

			got := acct.buildCanonicalResource(req, tt.endpoint)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateSignature(t *testing.T) {
	acct := NewAliyunAccount("my-access-id", "my-secret-key")

	signature := acct.generateSignature([]byte("testdata"))
	println(signature)
	expected := "ODPS my-access-id:iAcVTeAghMtN74BeKwFTj/pgFEA="

	assert.Equal(t, expected, signature)
}

func TestSignRequest(t *testing.T) {
	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Path: "/projects",
		},
		Header: make(http.Header),
	}

	acct := NewAliyunAccount("id", "secret")

	err := acct.SignRequest(req, "https://odps.example.com")
	assert.NoError(t, err)
	assert.Contains(t, req.Header.Get(common.HttpHeaderAuthorization), "ODPS id:")
}

func TestEnvironmentCredentials(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env-id")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env-secret")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "token")

	t.Run("AliyunAccountFromEnv", func(t *testing.T) {
		acct := AliyunAccountFromEnv()
		assert.Equal(t, "env-id", acct.AccessId())
		assert.Equal(t, "env-secret", acct.AccessKey())
	})

	t.Run("AccountFromEnv with token", func(t *testing.T) {
		acct := AccountFromEnv()
		stsAcct, ok := acct.(*StsAccount)
		assert.True(t, ok)
		credential, err := stsAcct.Credential()
		assert.NoError(t, err)
		assert.Equal(t, "env-id", *credential.AccessKeyId)
		assert.Equal(t, "token", *credential.SecurityToken)
	})
}

func TestCanonicalHeaders(t *testing.T) {
	headers := http.Header{
		"X-Odps-Test":    []string{"value1", "value2"},
		"X-Odps-Another": []string{"valueA"},
		"X-Odps-Multi":   []string{"a", "b", "c"},
		"Ignored-Header": []string{"value"},
	}

	acct := NewAliyunAccount("", "")
	canonical := acct.buildCanonicalHeaders(headers)

	expected := "x-odps-another:valueA\n" +
		"x-odps-multi:a,b,c\n" +
		"x-odps-test:value1,value2\n"

	assert.Equal(t, expected, canonical)
}
