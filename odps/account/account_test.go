package account

import (
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
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

	canonicalString := acct.buildCanonicalString(req, "/api")

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
		basePath string
		query    url.Values
		want     string
	}{
		{
			name:     "path normalization",
			reqPath:  "/api/v1/resource",
			basePath: "/api",
			want:     "/v1/resource",
		},
		{
			name:     "prefix belongs to another segment",
			reqPath:  "/api2/resources",
			basePath: "/api",
			want:     "/api2/resources",
		},
		{
			name:    "query parameters",
			reqPath: "/resource",
			query: url.Values{
				"action":  []string{"list"},
				"orderby": []string{"name"},
				"limit":   []string{"10"},
			},
			want: "/resource?action=list&limit=10&orderby=name",
		},
		{
			name:    "mixed parameters",
			reqPath: "/path",
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

			got := acct.buildCanonicalResource(req, tt.basePath)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateSignatureV2(t *testing.T) {
	acct := NewAliyunAccount("my-access-id", "my-secret-key")

	signature := acct.generateSignatureV2([]byte("testdata"))
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

func TestProviderNames(t *testing.T) {
	for _, tc := range []struct {
		provider Provider
		name     string
	}{
		{Taobao, "TAOBAO"},
		{Aliyun, "ALIYUN"},
		{STS, "STS"},
		{BearToken, "BEAR_TOKEN"},
		{0, "UnknownAccountProvider"},
	} {
		require.Equal(t, tc.name, tc.provider.String())
	}
}

func TestEnvironmentCredentialsRequireACompletePair(t *testing.T) {
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "fixture-secret")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "")
	require.Nil(t, AccountFromEnv())

	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "fixture-id")
	require.IsType(t, (*AliyunAccount)(nil), AccountFromEnv())
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
	require.Nil(t, AccountFromEnv())
}

func TestSignersValidateRequestsAndCredentials(t *testing.T) {
	const endpoint = "https://example.test/api"
	req, err := http.NewRequest(http.MethodGet, endpoint+"/projects/p", nil)
	require.NoError(t, err)
	for _, signer := range []Account{
		NewAliyunAccount("", "fixture-secret"),
		NewAliyunAccount("fixture-id", ""),
		NewStsAccount("", "fixture-secret", "fixture-token"),
		NewAppAccount("", "fixture-secret"),
		NewAppStsAccount("fixture-id", "", "fixture-token"),
	} {
		require.ErrorContains(t, signer.SignRequest(req, endpoint), "incomplete credential")
		require.Empty(t, req.Header.Get(common.HttpHeaderAuthorization))
	}

	signer := NewAliyunAccount("fixture-id", "fixture-secret")
	require.NotPanics(t, func() {
		require.Error(t, signer.SignRequest(req, "http://[::1"))
	})
	require.Error(t, signer.SignRequest(nil, endpoint))
	require.Error(t, signer.SignRequest(&http.Request{}, endpoint))
	withoutHeaders := &http.Request{Method: http.MethodGet, URL: req.URL}
	require.NoError(t, signer.SignRequest(withoutHeaders, endpoint))
	require.NotEmpty(t, withoutHeaders.Header.Get(common.HttpHeaderAuthorization))
}

func TestEndpointSlashDoesNotChangeSignature(t *testing.T) {
	const endpoint = "https://example.test/api"
	base, err := http.NewRequest(http.MethodGet, endpoint+"/projects/p?quotaName=batch", nil)
	require.NoError(t, err)
	withSlash := base.Clone(base.Context())
	signer := NewAliyunAccount("fixture-id", "fixture-secret")
	require.NoError(t, signer.SignRequest(base, endpoint))
	require.NoError(t, signer.SignRequest(withSlash, endpoint+"/"))
	require.Equal(t, base.Header.Get(common.HttpHeaderAuthorization), withSlash.Header.Get(common.HttpHeaderAuthorization))
}

func TestConcurrentCorporationUpdatesLeaveSignaturesConsistent(t *testing.T) {
	saved := corporation.Load()
	t.Cleanup(func() { corporation.Store(saved) })
	const endpoint = "https://example.test/api"
	req, err := http.NewRequest(http.MethodGet, endpoint+"/projects/p", nil)
	require.NoError(t, err)
	req.Header.Set(common.HttpHeaderDate, "Fri, 20 Nov 2026 00:00:00 GMT")
	signer := NewAliyunAccount("fixture-id", "fixture-secret", "cn-shanghai")
	expected := make(map[string]bool, 2)
	for _, corp := range []string{"aliyun", "apsara"} {
		SetCorporation(corp)
		signed := req.Clone(req.Context())
		require.NoError(t, signer.SignRequest(signed, endpoint))
		expected[signed.Header.Get(common.HttpHeaderAuthorization)] = true
	}
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 100 {
			SetCorporation("aliyun")
			SetCorporation("apsara")
		}
	})
	for range 4 {
		workers.Go(func() {
			for range 100 {
				signed := req.Clone(req.Context())
				if assert.NoError(t, signer.SignRequest(signed, endpoint)) {
					assert.True(t, expected[signed.Header.Get(common.HttpHeaderAuthorization)])
				}
			}
		})
	}
	workers.Wait()
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
