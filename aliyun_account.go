package odps

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
)

type AliyunAccount struct {
	endpoint  string
	accessId  string
	accessKey string
}

func NewAliyunAccount(accessId string, accessKey string, endpoint string) AliyunAccount {
	return AliyunAccount{
		endpoint:  endpoint,
		accessId:  accessId,
		accessKey: accessKey,
	}
}

func AliyunAccountFromEnv() AliyunAccount {
	account := AliyunAccount{}

	if endpoint, found := os.LookupEnv("odps_endpoint"); found {
		account.endpoint = endpoint
	}

	if accessId, found := os.LookupEnv("odps_accessId"); found {
		account.accessId = accessId
	}

	if accessKey, found := os.LookupEnv("odps_accessKey"); found {
		account.accessKey = accessKey
	}

	return account
}

func (account *AliyunAccount) AccessId() string {
	return account.accessId
}

func (account *AliyunAccount) AccessKey() string {
	return account.accessKey
}

func (account *AliyunAccount) Endpoint() string {
	return account.endpoint
}

func (account *AliyunAccount) GetType() Provider {
	return ALIYUN
}

func (account *AliyunAccount) SignRequest(req *http.Request) {
	var msg bytes.Buffer

	// write verb
	msg.WriteString(req.Method)
	msg.WriteByte('\n')

	// write common header
	msg.WriteString(req.Header.Get(HttpHeaderContentMD5))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(HttpHeaderContentType))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(HttpHeaderDate))
	msg.WriteByte('\n')

	// build canonical header
	var canonicalHeaderKeys []string

	for key := range req.Header {
		if strings.HasPrefix(strings.ToLower(key), HttpHeaderOdpsPrefix) {
			canonicalHeaderKeys = append(canonicalHeaderKeys, key)
		}
	}

	sort.Strings(canonicalHeaderKeys)

	for _, key := range canonicalHeaderKeys {
		msg.WriteString(strings.ToLower(key))
		msg.WriteByte(':')
		msg.WriteString(strings.Join(req.Header[key], ","))
		msg.WriteByte('\n')
	}

	// build canonical resource
	var canonicalResource bytes.Buffer
	endpoint, _ := url.Parse(account.endpoint)
	basePath := endpoint.Path
	if strings.HasPrefix(req.URL.Path, basePath) {
		canonicalResource.WriteString(req.URL.Path[len(endpoint.Path):])
	} else {
		canonicalResource.WriteString(req.URL.Path)
	}

	if urlParams := req.URL.Query(); len(urlParams) > 0 {
		canonicalResource.WriteByte('?')

		var paramKeys []string

		for k := range urlParams {
			paramKeys = append(paramKeys, k)
		}

		sort.Strings(paramKeys)

		for i, k := range paramKeys {
			if i > 0 {
				canonicalResource.WriteByte('&')
			}

			canonicalResource.WriteString(k)

			if v := urlParams.Get(k); v != "" {
				canonicalResource.WriteByte('=')
				canonicalResource.WriteString(v)
			}
		}
	}

	msg.Write(canonicalResource.Bytes())

	// signature = base64(HMacSha1(msg))
	hasher := hmac.New(sha1.New,  []byte(account.accessKey))
	hasher.Write(msg.Bytes())

	// Set header: "Authorization: ODPS" + AccessID + ":" + Signature
	var signature bytes.Buffer
	signature.WriteString("ODPS ")
	signature.WriteString(account.accessId)
	signature.WriteByte(':')
	signature.WriteString(base64.StdEncoding.EncodeToString(hasher.Sum(nil)))

	req.Header.Set(HttpHeaderAuthorization, signature.String())
}
