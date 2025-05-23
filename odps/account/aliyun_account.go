// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package account

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

var corporation = "aliyun"

func SetCorporation(corp string) {
	corporation = corp
}

type AliyunAccount struct {
	accessId  string
	accessKey string
	regionId  string
}

func NewAliyunAccount(accessId string, accessKey string, regionId ...string) *AliyunAccount {
	if len(regionId) > 0 {
		return &AliyunAccount{
			accessId:  accessId,
			accessKey: accessKey,
			regionId:  regionId[0],
		}
	} else {
		return &AliyunAccount{
			accessId:  accessId,
			accessKey: accessKey,
		}
	}
}

func AliyunAccountFromEnv() *AliyunAccount {
	account := AliyunAccount{}

	if accessId, found := os.LookupEnv("ALIBABA_CLOUD_ACCESS_KEY_ID"); found {
		account.accessId = accessId
	}

	if accessKey, found := os.LookupEnv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"); found {
		account.accessKey = accessKey
	}

	return &account
}

func AccountFromEnv() Account {
	accessId, found := os.LookupEnv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey, found := os.LookupEnv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	if !found {
		return nil
	}
	securityToken, found := os.LookupEnv("ALIBABA_CLOUD_SECURITY_TOKEN")
	if found {
		return NewStsAccount(accessId, accessKey, securityToken)
	} else {
		return NewAliyunAccount(accessId, accessKey)
	}
}

func (account *AliyunAccount) AccessId() string {
	return account.accessId
}

func (account *AliyunAccount) AccessKey() string {
	return account.accessKey
}

func (account *AliyunAccount) RegionId() string {
	return account.regionId
}

func (account *AliyunAccount) GetType() Provider {
	return Aliyun
}

func (account *AliyunAccount) SignRequest(req *http.Request, endpoint string) error {
	canonicalString := account.buildCanonicalString(req, endpoint)
	// Generate signature
	var signature string
	if account.regionId == "" {
		signature = account.generateSignatureV2(canonicalString.Bytes())
	} else {
		signature = account.generateSignatureV4(canonicalString.Bytes(), account.regionId)
	}
	// Set authorization header
	req.Header.Set(common.HttpHeaderAuthorization, signature)
	return nil
}

// buildCanonicalString constructs canonical string for ODPS signature
func (account *AliyunAccount) buildCanonicalString(req *http.Request, endpoint string) bytes.Buffer {
	var msg bytes.Buffer

	// Write HTTP method
	msg.WriteString(req.Method)
	msg.WriteByte('\n')

	// Write standard headers
	msg.WriteString(req.Header.Get(common.HttpHeaderContentMD5))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(common.HttpHeaderContentType))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(common.HttpHeaderDate))
	msg.WriteByte('\n')

	// Write canonical headers
	msg.WriteString(account.buildCanonicalHeaders(req.Header))

	// Write canonical resource
	msg.WriteString(account.buildCanonicalResource(req, endpoint))
	return msg
}

// buildCanonicalHeaders constructs canonical headers for ODPS signature
func (account *AliyunAccount) buildCanonicalHeaders(headers http.Header) string {
	var headerBuf bytes.Buffer
	var canonicalHeaderKeys []string

	// Collect ODPS-specific headers
	for key := range headers {
		lowerKey := strings.ToLower(key)
		if strings.HasPrefix(lowerKey, common.HttpHeaderOdpsPrefix) {
			canonicalHeaderKeys = append(canonicalHeaderKeys, lowerKey)
		}
	}
	// Sort and write headers
	sort.Strings(canonicalHeaderKeys)
	for _, key := range canonicalHeaderKeys {
		headerBuf.WriteString(key)
		headerBuf.WriteByte(':')
		headerBuf.WriteString(strings.Join(headers.Values(key), ","))
		headerBuf.WriteByte('\n')
	}
	return headerBuf.String()
}

// buildCanonicalResource constructs canonical resource path for ODPS signature
func (account *AliyunAccount) buildCanonicalResource(req *http.Request, endpoint string) string {
	var resBuf bytes.Buffer
	parsedEndpoint, _ := url.Parse(endpoint)
	basePath := parsedEndpoint.Path

	// Handle path normalization
	if strings.HasPrefix(req.URL.Path, basePath) {
		resBuf.WriteString(req.URL.Path[len(basePath):])
	} else {
		resBuf.WriteString(req.URL.Path)
	}

	// Handle query parameters
	queryParams := req.URL.Query()
	if len(queryParams) > 0 {
		resBuf.WriteByte('?')
		paramKeys := make([]string, 0, len(queryParams))
		for k := range queryParams {
			paramKeys = append(paramKeys, k)
		}
		sort.Strings(paramKeys)

		for i, key := range paramKeys {
			if i > 0 {
				resBuf.WriteByte('&')
			}
			resBuf.WriteString(key)
			if value := queryParams.Get(key); value != "" {
				resBuf.WriteByte('=')
				resBuf.WriteString(value)
			}
		}
	}

	return resBuf.String()
}

// generateSignature creates the final authorization signature V2
func (account *AliyunAccount) generateSignatureV2(data []byte) string {
	signature := base64HmacSha1([]byte(account.accessKey), data)
	var authBuf bytes.Buffer
	authBuf.WriteString("ODPS ")
	authBuf.WriteString(account.accessId)
	authBuf.WriteByte(':')
	authBuf.WriteString(signature)
	return authBuf.String()
}

// hmacsha256 executes HMAC-SHA256 signatures
func hmacsha256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// generateSignature creates the final authorization signature V4
func (account *AliyunAccount) generateSignatureV4(data []byte, regionName string) string {
	currentDate := time.Now().UTC().Format("20060102")
	credential := fmt.Sprintf("%s/%s/%s/odps/%s_v4_request", account.accessId, currentDate, regionName, corporation)

	kSecret := []byte(corporation + "_v4" + account.accessKey)
	kDate := hmacsha256(kSecret, []byte(currentDate))
	kRegion := hmacsha256(kDate, []byte(regionName))
	kService := hmacsha256(kRegion, []byte("odps"))
	signatureKey := hmacsha256(kService, []byte(corporation+"_v4_request"))
	signature := base64HmacSha1(signatureKey, data)
	return "ODPS " + credential + ":" + signature
}
