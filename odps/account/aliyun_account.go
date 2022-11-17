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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
)

type AliyunAccount struct {
	accessId  string
	accessKey string
}

func NewAliyunAccount(accessId string, accessKey string) *AliyunAccount {
	return &AliyunAccount{
		accessId:  accessId,
		accessKey: accessKey,
	}
}

func AliyunAccountFromEnv() *AliyunAccount {
	account := AliyunAccount{}

	if accessId, found := os.LookupEnv("odps_accessId"); found {
		account.accessId = accessId
	}

	if accessKey, found := os.LookupEnv("odps_accessKey"); found {
		account.accessKey = accessKey
	}

	return &account
}

func (account *AliyunAccount) AccessId() string {
	return account.accessId
}

func (account *AliyunAccount) AccessKey() string {
	return account.accessKey
}

func (account *AliyunAccount) GetType() Provider {
	return Aliyun
}

func (account *AliyunAccount) SignRequest(req *http.Request, endpoint string) {
	var msg bytes.Buffer

	// write verb
	msg.WriteString(req.Method)
	msg.WriteByte('\n')

	// write common header
	msg.WriteString(req.Header.Get(common.HttpHeaderContentMD5))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(common.HttpHeaderContentType))
	msg.WriteByte('\n')
	msg.WriteString(req.Header.Get(common.HttpHeaderDate))
	msg.WriteByte('\n')

	// build canonical header
	var canonicalHeaderKeys []string

	for key := range req.Header {
		if strings.HasPrefix(strings.ToLower(key), common.HttpHeaderOdpsPrefix) {
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
	endpointSeg, _ := url.Parse(endpoint)
	basePath := endpointSeg.Path
	if strings.HasPrefix(req.URL.Path, basePath) {
		canonicalResource.WriteString(req.URL.Path[len(endpointSeg.Path):])
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
	_signature := base64HmacSha1([]byte(account.accessKey), msg.Bytes())

	// Set header: "Authorization: ODPS" + AccessID + ":" + Signature
	var signature bytes.Buffer
	signature.WriteString("ODPS ")
	signature.WriteString(account.accessId)
	signature.WriteByte(':')
	signature.WriteString(_signature)

	req.Header.Set(common.HttpHeaderAuthorization, signature.String())
}
