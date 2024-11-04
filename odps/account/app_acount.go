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
	"fmt"
	"net/http"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type AppAccount struct {
	AliyunAccount
}

func NewAppAccount(accessId string, accessKey string) *AppAccount {
	return &AppAccount{
		AliyunAccount{
			accessId:  accessId,
			accessKey: accessKey,
		},
	}
}

func (account *AppAccount) SignRequest(req *http.Request, endpoint string) error {
	err := account.AliyunAccount.SignRequest(req, endpoint)

	if err != nil {
		return err
	}

	signature := req.Header.Get(common.HttpHeaderAuthorization)
	signature = base64HmacSha1([]byte(account.accessKey), []byte(signature))

	appAuth := fmt.Sprintf(
		"account_provider:%s,signature_method:hmac-sha1,access_id:%s,signature:%s",
		strings.ToLower(account.GetType().String()),
		account.accessId,
		signature,
	)

	req.Header.Set(common.HttpHeaderAppAuthentication, appAuth)

	return nil
}
