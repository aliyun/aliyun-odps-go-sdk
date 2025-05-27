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
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type StsAccount struct {
	stsToken string
	ApsaraAccount
}

func (sp *StsAccount) _signRequest(req *http.Request, endpoint string) error {
	err := sp.ApsaraAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, sp.stsToken)
	return nil
}

func NewStsAccount(accessId, accessKey, securityToken string, regionId ...string) *StsAccount {
	var aliyunAccount *ApsaraAccount
	if len(regionId) > 0 {
		aliyunAccount = NewApsaraAccount(accessId, accessKey, regionId[0])
	} else {
		aliyunAccount = NewApsaraAccount(accessId, accessKey)
	}

	sp := &StsAccount{
		stsToken:      securityToken,
		ApsaraAccount: *aliyunAccount,
	}

	return sp
}

func (account *StsAccount) GetType() Provider {
	return STS
}

func (account *StsAccount) SignRequest(req *http.Request, endpoint string) error {
	return account._signRequest(req, endpoint)
}

func (account *StsAccount) StsToken() string {
	return account.stsToken
}
