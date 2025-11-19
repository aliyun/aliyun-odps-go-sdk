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

	"github.com/aliyun/credentials-go/credentials"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type StsAccount struct {
	sp stsAccountProvider
}

type CredentialProvider interface {
	GetType() (*string, error)
	GetCredential() (*credentials.CredentialModel, error)
}

type stsAccountProvider interface {
	_signRequest(req *http.Request, endpoint string) error
	Credential() (*credentials.CredentialModel, error)
}

type stsStringProvider struct {
	stsToken string
	AliyunAccount
}

func (sp *stsStringProvider) _signRequest(req *http.Request, endpoint string) error {
	err := sp.AliyunAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, sp.stsToken)
	return nil
}

func (sp *stsStringProvider) Credential() (*credentials.CredentialModel, error) {
	return &credentials.CredentialModel{
		AccessKeyId:     &sp.accessId,
		AccessKeySecret: &sp.accessKey,
		SecurityToken:   &sp.stsToken,
	}, nil
}

type stsAliyunCredentialProvider struct {
	aliyunCredential credentials.Credential
	regionId         string
}

func (sp *stsAliyunCredentialProvider) _signRequest(req *http.Request, endpoint string) error {
	cred, err := sp.aliyunCredential.GetCredential()
	if err != nil {
		return err
	}

	aliyunAccount := NewAliyunAccount(*cred.AccessKeyId, *cred.AccessKeySecret, sp.regionId)
	err = aliyunAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}
	if cred.SecurityToken != nil {
		req.Header.Set(common.HttpHeaderAuthorizationSTSToken, *cred.SecurityToken)
	}
	return nil
}

func (sp *stsAliyunCredentialProvider) Credential() (*credentials.CredentialModel, error) {
	return sp.aliyunCredential.GetCredential()
}

type stsCustomCredentialProvider struct {
	provider CredentialProvider
	regionId string
}

func (sp *stsCustomCredentialProvider) _signRequest(req *http.Request, endpoint string) error {
	cred, err := sp.provider.GetCredential()
	if err != nil {
		return err
	}

	aliyunAccount := NewAliyunAccount(*cred.AccessKeyId, *cred.AccessKeySecret, sp.regionId)
	err = aliyunAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, *cred.BearerToken)

	return nil
}

func (sp *stsCustomCredentialProvider) Credential() (*credentials.CredentialModel, error) {
	return sp.provider.GetCredential()
}

func NewStsAccount(accessId, accessKey, securityToken string, regionId ...string) *StsAccount {
	var aliyunAccount *AliyunAccount
	if len(regionId) > 0 {
		aliyunAccount = NewAliyunAccount(accessId, accessKey, regionId[0])
	} else {
		aliyunAccount = NewAliyunAccount(accessId, accessKey)
	}

	sp := &stsStringProvider{
		stsToken:      securityToken,
		AliyunAccount: *aliyunAccount,
	}

	return &StsAccount{
		sp: sp,
	}
}

func NewStsAccountWithCredential(aliyunCredential credentials.Credential, regionId ...string) *StsAccount {
	var sp *stsAliyunCredentialProvider
	if len(regionId) > 0 {
		sp = &stsAliyunCredentialProvider{
			aliyunCredential: aliyunCredential,
			regionId:         regionId[0],
		}
	} else {
		sp = &stsAliyunCredentialProvider{
			aliyunCredential: aliyunCredential,
		}
	}
	return &StsAccount{
		sp: sp,
	}
}

func NewStsAccountWithProvider(provider CredentialProvider, regionId ...string) *StsAccount {
	var sp *stsCustomCredentialProvider
	if len(regionId) > 0 {
		sp = &stsCustomCredentialProvider{
			provider: provider,
			regionId: regionId[0],
		}
	} else {
		sp = &stsCustomCredentialProvider{
			provider: provider,
		}
	}
	return &StsAccount{
		sp: sp,
	}
}

func (account *StsAccount) GetType() Provider {
	return STS
}

func (account *StsAccount) SignRequest(req *http.Request, endpoint string) error {
	return account.sp._signRequest(req, endpoint)
}

func (account *StsAccount) Credential() (*credentials.CredentialModel, error) {
	return account.sp.Credential()
}
