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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/credentials-go/credentials"
	"net/http"
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
	AccessId() string
	AccessKey() string
	SecurityToken() string
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

func (sp *stsStringProvider) AccessId() string {
	return sp.stsToken
}
func (sp *stsStringProvider) AccessKey() string {
	return sp.stsToken
}
func (sp *stsStringProvider) SecurityToken() string {
	return sp.stsToken
}

type stsAliyunCredentialProvider struct {
	aliyunCredential credentials.Credential
}

func (sp *stsAliyunCredentialProvider) _signRequest(req *http.Request, endpoint string) error {
	cred, err := sp.aliyunCredential.GetCredential()
	if err != nil {
		return err
	}

	aliyunAccount := NewAliyunAccount(*cred.AccessKeyId, *cred.AccessKeySecret)
	err = aliyunAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, *cred.BearerToken)

	return nil
}

func (sp *stsAliyunCredentialProvider) AccessId() string {
	credential, _ := sp.aliyunCredential.GetCredential()
	return *credential.AccessKeyId
}
func (sp *stsAliyunCredentialProvider) AccessKey() string {
	credential, _ := sp.aliyunCredential.GetCredential()
	return *credential.AccessKeySecret
}
func (sp *stsAliyunCredentialProvider) SecurityToken() string {
	credential, _ := sp.aliyunCredential.GetCredential()
	return *credential.SecurityToken
}

type stsCustomCredentialProvider struct {
	provider CredentialProvider
}

func (sp *stsCustomCredentialProvider) _signRequest(req *http.Request, endpoint string) error {
	cred, err := sp.provider.GetCredential()
	if err != nil {
		return err
	}

	aliyunAccount := NewAliyunAccount(*cred.AccessKeyId, *cred.AccessKeySecret)
	err = aliyunAccount.SignRequest(req, endpoint)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, *cred.BearerToken)

	return nil
}

func (sp *stsCustomCredentialProvider) AccessId() string {
	credential, _ := sp.provider.GetCredential()
	return *credential.AccessKeyId
}
func (sp *stsCustomCredentialProvider) AccessKey() string {
	credential, _ := sp.provider.GetCredential()
	return *credential.AccessKeySecret
}
func (sp *stsCustomCredentialProvider) SecurityToken() string {
	credential, _ := sp.provider.GetCredential()
	return *credential.SecurityToken
}

func NewStsAccount(accessId, accessKey, securityToken string) *StsAccount {
	sp := &stsStringProvider{
		stsToken: securityToken,
		AliyunAccount: AliyunAccount{
			accessKey: accessKey,
			accessId:  accessId,
		},
	}

	return &StsAccount{
		sp: sp,
	}
}

func NewStsAccountWithCredential(aliyunCredential credentials.Credential) *StsAccount {
	sp := &stsAliyunCredentialProvider{
		aliyunCredential: aliyunCredential,
	}

	return &StsAccount{
		sp: sp,
	}
}

func NewStsAccountWithProvider(provider CredentialProvider) *StsAccount {
	sp := &stsCustomCredentialProvider{
		provider: provider,
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

func (account *StsAccount) AccessId() string {
	return account.sp.AccessId()
}

func (account *StsAccount) AccessKey() string {
	return account.sp.AccessKey()
}

func (account *StsAccount) SecurityToken() string {
	return account.sp.SecurityToken()
}
