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
	"context"
	"errors"
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
	err := sp.SignRequest(req, endpoint)
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

// errIncompleteCredential is returned when a credential provider hands the
// account an unusable credential. It deliberately carries no credential value,
// so an error message can be logged or shown without leaking a secret.
var errIncompleteCredential = errors.New("odps account: incomplete credential (access key id and secret are required)")

// signWithCredential signs req with the access key pair from cred.
//
// A provider is called per request, so a rotated temporary credential is picked
// up by the next signing. An empty/nil pair or a nil credential is reported as
// an error instead of being dereferenced (the previous code dereferenced
// cred.AccessKeyId / cred.AccessKeySecret unguarded, so one bad provider reply
// killed the process).
func signWithCredential(cred *credentials.CredentialModel, regionId, endpoint string, req *http.Request) error {
	if cred == nil {
		return errIncompleteCredential
	}
	if cred.AccessKeyId == nil || *cred.AccessKeyId == "" ||
		cred.AccessKeySecret == nil || *cred.AccessKeySecret == "" {
		return errIncompleteCredential
	}

	aliyunAccount := NewAliyunAccount(*cred.AccessKeyId, *cred.AccessKeySecret, regionId)
	if err := aliyunAccount.SignRequest(req, endpoint); err != nil {
		return err
	}

	if token, ok := stsTokenHeaderValue(cred); ok {
		req.Header.Set(common.HttpHeaderAuthorizationSTSToken, token)
	} else {
		req.Header.Del(common.HttpHeaderAuthorizationSTSToken)
	}

	return nil
}

// stsTokenHeaderValue picks the value for the authorization-sts-token header.
//
// SecurityToken is what a temporary-credential provider fills, so it wins.
// BearerToken is only a fallback, kept so a provider that has always put its
// token there keeps working; a credential with neither sends no token header.
func stsTokenHeaderValue(cred *credentials.CredentialModel) (string, bool) {
	if cred.SecurityToken != nil {
		return *cred.SecurityToken, true
	}
	if cred.BearerToken != nil {
		return *cred.BearerToken, true
	}

	return "", false
}

type stsAliyunCredentialProvider struct {
	refresh          chan struct{}
	aliyunCredential credentials.Credential
	regionId         string
}

func (sp *stsAliyunCredentialProvider) _signRequest(req *http.Request, endpoint string) error {
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	cred, err := sp.credential(ctx)
	if err != nil {
		return err
	}

	return signWithCredential(cred, sp.regionId, endpoint, req)
}

func (sp *stsAliyunCredentialProvider) Credential() (*credentials.CredentialModel, error) {
	return sp.credential(context.Background())
}

func (sp *stsAliyunCredentialProvider) credential(ctx context.Context) (*credentials.CredentialModel, error) {
	// The credentials-go providers may mutate their cache during refresh.
	select {
	case sp.refresh <- struct{}{}:
		defer func() { <-sp.refresh }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	credential, err := sp.aliyunCredential.GetCredential()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return credential, err
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

	return signWithCredential(cred, sp.regionId, endpoint, req)
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
	sp := &stsAliyunCredentialProvider{
		refresh:          make(chan struct{}, 1),
		aliyunCredential: aliyunCredential,
	}
	if len(regionId) > 0 {
		sp.regionId = regionId[0]
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
