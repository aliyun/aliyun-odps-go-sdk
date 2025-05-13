package account_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
)

func TestNewStsAccount(t *testing.T) {
	stsAccount := account.NewStsAccount("ak", "sk", "token")
	credential, err := stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)

	stsAccount = account.NewStsAccount("ak", "sk", "token", "regionId")
	credential, err = stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)
}

func TestNewStsAccountWithCredential(t *testing.T) {
	config := new(credentials.Config).
		// Which type of credential you want
		SetType("access_key").
		// AccessKeyId of your account
		SetAccessKeyId("AccessKeyId").
		// AccessKeySecret of your account
		SetAccessKeySecret("AccessKeySecret")

	provider, err := credentials.NewCredential(config)
	if err != nil {
		return
	}
	stsAccount := account.NewStsAccountWithCredential(provider)
	credential, err := stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)

	stsAccount = account.NewStsAccountWithCredential(provider, "regionId")
	credential, err = stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)
}

type CustomCredentialProvider struct{}

func (cp *CustomCredentialProvider) GetType() (*string, error) {
	s := "CustomProvider"
	return &s, nil
}

func (cp *CustomCredentialProvider) GetCredential() (*credentials.CredentialModel, error) {
	accessKeyId := ""
	accessKeySecurity := ""
	accessKeyToken := ""

	return &credentials.CredentialModel{
		AccessKeyId:     &accessKeyId,
		AccessKeySecret: &accessKeyToken,
		SecurityToken:   &accessKeySecurity,
	}, nil
}

func TestNewStsAccountWithProvider(t *testing.T) {
	provider := &CustomCredentialProvider{}
	stsAccount := account.NewStsAccountWithProvider(provider)
	credential, err := stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)

	stsAccount = account.NewStsAccountWithProvider(provider, "regionId")
	credential, err = stsAccount.Credential()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(*credential.AccessKeyId)
	t.Log(*credential.AccessKeySecret)
	t.Log(*credential.SecurityToken)
}
