package account

import (
	"github.com/aliyun/aliyun-odps-go-sdk"
	"net/http"
)

type StsAccount struct {
	stsToken string
	AliyunAccount
}

func NewStsAccount(accessId, accessKey, stsToken string) *StsAccount {
	return &StsAccount{
		stsToken: stsToken,
		AliyunAccount: AliyunAccount{
			accessKey: accessKey,
			accessId:  accessId,
		},
	}
}

func (account *StsAccount) GetType() AccountProvider {
	return AccountSTS
}

func (account *StsAccount) SignRequest(req *http.Request, endpoint string) {
	account.AliyunAccount.SignRequest(req, endpoint)
	req.Header.Set(odps.HttpHeaderAuthorizationSTSToken, account.stsToken)
}
