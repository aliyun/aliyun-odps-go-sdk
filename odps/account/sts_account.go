package account

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
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

func (account *StsAccount) GetType() Provider {
	return STS
}

func (account *StsAccount) SignRequest(req *http.Request, endpoint string) {
	account.AliyunAccount.SignRequest(req, endpoint)
	req.Header.Set(common.HttpHeaderAuthorizationSTSToken, account.stsToken)
}
