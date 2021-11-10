package odps

import "net/http"

type StsAccount struct {
	stsToken string
	AliyunAccount
}

func NewStsAccount(accessId, accessKey, endpoint, stsToken string ) StsAccount {
	return StsAccount {
		stsToken: stsToken,
		AliyunAccount: AliyunAccount{
			endpoint: endpoint,
			accessKey: accessKey,
			accessId: accessId,
		},
	}
}

func (account *StsAccount) SignRequest(req *http.Request) {
	account.AliyunAccount.SignRequest(req)
	req.Header.Set(HttpHeaderAuthorizationSTSToken, account.stsToken)
}