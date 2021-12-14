package account

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"net/http"
	"strings"
)

type AppStsAccount struct {
	stsToken string
	AliyunAccount
}

func NewAppStsAccount(accessId, accessKey, stsToken string) *AppStsAccount {
	return &AppStsAccount{
		stsToken: stsToken,
		AliyunAccount: AliyunAccount{
			accessKey: accessKey,
			accessId:  accessId,
		},
	}
}

func (account *AppStsAccount) SignRequest(req *http.Request, endpoint string) {
	account.AliyunAccount.SignRequest(req, endpoint)
	signature := req.Header.Get(common.HttpHeaderAuthorization)
	signature = base64HmacSha1([]byte(account.accessKey), []byte(signature))

	stsAuth := fmt.Sprintf(
		"account_provider:%s,signature_method:hmac-sha1,access_id:%s,signature:%s",
		strings.ToLower(account.GetType().String()),
		account.accessId,
		signature,
	)

	req.Header.Set(common.HttpHeaderSTSAuthentication, stsAuth)
	req.Header.Set(common.HttpHeaderSTSToken, account.stsToken)
}