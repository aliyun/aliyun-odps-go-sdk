package odps

import (
	"fmt"
	"net/http"
	"strings"
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

func (account *AppAccount) SignRequest(req *http.Request, endpoint string) {
	account.AliyunAccount.SignRequest(req, endpoint)
	signature := req.Header.Get(HttpHeaderAuthorization)
	signature = base64HmacSha1([]byte(account.accessKey), []byte(signature))

	appAuth := fmt.Sprintf(
		"account_provider:%s,signature_method:hmac-sha1,access_id:%s,signature:%s",
		strings.ToLower(account.GetType().String()),
		account.accessId,
		signature,
	)

	req.Header.Set(HttpHeaderAppAuthentication, appAuth)
}
