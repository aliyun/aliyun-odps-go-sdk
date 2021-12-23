package account

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"net/http"
)

type BearerTokenAccount struct {
	endPoint string
	token    string
}

func NewBearerTokenAccount(endPoint, token string) BearerTokenAccount {
	return BearerTokenAccount{
		endPoint: endPoint,
		token:    token,
	}
}

func (account *BearerTokenAccount) GetType() Provider {
	return BearToken
}

func (account *BearerTokenAccount) Endpoint() string {
	return account.endPoint
}

func (account *BearerTokenAccount) SignRequest(req *http.Request, _ string) {
	req.Header.Set(common.HttpHeaderODPSBearerToken, account.token)
}
