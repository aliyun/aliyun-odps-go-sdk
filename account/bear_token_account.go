package account

import (
	"github.com/aliyun/aliyun-odps-go-sdk"
	"net/http"
)

type BearTokenAccount struct {
	endPoint string
	token    string
}

func NewBearTokenAccount(endPoint, token string) BearTokenAccount {
	return BearTokenAccount{
		endPoint: endPoint,
		token:    token,
	}
}

func (account *BearTokenAccount) GetType() AccountProvider {
	return AccountBearToken
}

func (account *BearTokenAccount) Endpoint() string {
	return account.endPoint
}

func (account *BearTokenAccount) SignRequest(req *http.Request, _ string) {
	req.Header.Set(odps.HttpHeaderODPSBearerToken, account.token)
}
