package account

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"strings"
)

type AccountProvider uint

const (
	_ AccountProvider = iota
	// AccountTaobao 淘宝账号
	AccountTaobao
	// AccountAliyun 阿里云账号
	AccountAliyun
	AccountSTS
	// AccountBearToken logview token
	AccountBearToken
)

func (p AccountProvider) String() string {
	switch p {
	case AccountTaobao:
		return "TAOBAO"
	case AccountAliyun:
		return "ALIYUN"
	case AccountSTS:
		return "STS"
	case AccountBearToken:
		return "BEAR_TOKEN"
	default:
		return "UnknownAccountProvider"
	}
}

type Account interface {
	GetType() AccountProvider
	SignRequest(req *http.Request, endpoint string)
}

func base64HmacSha1(key []byte, data []byte) string {
	hasher := hmac.New(sha1.New, key)
	hasher.Write(data)
	sig := base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	return strings.TrimSpace(sig)
}

// TODO 添加其他类型的账号
