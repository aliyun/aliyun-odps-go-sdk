package account

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"strings"
)

type Provider uint

const (
	_ Provider = iota
	Taobao
	Aliyun
	STS
	// BearToken logview token
	BearToken
)

func (p Provider) String() string {
	switch p {
	case Taobao:
		return "TAOBAO"
	case Aliyun:
		return "ALIYUN"
	case STS:
		return "STS"
	case BearToken:
		return "BEAR_TOKEN"
	default:
		return "UnknownAccountProvider"
	}
}

type Account interface {
	GetType() Provider
	SignRequest(req *http.Request, endpoint string)
}

func base64HmacSha1(key []byte, data []byte) string {
	hasher := hmac.New(sha1.New, key)
	hasher.Write(data)
	sig := base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	return strings.TrimSpace(sig)
}

// TODO 添加其他类型的账号
