package odps

import "net/http"

type Provider uint

const (
	_ Provider = iota
	// TAOBAO 淘宝账号
	TAOBAO
	// ALIYUN 阿里云账号
	ALIYUN
	STS
	// BearToken logview token
	BearToken
)

type Account interface {
	GetType() Provider
	SignRequest(req *http.Request)
	Endpoint() string
}

// TODO 添加其他类型的账号