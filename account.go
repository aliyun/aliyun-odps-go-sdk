package odps

import "net/http"

type Provider uint

const (
	_ = iota
	TAOBAO
	ALIYUN
	STS
	BEARER_TOKEN
)

type Account interface {
	GetType() Provider
	SignRequest(req *http.Request)
	Endpoint() string
}
