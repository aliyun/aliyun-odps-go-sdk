package odps

import "time"

const UserAgentValue = "odps-go-sdk/0.0.1 Go/1.17.2"
const HttpHeaderOdpsPrefix = "x-odps-"

var GMT, _ = time.LoadLocation("GMT")

const (
	HttpHeaderDate           = "Date"
	HttpHeaderContentType    = "Content-Type"
	HttpHeaderContentMD5     = "Content-MD5"
	HttpHeaderContentLength  = "Content-Length"
	HttpHeaderXOdpsUserAgent = "x-odps-user-agent"
	HttpHeaderAuthorization  = "Authorization"
)
