package consts

import "time"

const UserAgentValue = "odps-go-sdk/0.0.1 Go/1.17.2"
const HttpHeaderOdpsPrefix = "x-odps-"

var HttpMethod = struct {
	GetMethod    string
	PutMethod    string
	PostMethod   string
	DeleteMethod string
}{
	GetMethod:    "GET",
	PutMethod:    "PUT",
	PostMethod:   "POST",
	DeleteMethod: "DELETE",
}

var GMT, _ = time.LoadLocation("GMT")

const (
	HttpHeaderDate                  = "Date"
	HttpHeaderContentType           = "Content-Type"
	HttpHeaderContentMD5            = "Content-MD5"
	HttpHeaderContentLength         = "Content-Length"
	HttpHeaderLastModified          = "Last-Modified"
	HttpHeaderXOdpsUserAgent        = "x-odps-user-agent"
	HttpHeaderOdpsOwner             = "x-odps-owner"
	HttpHeaderOdpsCreationTime      = "x-odps-creation-time"
	HttpHeaderOdpsRequestId         = "x-odps-request-id"
	HttpHeaderLocation              = "Location"
	HttpHeaderOdpsStartTime         = "x-odps-start-time"
	HttpHeaderOdpsEndTime           = "x-odps-end-time"
	HttpHeaderSqlTimezone           = "x-odps-sql-timezone"
	HttpHeaderOdpsSupervisionToken  = "odps-x-supervision-token"
	HttpHeaderAuthorization         = "Authorization"
	HttpHeaderAuthorizationSTSToken = "authorization-sts-token"
	HttpHeaderAppAuthentication     = "application-authentication"
	HttpHeaderSTSAuthentication     = "sts-authentication"
	HttpHeaderSTSToken              = "sts-token"
	HttpHeaderODPSBearerToken       = "x-odps-bearer-token"
	HttpHeaderOdpsDateTransFrom     = "odps-tunnel-date-transform"
	HttpHeaderOdpsTunnelVersion     = "x-odps-tunnel-version"

	XMLContentType = "application/xml"
)
