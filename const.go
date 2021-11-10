package odps

import "time"

const UserAgentValue = "odps-go-sdk/0.0.1 Go/1.17.2"
const HttpHeaderOdpsPrefix = "x-odps-"

const (
	GetMethod    = "GET"
	PutMethod    = "PUT"
	PostMethod   = "POST"
	DeleteMethod = "DELETE"
)


var GMT, _ = time.LoadLocation("GMT")

const (
	HttpHeaderDate             = "Date"
	HttpHeaderContentType      = "Content-Type"
	HttpHeaderContentMD5       = "Content-MD5"
	HttpHeaderContentLength    = "Content-Length"
	HttpHeaderLastModified     = "Last-Modified"
	HttpHeaderXOdpsUserAgent   = "x-odps-user-agent"
	HttpHeaderOdpsOwner        = "x-odps-owner"
	HttpHeaderOdpsCreationTime = "x-odps-creation-time"
	HttpHeaderOdpsRequestId    = "x-odps-request-id"
	HttpHeaderLocation         = "Location"
	HttpHeaderOdpsStartTime    = "x-odps-start-time"
	HttpHeaderOdpsEndTime      = "x-odps-end-time"

	HttpHeaderAuthorization = "Authorization"
	HttpHeaderAuthorizationSTSToken = "authorization-sts-token"
	HttpHeaderAppAuthentication = "application-authentication"
	HttpHeaderSTSAuthentication = "sts-authentication"
	HttpHeaderSTSToken = "sts-token"
	HttpHeaderODPSBearerToken = "x-odps-bearer-token"


	XMLContentType = "application/xml"
)

