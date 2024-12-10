// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import "time"

const (
	UserAgentValue       = "odps-go-sdk/0.0.1 Go/1.17.2"
	HttpHeaderOdpsPrefix = "x-odps-"
)

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
	HttpHeaderDate                          = "Date"
	HttpHeaderContentType                   = "Content-Type"
	HttpHeaderContentMD5                    = "Content-MD5"
	HttpHeaderContentDisposition            = "Content-Disposition"
	HttpHeaderContentLength                 = "Content-Length"
	HttpHeaderContentEncoding               = "Content-Encoding"
	HttpHeaderLastModified                  = "Last-Modified"
	HttpHeaderUserAgent                     = "User-Agent"
	HttpHeaderXOdpsUserAgent                = "x-odps-user-agent"
	HttpHeaderOdpsOwner                     = "x-odps-owner"
	HttpHeaderOdpsCreationTime              = "x-odps-creation-time"
	HttpHeaderOdpsRequestId                 = "x-odps-request-id"
	HttpHeaderLocation                      = "Location"
	HttpHeaderOdpsStartTime                 = "x-odps-start-time"
	HttpHeaderOdpsEndTime                   = "x-odps-end-time"
	HttpHeaderSqlTimezone                   = "x-odps-sql-timezone"
	HttpHeaderOdpsSupervisionToken          = "odps-x-supervision-token"
	HttpHeaderAuthorization                 = "Authorization"
	HttpHeaderAuthorizationSTSToken         = "authorization-sts-token"
	HttpHeaderAppAuthentication             = "application-authentication"
	HttpHeaderSTSAuthentication             = "sts-authentication"
	HttpHeaderSTSToken                      = "sts-token"
	HttpHeaderODPSBearerToken               = "x-odps-bearer-token"
	HttpHeaderOdpsDateTransFrom             = "odps-tunnel-date-transform"
	HttpHeaderOdpsTunnelVersion             = "x-odps-tunnel-version"
	HttpHeaderOdpsSlotNum                   = "odps-tunnel-slot-num"
	HttpHeaderRoutedServer                  = "odps-tunnel-routed-server"
	HttpHeaderTransferEncoding              = "Transfer-Encoding"
	HttpHeaderAcceptEncoding                = "Accept-Encoding"
	HttpHeaderOdpsSdkSupportSchemaEvolution = "odps-tunnel-sdk-support-schema-evolution"
	HttpHeaderOdpsTunnelLatestSchemaVersion = "odps-tunnel-latest-schema-version"
	HttpHeaderOdpsSchemaName                = "schema-name"
	HttpHeaderOdpsResourceName              = "x-odps-resource-name"
	HttpHeaderOdpsResourceType              = "x-odps-resource-type"
	HttpHeaderOdpsResourceSize              = "x-odps-resource-size"
	HttpHeaderOdpsComment                   = "x-odps-comment"
	HttpHeaderOdpsUpdator                   = "x-odps-updator"
	HttpHeaderOdpsCopyTableSource           = "x-odps-copy-table-source"
	HttpHeaderOdpsCopyFileSource            = "x-odps-copy-file-source"
	HttpHeaderOdpsResourceIsTemp            = "x-odps-resource-istemp"
	HttpHeaderOdpsResourceMergeTotalBytes   = "x-odps-resource-merge-total-bytes"

	XMLContentType = "application/xml"
)
