package tunnel

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"net/http"
)

func addCommonSessionHttpHeader(header http.Header) {
	header.Add(odps.HttpHeaderOdpsDateTransFrom, DateTransformVersion)
	header.Add(odps.HttpHeaderOdpsTunnelVersion, Version)
}
