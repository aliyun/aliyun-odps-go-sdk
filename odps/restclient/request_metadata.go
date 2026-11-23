// Licensed under the Apache License, Version 2.0.

package restclient

import (
	"context"
	"net/http"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// RequestMetadata is optional call and trace information sent with signed requests.
type RequestMetadata struct {
	CallContext     string
	TraceParent     string
	TraceState      string
	UserAgentSuffix string
}

type metadataKey struct{}

// WithRequestMetadata carries request metadata to the REST signer.
func WithRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, metadata)
}

func setRequestMetadata(req *http.Request) {
	metadata, ok := req.Context().Value(metadataKey{}).(RequestMetadata)
	if !ok {
		return
	}
	if metadata.CallContext != "" {
		req.Header.Set("x-odps-call-context", metadata.CallContext)
	}
	if metadata.TraceParent != "" {
		req.Header.Set("traceparent", metadata.TraceParent)
	}
	if metadata.TraceState != "" {
		req.Header.Set("tracestate", metadata.TraceState)
	}
	if suffix := strings.TrimSpace(metadata.UserAgentSuffix); suffix != "" {
		agent := req.Header.Get(common.HttpHeaderXOdpsUserAgent)
		req.Header.Set(common.HttpHeaderXOdpsUserAgent, agent+" "+suffix)
	}
}
