// Licensed under the Apache License, Version 2.0.

package restclient

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

func TestRequestMetadataStaysRequestScoped(t *testing.T) {
	for _, tt := range []struct {
		name     string
		metadata *RequestMetadata
	}{
		{name: "absent"},
		{name: "empty", metadata: &RequestMetadata{}},
		{name: "full", metadata: &RequestMetadata{
			CallContext: "caller", TraceParent: "parent", TraceState: "tenant=team", UserAgentSuffix: " adapter ",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			if tt.metadata != nil {
				ctx = WithRequestMetadata(ctx, *tt.metadata)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.test", nil)
			require.NoError(t, err)
			req.Header.Set(common.HttpHeaderXOdpsUserAgent, "SDK")
			setRequestMetadata(req)
			if tt.name == "full" {
				require.Equal(t, "caller", req.Header.Get("x-odps-call-context"))
				require.Equal(t, "parent", req.Header.Get("traceparent"))
				require.Equal(t, "tenant=team", req.Header.Get("tracestate"))
				require.Equal(t, "SDK adapter", req.Header.Get(common.HttpHeaderXOdpsUserAgent))
			} else {
				require.Empty(t, req.Header.Get("x-odps-call-context"))
				require.Empty(t, req.Header.Get("traceparent"))
				require.Empty(t, req.Header.Get("tracestate"))
				require.Equal(t, "SDK", req.Header.Get(common.HttpHeaderXOdpsUserAgent))
			}
		})
	}
}
