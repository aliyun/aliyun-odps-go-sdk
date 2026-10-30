package security_test

import (
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// liveServiceConfigured reports whether the package-level restClient above can
// actually talk to a project: it is built from ALIBABA_CLOUD_ACCESS_KEY_ID /
// ALIBABA_CLOUD_ACCESS_KEY_SECRET and odps_endpoint, and with those missing the
// account is nil, which the request signing dereferences and panics on.
//
// The examples here have no *testing.T to skip with, so they return before
// touching the service; their "// Output:" blocks expect no output, which still
// matches. With credentials configured they run exactly as before.
func liveServiceConfigured() bool {
	return account2.AccountFromEnv() != nil && restclient.LoadEndpointFromEnv() != ""
}
