package sqldriver_test

import (
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// liveServiceConfigured reports whether the examples below can reach a project:
// they build a DSN from ALIBABA_CLOUD_ACCESS_KEY_ID /
// ALIBABA_CLOUD_ACCESS_KEY_SECRET and odps_endpoint, and AccountFromEnv returns
// nil when the account is not configured, which the examples then dereference
// with account.GetType() and crash on. A crash takes the offline driver tests in
// this package with it, so the live examples return early instead - their
// "// Output:" blocks expect no output, which still matches - and the live
// tests Skip. The *_real_service_test.go files already gate on the same
// environment; this is the same rule in one place.
func liveServiceConfigured() bool {
	return account2.AccountFromEnv() != nil && restclient.LoadEndpointFromEnv() != ""
}
