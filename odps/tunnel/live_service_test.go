package tunnel_test

import (
	"testing"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// liveServiceConfigured reports whether an account and an odps_endpoint are
// configured, which is what the package-level odpsIns and tunnelIns above need
// to create tables and sessions on a real project. Checking odpsIns == nil is
// not enough: odps.NewOdps returns a usable struct for a nil account, and the
// first signed request then panics - aborting the test binary and with it the
// offline protoc, slot-routing and retry-contract tests in this package.
//
// Examples have no *testing.T to skip with, so they return before touching the
// service; their "// Output:" blocks expect no output, which still matches.
func liveServiceConfigured() bool {
	return account2.AccountFromEnv() != nil && restclient.LoadEndpointFromEnv() != ""
}

func skipWithoutLiveService(t *testing.T) {
	t.Helper()
	if !liveServiceConfigured() {
		t.Skip("needs a live MaxCompute project: set ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET and odps_endpoint")
	}
}
