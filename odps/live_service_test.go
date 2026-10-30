package odps_test

import (
	"testing"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// liveServiceConfigured reports whether the environment has an account
// (ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET, plus
// ALIBABA_CLOUD_SECURITY_TOKEN for STS) and an odps_endpoint. The examples in
// this package are driven against a real MaxCompute project and need both.
//
// Without credentials the request signing dereferences a nil account and
// panics, and a panic aborts the test binary, which would take the offline
// contract tests in this package down with it. So the live-service cases state
// that they need a service and stop instead of running:
//
//   - a test calls skipWithoutLiveService(t) and shows up as SKIP;
//   - an example has no *testing.T to skip with, so it returns before touching
//     the service. Its "// Output:" block expects no output, which still matches.
//
// With credentials configured, every one of these runs exactly as before.
func liveServiceConfigured() bool {
	return account2.AccountFromEnv() != nil && restclient.LoadEndpointFromEnv() != ""
}

func skipWithoutLiveService(t *testing.T) {
	t.Helper()
	if !liveServiceConfigured() {
		t.Skip("needs a live MaxCompute project: set ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET and odps_endpoint")
	}
}
