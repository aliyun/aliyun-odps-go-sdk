package account_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// liveServiceConfigured reports whether an account and an odps_endpoint are
// configured; TestSignatureV4 below signs a real request against a real project
// and needs both. It reports the transport error through t.Error rather than
// panicking, but the failure is not about signing - there is no service to
// reach - so it says so and stops when nothing is configured.
func liveServiceConfigured() bool {
	return account.AccountFromEnv() != nil && restclient.LoadEndpointFromEnv() != ""
}

func skipWithoutLiveService(t *testing.T) {
	t.Helper()
	if !liveServiceConfigured() {
		t.Skip("needs a live MaxCompute project: set ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET and odps_endpoint")
	}
}
