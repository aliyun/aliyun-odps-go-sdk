package account_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

func TestNewAliyunAccount(t *testing.T) {
	aliyunAccount := account.NewAliyunAccount("ak", "sk")
	t.Log(aliyunAccount.AccessId())
	t.Log(aliyunAccount.AccessKey())
	t.Log(aliyunAccount.RegionId())
	t.Log(aliyunAccount.GetType())

	aliyunAccount = account.NewAliyunAccount("ak", "sk", "regionId")
	t.Log(aliyunAccount.AccessId())
	t.Log(aliyunAccount.AccessKey())
	t.Log(aliyunAccount.RegionId())
	t.Log(aliyunAccount.GetType())
}

func TestSignatureV4(t *testing.T) {
	const endpoint = "https://example.test/api"
	req, err := http.NewRequest(http.MethodGet, endpoint+"/projects/test_project", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Date", "Mon, 05 Oct 2026 00:00:00 GMT")
	signer := account.NewStsAccount("test-ak", "test-sk", "test-sts-token", "cn-shanghai")
	if err := signer.SignRequest(req, endpoint); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get(common.HttpHeaderAuthorization); !strings.HasPrefix(got, "ODPS test-ak/20261005/cn-shanghai/odps/") {
		t.Fatalf("unexpected signing scope: %q", got)
	}
	if got := req.Header.Get(common.HttpHeaderAuthorizationSTSToken); got != "test-sts-token" {
		t.Fatalf("unexpected test session token: %q", got)
	}
}

func TestRegionId(t *testing.T) {
	aliyunAccount := account.NewAliyunAccount("ak", "sk", "cn-shanghai")
	odpsIns := odps.NewOdps(aliyunAccount, "endpoint")

	region := odpsIns.RegionId()
	t.Log(region)
	// expect region == cn-shanghai
	if region != "cn-shanghai" {
		t.Error("region is not cn-shanghai")
	}
}

func TestCorporation(t *testing.T) {
	aliyunAccount := account.NewAliyunAccount("ak", "sk", "cn-shanghai")
	account.SetCorporation("apsara")
	t.Cleanup(func() { account.SetCorporation("aliyun") })

	request, err := http.NewRequest("GET", "http://www.mock.com", nil)
	if err != nil {
		t.Error(err)
	}
	err = aliyunAccount.SignRequest(request, "http://www.mock.com")
	if err != nil {
		t.Error(err)
	}
	auth := request.Header.Get(common.HttpHeaderAuthorization)
	t.Log(auth)
	// expect auth has 'apsara'
	if !strings.Contains(auth, "apsara") {
		t.Error("auth does not contain 'apsara'")
	}
}
