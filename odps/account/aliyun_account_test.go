package account_test

import (
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

func TestNewAliyunAccount(t *testing.T) {
	aliyunAccount := account.NewApsaraAccount("ak", "sk")
	t.Log(aliyunAccount.AccessId())
	t.Log(aliyunAccount.AccessKey())
	t.Log(aliyunAccount.RegionId())
	t.Log(aliyunAccount.GetType())

	aliyunAccount = account.NewApsaraAccount("ak", "sk", "regionId")
	t.Log(aliyunAccount.AccessId())
	t.Log(aliyunAccount.AccessKey())
	t.Log(aliyunAccount.RegionId())
	t.Log(aliyunAccount.GetType())
}

func TestSignatureV4(t *testing.T) {
	var ak, sk, stsToken string
	if accessId, found := os.LookupEnv("ACCESS_KEY_ID"); found {
		ak = accessId
	}

	if accessKey, found := os.LookupEnv("ACCESS_KEY_SECRET"); found {
		sk = accessKey
	}
	if token, found := os.LookupEnv("SECURITY_TOKEN"); found {
		stsToken = token
	}
	endpoint := restclient.LoadEndpointFromEnv()
	aliyunAccount := account.NewStsAccount(ak, sk, stsToken, "cn-shanghai")
	odpsIns := odps.NewOdps(aliyunAccount, endpoint)
	odpsIns.SetDefaultProjectName("go_sdk_regression_testing")

	// ignore AkV4 Test because unrelated env is not ready
	// err := odpsIns.DefaultProject().Load()
	//if err != nil {
	//	t.Error(err)
	//}
	//t.Log(odpsIns.DefaultProject().RegionId())
}

func TestRegionId(t *testing.T) {
	aliyunAccount := account.NewApsaraAccount("ak", "sk", "cn-shanghai")
	odpsIns := odps.NewOdps(aliyunAccount, "endpoint")

	region := odpsIns.RegionId()
	t.Log(region)
	// expect region == cn-shanghai
	if region != "cn-shanghai" {
		t.Error("region is not cn-shanghai")
	}
}

func TestCorporation(t *testing.T) {
	aliyunAccount := account.NewApsaraAccount("ak", "sk", "cn-shanghai")
	account.SetCorporation("apsara")

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
