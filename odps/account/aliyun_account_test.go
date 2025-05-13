package account_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
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
