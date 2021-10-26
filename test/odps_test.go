package testu

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"testing"
)

func TestOdps(t *testing.T)  {
	odpsIns := odps.NewOdps(DefaultAccount)

	odpsIns.Projects()
}
