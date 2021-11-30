package tunnel_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/tunnel"
	"os"
)

var tunnelIns tunnel.TableTunnel

func init() {
	accessId := os.Getenv("tunnel_odps_accessId")
	accessKey := os.Getenv("tunnel_odps_accessKey")
	odpsEndpoint := os.Getenv("odps_endpoint")
	tunnelEndpoint := os.Getenv("tunnel_odps_endpoint")

	account := odps.NewAliyunAccount(accessId, accessKey)
	odpsIns := odps.NewOdps(&account, odpsEndpoint)
	tunnelIns = tunnel.NewTableTunnel(odpsIns, tunnelEndpoint)
}

func ExampleUploadSession()  {
	session, err := tunnelIns.CreateUploadSession(
		"test_new_console_gcc",
		"sale_detail",
		"sale_date='202111',region='hangzhou'",
	)
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("status: %s", session.Status()))
		for _, column := range session.Schema().Columns {
			println(fmt.Sprintf("column: %+v", column))
		}

		for _, column := range session.Schema().PartitionColumns {
			println(fmt.Sprintf("partition key: %+v", column))
		}

		var _ = session.Schema().ToArrowSchema()
	}

	// Output:
}