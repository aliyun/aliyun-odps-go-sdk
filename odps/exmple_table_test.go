package odps_test

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func ExampleTable() {
	table := odps.NewTable(odpsIns, "odps_smoke_test", "user")
	var _ = table.Load()
	print(fmt.Sprintf("%#v", table))
	// Output:
}

func ExampleTables() {
	// Output:
}
