package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
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
