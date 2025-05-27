package odps_test

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestTables_List(t *testing.T) {
	odpsIns.DefaultProject().Schemas().Get("mv").Tables().List(func(table *odps.Table, err error) {
		err = table.Load()
		if err != nil {
			t.Error(err)
		}
		print(table.Name() + "\t" + table.Type().String() + "\n")
	})
}
