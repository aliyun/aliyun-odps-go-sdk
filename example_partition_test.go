package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
)

func ExamplePartition_Load() {
	kv := make(map[string]string, 2)
	kv["sale_date"] = "201910"
	kv["region"] = "shanghai"

	partition := odps.NewPartition(odpsIns, "project_1", "sale_detail", kv)
	err := partition.Load()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("Name: %s", partition.Name()))
		println(fmt.Sprintf("Record number: %d", partition.RecordNum()))
		println(fmt.Sprintf("Create Time: %s", partition.CreatedTime()))
	}

	// Output:
}

func ExamplePartition_LoadExtended() {
	kv := make(map[string]string, 2)
	kv["sale_date"] = "201910"
	kv["region"] = "shanghai"

	partition := odps.NewPartition(odpsIns, "project_1", "sale_detail", kv)
	err := partition.LoadExtended()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("Name: %s", partition.Name()))
		println(fmt.Sprintf("File number: %d", partition.FileNumEx()))
		println(fmt.Sprintf("PhysicalSizefd: %d", partition.PhysicalSizeEx()))
		println(fmt.Sprintf("Reserved: %s", partition.ReservedEx()))
	}

	// Output:
}

