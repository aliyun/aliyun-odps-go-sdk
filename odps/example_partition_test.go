// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps_test

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"log"
)

func ExamplePartition_Load() {
	partition := odps.NewPartition(odpsIns, "project_1", "sale_detail", "sale_date=201910/region=shanghai")
	err := partition.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("Value: %s", partition.Value()))
	println(fmt.Sprintf("Record number: %d", partition.RecordNum()))
	println(fmt.Sprintf("Create Time: %s", partition.CreatedTime()))

	// Output:
}

func ExamplePartition_LoadExtended() {
	partition := odps.NewPartition(odpsIns, "project_1", "sale_detail", "sale_date=201910/region=shanghai")
	err := partition.LoadExtended()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("Value: %s", partition.Value()))
	println(fmt.Sprintf("File number: %d", partition.FileNumEx()))
	println(fmt.Sprintf("PhysicalSizefd: %d", partition.PhysicalSizeEx()))
	println(fmt.Sprintf("Reserved: %s", partition.ReservedEx()))
	// Output:
}
