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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"log"
	"time"
)

func ExampleOdps_RunSQL() {

	column := tableschema.Column{
		Name: "id",
		Type: datatype.DateType,
	}
	tableSchema := tableschema.NewSchemaBuilder().Columns(column).Name("has_date").Build()
	err2 := odpsIns.Tables().Create(tableSchema, true, nil, nil)
	if err2 != nil {
		log.Fatalf("%+v", err2)
	}

	t := time.Now()
	ts := t.Format("2006-01-02")
	sql := fmt.Sprintf("insert into has_date values (date'%s');", ts)

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	// Output:
}
