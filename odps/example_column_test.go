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
	"encoding/json"
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func ExampleColumn() {
	raw := `[{
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "name",
            "type": "string"
           },
           {
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "age",
            "type": "bigint"
           },
           {
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "property",
            "type": "map<varchar(100), struct<a:int, b:int>>"
           }
           ]`

	var column []tableschema.Column

	err := json.Unmarshal([]byte(raw), &column)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Printf("%s: %s\n", column[0].Name, column[0].Type)
	fmt.Printf("%s: %s\n", column[1].Name, column[1].Type)
	fmt.Printf("%s: %s\n", column[2].Name, column[2].Type)

	// Output:
	// name: STRING
	// age: BIGINT
	// property: MAP<VARCHAR(100),STRUCT<`a`:INT,`b`:INT>>
}
