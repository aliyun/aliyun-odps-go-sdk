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

package data

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"reflect"
)

type Data interface {
	Type() datatype.DataType
	fmt.Stringer
	Sql() string
}

func IsDataEqual(d1 Data, d2 Data) bool {
	if reflect.DeepEqual(d1, d2) {
		return true
	}

	return isMapEqual(d1, d2)
}

func isMapEqual(d1 Data, d2 Data) bool {
	m1, ok := d1.(*Map)
	if !ok {
		return false
	}

	m2, ok := d2.(*Map)
	if !ok {
		return false
	}

	if len(m1.data) != len(m2.data) {
		return false
	}

	for key, value1 := range m1.data {
		value2, ok := m2.data[key]
		if !ok {
			return false
		}

		if !IsDataEqual(value1, value2) {
			return false
		}
	}

	return true
}
