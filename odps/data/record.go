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

import "strings"

type Record []Data

func NewRecord(columnNums int) Record {
	return make([]Data, columnNums)
}

func (r *Record) Len() int {
	return len(*r)
}

func (r *Record) Get(i int) Data {
	return (*r)[i]
}

func (r *Record) String() string {
	var sb strings.Builder
	sb.WriteString("[")

	n := len(*r) - 1
	for i, f := range *r {
		sb.WriteString(f.String())

		if i < n {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("]")

	return sb.String()
}
