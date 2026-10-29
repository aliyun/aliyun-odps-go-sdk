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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// NULLDiagnostic is the text used for a null value in a diagnostic rendering. It is
// exported so callers that parse this output (logs, assertions) can name it.
const NULLDiagnostic = "NULL"

// diagnosticValue renders one value for the String() (diagnostic) form of a container -
// an array element, a struct field, a record column.
//
// A null renders as the bare marker NULL. A non-null value whose own rendering is
// identical to that marker is quoted, so `array(NULL)` (a null element) and
// `array('NULL')` (a string whose text happens to be "NULL") are not the same text -
// the two used to be indistinguishable, which is exactly the confusion that makes a
// NULL-handling bug look like working code. Only colliding values are quoted: every
// other rendering, including the existing expectations for strings such as
// `struct<x:11,y:hello,...>`, is unchanged. SQL text goes through Sql(), which quotes
// consistently and is the right thing to compare against when a value must round-trip.
func diagnosticValue(d Data) string {
	if isNullValue(d) {
		return NULLDiagnostic
	}

	rendered := d.String()
	if rendered == NULLDiagnostic {
		return "'" + rendered + "'"
	}

	return rendered
}

// isNullValue reports the two ways a value can carry "no data": the nil Data
// interface, and an explicit data.Null - a non-nil NullData{} whose Type() is the
// NULL type and whose String() is already the marker text. Both keep the bare
// marker. Without the second case the quoting rule below turns data.Null into
// 'NULL', which is exactly how a string whose text is "NULL" renders, so an
// explicit null would become indistinguishable from that string - the opposite of
// what the rule exists for.
func isNullValue(d Data) bool {
	if d == nil {
		return true
	}

	return datatype.IsNullType(d.Type())
}
