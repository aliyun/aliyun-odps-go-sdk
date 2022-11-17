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
	"testing"
)

func TestDataScan(t *testing.T) {
	decimal, _ := DecimalFromStr("100.23")
	array := NewArray()
	array.Append(Int(1), Int(2))

	m := NewMap()
	sa := String("a")
	sb := String("b")
	m.Set(&sa, &sb)

	st := NewStruct()

	d, _ := NewDate("2021-12-08")
	st.SetField("a", Float(10.23))
	st.SetField("b", d)

	values := []struct {
		src Data
		dst Data
	}{
		{decimal, new(Decimal)},
		{TinyInt(10), new(TinyInt)},
		{array, new(Array)},
		{m, new(Map)},
		{st, new(Struct)},
		{d, new(Date)},
		{Binary([]byte{1, 2, 3}), new(Binary)},
	}

	for _, value := range values {
		src, dst := value.src, value.dst
		err := tryConvertType(src, dst)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		if src.String() != dst.String() {
			t.Fatalf("expected %s, but get %s", src, dst)
		}
	}

	var dd Date
	err := dd.Scan(d)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var bb Binary
	err = bb.Scan(Binary([]byte{1, 2, 3}))
	if err != nil {
		t.Fatalf("%+v", err)
	}
}
