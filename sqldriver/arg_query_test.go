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

package sqldriver

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/stretchr/testify/require"
)

func TestSQLParameterBinding(t *testing.T) {
	instant := time.Date(2024, 1, 2, 3, 4, 5, 6, time.FixedZone("plus-8", 8*60*60))
	named := func(name string, value driver.Value) driver.NamedValue {
		return driver.NamedValue{Name: name, Value: value}
	}
	for _, tt := range []struct {
		name  string
		query string
		args  []driver.NamedValue
		want  string
	}{
		{"positional", "select '?', ?, 1-?, 2-?", []driver.NamedValue{named("", "O'Reilly\\北京"), named("", int64(-2)), named("", float64(2.5))},
			"select '?', 'O\\'Reilly\\\\北京', 1-(-2), 2-(2.5D)"},
		{"named", "select @name, '@name', @name -- @x", []driver.NamedValue{named("name", "tom")},
			"select 'tom', '@name', 'tom' -- @x"},
		{"comments and quotes", "select /* ? */ \"\\\"?\", \x60?\x60, 'it''s ?', ? -- ?\r\n", []driver.NamedValue{named("", int64(3))},
			"select /* ? */ \"\\\"?\", \x60?\x60, 'it''s ?', 3 -- ?\r\n"},
		{"raw string", "select R'('?'\\@name)', ?, r\"(\"?\\)\"", []driver.NamedValue{named("", true)},
			"select R'('?'\\@name)', true, r\"(\"?\\)\""},
		{"values", "select ?, ?, ?, ?, ?, ?", []driver.NamedValue{
			named("", nil), named("", []byte(nil)), named("", []byte{0, 255}), named("", instant),
			named("", data.NewDecimal(8, 3, "-1.020")), named("", "line\n\x007"),
		}, "select NULL, NULL, UNHEX('00ff'), TIMESTAMP '2024-01-01 19:04:05.000000006Z', (-1.020BD), 'line\\012\\000\\067'"},
		{"script variable", "@result := select 1; select * from @result;", nil, "@result := select 1; select * from @result;"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bindSQL(tt.query, tt.args)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
	for _, tt := range []struct {
		query string
		args  []driver.NamedValue
		want  string
	}{
		{"select ?, ?", []driver.NamedValue{named("", int64(1))}, "expected 2 positional SQL arguments, got 1"},
		{"select ?", nil, "expected 1 positional SQL arguments, got 0"},
		{"select 1", []driver.NamedValue{named("", 1)}, "unsupported parameter type"},
		{"select 1", []driver.NamedValue{named("", int64(1))}, "expected 0 positional SQL arguments, got 1"},
		{"select @a", []driver.NamedValue{named("b", int64(1))}, "missing SQL argument @a"},
		{"select @a", []driver.NamedValue{named("a", int64(1)), named("b", int64(2))}, "unused SQL argument @b"},
		{"select @a", []driver.NamedValue{named("a", int64(1)), named("a", int64(2))}, "duplicate SQL argument @a"},
		{"select @a, ?", []driver.NamedValue{named("a", int64(1)), named("", int64(2))}, "cannot mix"},
		{"select @a, ?", []driver.NamedValue{named("a", int64(1))}, "cannot bind '?'"},
		{"select '?", []driver.NamedValue{named("", int64(1))}, "unterminated SQL quote"},
		{"select ?", []driver.NamedValue{named("", math.Inf(1))}, "non-finite"},
		{"select ?", []driver.NamedValue{named("", "\xff")}, "invalid UTF-8"},
		{"select ?", []driver.NamedValue{named("", data.NewDecimal(7, 0, "invalid"))}, "invalid DECIMAL"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			_, err := bindSQL(tt.query, tt.args)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
