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

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

// Geography 承载 MaxCompute GEOGRAPHY 列的 WKB (Well-Known Binary) 字节。
// 不内置 WKT/WKB 互转；如需 WKT 请自行使用 paulmach/orb 或 twpayne/go-geom 解析。
type Geography []byte

func (g Geography) Type() datatype.DataType {
	return datatype.GeographyType
}

// AsBinary 返回 WKB 字节。
func (g Geography) AsBinary() []byte {
	return []byte(g)
}

func (g Geography) String() string {
	return fmt.Sprintf("unhex('%X')", []byte(g))
}

// Sql 返回可直接拼入 SQL 字面量的表达式。
func (g Geography) Sql() string {
	return fmt.Sprintf("ST_GEOGFROMWKB(unhex('%X'))", []byte(g))
}

func (g *Geography) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, g))
}
