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
	"github.com/pkg/errors"
)

type CharOverflowError struct {
	maxLen  int
	realLen int
}

type Char struct {
	length int
	data   string
}

type VarChar struct {
	length int
	data   string
}

func NewCharOverflowError(maxLen, realLen int) CharOverflowError {
	return CharOverflowError{maxLen, realLen}
}

func (c CharOverflowError) Error() string {
	return fmt.Sprintf("string length is %d, bigger than the max length %d", c.realLen, c.maxLen)
}

func NewChar(length int, data string) (*Char, error) {
	if length > 255 {
		return nil, errors.Errorf("max length of char is 255, not %d is given", length)
	}

	if len(data) > length {
		return nil, errors.WithStack(NewCharOverflowError(length, len(data)))
	}

	return &Char{length: length, data: data}, nil
}

func (c *Char) Type() datatype.DataType {
	return datatype.NewCharType(c.length)
}

func (c *Char) String() string {
	return c.data
}

func (c *Char) Sql() string {
	return fmt.Sprintf("cast('%s' as char(%d))", c.data, c.length)
}

func (c *Char) Data() string {
	return c.data
}

func (c *Char) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, c))
}

func NewVarChar(length int, data string) (*VarChar, error) {
	if length > 65536 {
		return nil, errors.Errorf("max length of char is 65536, not %d is given", length)
	}

	if len(data) > length {
		return nil, errors.WithStack(NewCharOverflowError(length, len(data)))
	}

	return &VarChar{length: length, data: data}, nil
}

func (v *VarChar) Type() datatype.DataType {
	return datatype.NewVarcharType(v.length)
}

func (v *VarChar) String() string {
	return v.data
}

func (v *VarChar) Sql() string {
	return fmt.Sprintf("cast('%s' as varchar(%d))", v.data, v.length)
}

func (v *VarChar) Scan(value interface{}) error {
	return errors.WithStack(tryConvertType(value, v))
}

func (v *VarChar) Data() string {
	return v.data
}
