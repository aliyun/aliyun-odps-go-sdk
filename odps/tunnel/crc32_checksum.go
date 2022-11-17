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

package tunnel

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type Crc32CheckSum struct {
	table  *crc32.Table
	value  uint32
	buffer *bytes.Buffer
}

func NewCrc32CheckSum() Crc32CheckSum {
	return Crc32CheckSum{
		table:  crc32.MakeTable(crc32.Castagnoli),
		buffer: bytes.NewBuffer(make([]byte, 0, 8)),
	}
}

// Update can not use data of int type, as the size of int is different
// on 32 and 64 platform. In java the size of int is always 32 bits, so
// the same int data can generate different crc value when using java and go
func (crc *Crc32CheckSum) Update(data interface{}) {
	var _ = binary.Write(crc.buffer, binary.LittleEndian, data)
	crc.value = crc32.Update(crc.value, crc.table, crc.buffer.Bytes())
	crc.buffer.Reset()
}

func (crc *Crc32CheckSum) Value() uint32 {
	return crc.value
}

func (crc *Crc32CheckSum) Reset() {
	crc.value = 0
}
