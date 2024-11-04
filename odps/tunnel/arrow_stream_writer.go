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
	"io"

	"github.com/pkg/errors"
)

const DefaultChunkSize = 65536

// ArrowStreamWriter calculates the crc value in chunk unit
type ArrowStreamWriter struct {
	inner              io.WriteCloser
	chunkCrc           Crc32CheckSum
	globalCrc          Crc32CheckSum
	currentChunkLength int
	firstWrite         bool
}

func NewArrowStreamWriter(w io.WriteCloser) *ArrowStreamWriter {
	return &ArrowStreamWriter{
		inner:              w,
		chunkCrc:           NewCrc32CheckSum(),
		globalCrc:          NewCrc32CheckSum(),
		currentChunkLength: 0,
		firstWrite:         true,
	}
}

func (aw *ArrowStreamWriter) Write(data []byte) (int, error) {
	if aw.firstWrite {
		err := aw.writeUint32(DefaultChunkSize)
		if err != nil {
			return 0, errors.WithStack(err)
		}

		aw.firstWrite = false
	}

	totalWrite := 0

	for dataLength := len(data); totalWrite < dataLength; {
		toWriteN := min(aw.leftChunkLength(), dataLength-totalWrite)
		bytesToWrite := data[totalWrite : totalWrite+toWriteN]

		totalWrite += toWriteN
		aw.currentChunkLength += toWriteN

		aw.chunkCrc.Update(bytesToWrite)
		aw.globalCrc.Update(bytesToWrite)

		_, err := aw.writeAll(bytesToWrite)
		if err != nil {
			return 0, errors.WithStack(err)
		}

		if aw.chunkIsFull() {
			crc := aw.chunkCrc.Value()
			err = aw.writeUint32(crc)
			aw.chunkCrc.Reset()
			if err != nil {
				return 0, errors.WithStack(err)
			}
			aw.currentChunkLength = 0
		}
	}

	return totalWrite, nil
}

func (aw *ArrowStreamWriter) Close() error {
	crc := aw.globalCrc.Value()
	err1 := aw.writeUint32(crc)
	aw.globalCrc.Reset()

	err2 := aw.inner.Close()

	if err1 != nil {
		return errors.WithStack(err1)
	}

	return errors.WithStack(err2)
}

func (aw *ArrowStreamWriter) chunkIsFull() bool {
	return aw.currentChunkLength >= DefaultChunkSize
}

func (aw *ArrowStreamWriter) leftChunkLength() int {
	return DefaultChunkSize - aw.currentChunkLength
}

func (aw *ArrowStreamWriter) writeUint32(crcValue uint32) error {
	b := uint32ToBytes(crcValue)
	var _, err = aw.writeAll(b)

	return errors.WithStack(err)
}

func (aw *ArrowStreamWriter) writeAll(b []byte) (int, error) {
	total := len(b)

	for hasWrite := 0; hasWrite < total; {
		n, err := aw.inner.Write(b)
		hasWrite += n

		if err != nil {
			return hasWrite, errors.WithStack(err)
		}
	}

	return total, nil
}

func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(n >> 24 & 0x000000FF)
	b[1] = byte(n >> 16 & 0x000000FF)
	b[2] = byte(n >> 8 & 0x000000FF)
	b[3] = byte(n & 0x000000FF)

	return b
}
