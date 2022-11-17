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
	"github.com/pkg/errors"
	"io"
)

var ArrowCrcErr = errors.New("crc value error when get a tunnel arrow stream")

type arrowHttpReaderStatus int

const (
	readToChunkStatus = iota
	readFromChunkStatus
)

type ArrowStreamReader struct {
	inner     io.ReadCloser
	chunkCrc  Crc32CheckSum
	globalCrc Crc32CheckSum
	chunkSize int
	chunk     *chunk
	firstRead bool
	eof       bool
	status    arrowHttpReaderStatus
}

func NewArrowStreamReader(rc io.ReadCloser) *ArrowStreamReader {
	return &ArrowStreamReader{
		inner:     rc,
		chunkCrc:  NewCrc32CheckSum(),
		globalCrc: NewCrc32CheckSum(),
		firstRead: true,
		status:    readToChunkStatus,
	}
}

func (ar *ArrowStreamReader) ReadChunk() error {
	ar.status = readFromChunkStatus

	// read chunk size from the first 4 bytes
	if ar.firstRead {
		chunkSize, err := ar.readUint32()
		if err != nil {
			return errors.WithStack(err)
		}

		ar.chunkSize = int(chunkSize) + 4
		ar.chunk = newChunk(ar.chunkSize)
		ar.firstRead = false
	}

	// read chunkSize bytes or read to end of inner reader
	ar.chunk.reset()
	n, err := io.ReadFull(ar.inner, ar.chunk.buf)

	switch err {
	case nil:
		ar.chunk.setLength(ar.chunkSize)
		dataBytes := ar.chunk.bytes()[0 : ar.chunkSize-4]
		crcBytes := ar.chunk.bytes()[ar.chunkSize-4:]
		ar.chunk.truncate(ar.chunkSize - 4)

		ar.chunkCrc.Update(dataBytes)
		ar.globalCrc.Update(dataBytes)

		chunkCrcValue := bytesToUint32(crcBytes)
		crcExpected := ar.chunkCrc.Value()

		if chunkCrcValue != crcExpected {
			return ArrowCrcErr
		}
	case io.EOF, io.ErrUnexpectedEOF:
		ar.eof = true
		ar.chunk.setLength(n)

		if ar.chunk.length() < 4 {
			return errors.New("not enough bytes, at least 4 bytes for crc value")
		}

		dataBytes := ar.chunk.bytes()[0 : n-4]
		crcBytes := ar.chunk.bytes()[n-4:]
		ar.chunk.truncate(n - 4)

		ar.globalCrc.Update(dataBytes)
		globalCrcValue := bytesToUint32(crcBytes)
		crcExpected := ar.globalCrc.Value()
		ar.globalCrc.Reset()

		if globalCrcValue != crcExpected {
			return ArrowCrcErr
		}

		return nil
	}

	// err != nil and err != io.EOF
	return errors.WithStack(err)
}

func (ar *ArrowStreamReader) Read(dst []byte) (int, error) {
	// read chunkSize bytes or read to end of inner reader
	if ar.status == readToChunkStatus {
		err := ar.ReadChunk()
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}

	n, err := io.ReadFull(ar.chunk, dst)
	if err == io.EOF {
		ar.status = readToChunkStatus
	} else if err != nil {
		return n, errors.WithStack(err)
	}

	if ar.chunk.length() == 0 && ar.eof {
		return n, io.EOF
	}

	return n, nil
}

func (ar *ArrowStreamReader) Close() error {
	return errors.WithStack(ar.inner.Close())
}

func (ar *ArrowStreamReader) readUint32() (uint32, error) {
	uint32Bytes := make([]byte, 4)
	_, err := io.ReadFull(ar.inner, uint32Bytes)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return bytesToUint32(uint32Bytes), nil
}

type chunk struct {
	buf    []byte
	end    int
	offset int
}

func newChunk(chunkSize int) *chunk {
	return &chunk{
		buf:    make([]byte, chunkSize),
		offset: 0,
		end:    0,
	}
}

func (c *chunk) setLength(n int) {
	c.end = n
}

func (c *chunk) length() int {
	return c.end - c.offset
}

func (c *chunk) bytes() []byte {
	return c.buf[c.offset:c.end]
}

func (c *chunk) truncate(n int) {
	c.end = n
}

func (c *chunk) reset() {
	c.offset = 0
	c.end = 0
}

func (c *chunk) Read(p []byte) (n int, err error) {
	if c.offset >= c.end {
		return 0, io.EOF
	}

	n = copy(p, c.bytes())
	c.offset += n

	return n, nil
}

func bytesToUint32(b []byte) uint32 {
	return (uint32(b[0])&0xFF)<<24 +
		(uint32(b[1])&0xFF)<<16 +
		(uint32(b[2])&0xFF)<<8 +
		(uint32(b[3]) & 0xFF)
}
