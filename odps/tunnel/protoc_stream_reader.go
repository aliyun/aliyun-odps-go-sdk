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
	"math"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

type ProtocStreamReader struct {
	inner io.Reader
}

func NewProtocStreamReader(r io.Reader) *ProtocStreamReader {
	return &ProtocStreamReader{
		inner: r,
	}
}

func (r *ProtocStreamReader) ReadTag() (protowire.Number, protowire.Type, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	num, typ := protowire.DecodeTag(m)
	if num < protowire.MinValidNumber {
		return 0, 0, errors.New("failed to read a tag from the proto stream")
	}

	return num, typ, nil
}

func (r *ProtocStreamReader) ReadVarint() (uint64, error) {
	b := []byte{'0'}
	buf := make([]byte, 0, 1)

	// 每次读取一个字节，直到读到合法的varint-encoded length值
	for {
		n, err := r.inner.Read(b)
		if n <= 0 {
			return 0, errors.WithStack(err)
		}

		buf = append(buf, b...)

		// 为了支持64位整数表示的长度，varInts最多需要10个字节
		if len(buf) > 10 {
			return 0, errors.New("invalid bytes for varint")
		}

		v, n := protowire.ConsumeVarint(buf)
		if n > 0 {
			return v, nil
		}
	}
}

func (r *ProtocStreamReader) ReadFixed32() (uint32, error) {
	b := []byte{'0', '0', '0', '0'}

	for {
		n, err := io.ReadFull(r.inner, b)
		if n != 4 {
			return 0, errors.WithStack(err)
		}

		v, _ := protowire.ConsumeFixed32(b)
		return v, nil
	}
}

func (r *ProtocStreamReader) ReadFixed64() (uint64, error) {
	b := []byte{'0', '0', '0', '0', '0', '0', '0', '0'}

	for {
		n, err := io.ReadFull(r.inner, b)
		if n != 8 {
			return 0, errors.WithStack(err)
		}

		v, _ := protowire.ConsumeFixed64(b)
		return v, nil
	}
}

func (r *ProtocStreamReader) ReadBytes() ([]byte, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	b := make([]byte, m)
	_, err = io.ReadFull(r.inner, b)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return b, nil
}

func (r *ProtocStreamReader) ReadBool() (bool, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return false, errors.WithStack(err)
	}

	return protowire.DecodeBool(m), nil
}

func (r *ProtocStreamReader) ReadInt32() (int32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int32(m), nil
}

func (r *ProtocStreamReader) ReadSInt32() (int32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int32(protowire.DecodeZigZag(m & math.MaxUint32)), nil
}

func (r *ProtocStreamReader) ReadUInt32() (uint32, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return uint32(m), nil
}

func (r *ProtocStreamReader) ReadInt64() (int64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int64(m), nil
}

func (r *ProtocStreamReader) ReadSInt64() (int64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return protowire.DecodeZigZag(m), nil
}

func (r *ProtocStreamReader) ReadUInt64() (uint64, error) {
	m, err := r.ReadVarint()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return uint64(m), nil
}

func (r *ProtocStreamReader) ReadSFixed32() (int32, error) {
	v, err := r.ReadFixed32()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int32(v), nil
}

func (r *ProtocStreamReader) ReadFloat32() (float32, error) {
	v, err := r.ReadFixed32()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return math.Float32frombits(v), nil
}

func (r *ProtocStreamReader) ReadSFixed64() (int64, error) {
	v, err := r.ReadFixed64()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int64(v), nil
}

func (r *ProtocStreamReader) ReadFloat64() (float64, error) {
	v, err := r.ReadFixed64()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return math.Float64frombits(v), nil
}

func (r *ProtocStreamReader) ReadString() (string, error) {
	v, err := r.ReadBytes()
	if err != nil {
		return "", errors.WithStack(err)
	}

	return string(v), nil
}
