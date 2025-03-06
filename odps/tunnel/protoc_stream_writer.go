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

	"google.golang.org/protobuf/encoding/protowire"
)

type ProtocStreamWriter struct {
	inner io.Writer
}

func NewProtocStreamWriter(w io.Writer) *ProtocStreamWriter {
	return &ProtocStreamWriter{
		inner: w,
	}
}

func (r *ProtocStreamWriter) WriteTag(num protowire.Number, typ protowire.Type) error {
	return r.WriteVarint(protowire.EncodeTag(num, typ))
}

func (r *ProtocStreamWriter) WriteVarint(v uint64) error {
	b := protowire.AppendVarint(nil, v)
	err := writeFull(r.inner, b)
	return err
}

func (r *ProtocStreamWriter) WriteFixed32(val uint32) error {
	b := protowire.AppendFixed32(nil, val)
	err := writeFull(r.inner, b)
	return err
}

func (r *ProtocStreamWriter) WriteFixed64(val uint64) error {
	b := protowire.AppendFixed64(nil, val)
	err := writeFull(r.inner, b)
	return err
}

func (r *ProtocStreamWriter) WriteBytes(data []byte) error {
	if err := r.WriteVarint(uint64(len(data))); err != nil {
		return err
	}
	err := writeFull(r.inner, data)
	return err
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (r *ProtocStreamWriter) WriteBool(val bool) error {
	// true: 1, false: 0
	u := protowire.EncodeBool(val)
	return r.WriteVarint(u)
}

func (r *ProtocStreamWriter) WriteInt32(val int32) error {
	return r.WriteVarint(uint64(val))
}

func (r *ProtocStreamWriter) WriteSInt32(val int32) error {
	u := protowire.EncodeZigZag(int64(val))
	return r.WriteVarint(u)
}

func (r *ProtocStreamWriter) WriteInt64(val int64) error {
	return r.WriteVarint(uint64(val))
}

func (r *ProtocStreamWriter) WriteSInt64(val int64) error {
	u := protowire.EncodeZigZag(val)
	return r.WriteVarint(u)
}

func (r *ProtocStreamWriter) WriteUInt32(val uint32) error {
	return r.WriteVarint(uint64(val))
}

func (r *ProtocStreamWriter) WriteUInt64(val uint64) error {
	return r.WriteVarint(val)
}

func (r *ProtocStreamWriter) WriteFloat32(val float32) error {
	return r.WriteFixed32(math.Float32bits(val))
}

func (r *ProtocStreamWriter) WriteFloat64(val float64) error {
	return r.WriteFixed64(math.Float64bits(val))
}
