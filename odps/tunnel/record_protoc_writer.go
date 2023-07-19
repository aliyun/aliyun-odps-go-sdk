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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
)

type RecordProtocWriter struct {
	httpRes             *httpConnection
	writeCloser         io.Closer
	protocWriter        *ProtocStreamWriter
	columns             []tableschema.Column
	shouldTransformDate bool
	recordCrc           Crc32CheckSum
	crcOfCrc            Crc32CheckSum // crc of record crc
	count               int64
}

func newRecordProtocWriter(w io.WriteCloser, columns []tableschema.Column, shouldTransformDate bool) RecordProtocWriter {
	return RecordProtocWriter{
		httpRes:             nil,
		writeCloser:         w,
		protocWriter:        NewProtocStreamWriter(w),
		columns:             columns,
		shouldTransformDate: shouldTransformDate,
		recordCrc:           NewCrc32CheckSum(),
		crcOfCrc:            NewCrc32CheckSum(),
	}
}

func newRecordProtocHttpWriter(conn *httpConnection, columns []tableschema.Column, shouldTransformDate bool) RecordProtocWriter {
	return RecordProtocWriter{
		httpRes:             conn,
		writeCloser:         conn.Writer,
		protocWriter:        NewProtocStreamWriter(conn.Writer),
		columns:             columns,
		shouldTransformDate: shouldTransformDate,
		recordCrc:           NewCrc32CheckSum(),
		crcOfCrc:            NewCrc32CheckSum(),
	}
}

func (r *RecordProtocWriter) Write(record data.Record) error {
	recordColNum := record.Len()

	if recordColNum > len(r.columns) {
		return errors.New("record values are more than schema.")
	}

	for colIndex, value := range record {
		if value == nil {
			continue
		}

		r.recordCrc.Update(int32(colIndex + 1))
		err := r.writeFieldTag(colIndex+1, r.columns[colIndex].Type)
		if err != nil {
			return errors.WithStack(err)
		}

		err = r.writeField(value)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	err := r.protocWriter.WriteTag(EndRecord, protowire.VarintType)
	if err != nil {
		return errors.WithStack(err)
	}

	recordCrcVal := r.recordCrc.Value()
	err = r.protocWriter.WriteUInt32(recordCrcVal)
	if err != nil {
		return errors.WithStack(err)
	}

	r.recordCrc.Reset()
	r.crcOfCrc.Update(recordCrcVal)
	r.count += 1

	return nil
}

func (r *RecordProtocWriter) writeFieldTag(colIndex int, dt datatype.DataType) error {
	var wireType protowire.Type

	switch dt.ID() {
	case datatype.DATETIME,
		datatype.BOOLEAN,
		datatype.BIGINT,
		datatype.TINYINT,
		datatype.SMALLINT,
		datatype.INT,
		datatype.DATE,
		datatype.IntervalYearMonth:
		wireType = protowire.VarintType
	case datatype.DOUBLE:
		wireType = protowire.Fixed64Type
	case datatype.FLOAT:
		wireType = protowire.Fixed32Type
	case datatype.IntervalDayTime,
		datatype.TIMESTAMP,
		datatype.STRING,
		datatype.CHAR,
		datatype.VARCHAR,
		datatype.BINARY,
		datatype.DECIMAL,
		datatype.ARRAY,
		datatype.MAP,
		datatype.STRUCT:
		wireType = protowire.BytesType
	default:
		return errors.Errorf("Invalid data type, %s", dt.Name())
	}

	err := r.protocWriter.WriteTag(protowire.Number(int32(colIndex)), wireType)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *RecordProtocWriter) writeField(val data.Data) error {
	switch val := val.(type) {
	case data.Double:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteFloat64(float64(val)))
	case data.Float:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteFloat32(float32(val)))
	case data.Bool:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteBool(bool(val)))
	case data.BigInt:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteSInt64(int64(val)))
	case data.IntervalYearMonth:
		r.recordCrc.Update(int64(val))
		return errors.WithStack(r.protocWriter.WriteSInt64(int64(val)))
	case data.Int:
		r.recordCrc.Update(int64(val))
		return errors.WithStack(r.protocWriter.WriteSInt64(int64(val)))
	case data.SmallInt:
		r.recordCrc.Update(int64(val))
		return errors.WithStack(r.protocWriter.WriteSInt64(int64(val)))
	case data.TinyInt:
		r.recordCrc.Update(int64(val))
		return errors.WithStack(r.protocWriter.WriteSInt64(int64(val)))
	case *data.String:
		b := []byte(string(*val))
		r.recordCrc.Update(b)
		return errors.WithStack(r.protocWriter.WriteBytes(b))
	case *data.VarChar:
		b := []byte(val.Data())
		r.recordCrc.Update(b)
		return errors.WithStack(r.protocWriter.WriteBytes(b))
	case *data.Char:
		b := []byte(val.Data())
		r.recordCrc.Update(b)
		return errors.WithStack(r.protocWriter.WriteBytes(b))
	case data.Binary:
		r.recordCrc.Update(val)
		return errors.WithStack(r.protocWriter.WriteBytes(val))
	case data.DateTime:
		t := val.Time()
		// 应该直接写成： milliSeconds := t.UnixMilli()
		// 但是 Time.UnixMilli is added in go.1.17
		// func (t Time) UnixMilli() int64 {
		//	return t.unixSec()*1e3 + int64(t.nsec())/1e6
		// }
		unixSec := t.Unix()
		nanoSec := t.Nanosecond()
		milliSeconds := unixSec*1e3 + int64(nanoSec)/1e6

		// TODO 需要根据schema中的shouldTransform，来确定是否将时间转换为本地时区的时间
		r.recordCrc.Update(milliSeconds)
		return errors.WithStack(r.protocWriter.WriteSInt64(milliSeconds))
	case data.Date:
		t := val.Time()
		// 获取从1970年以来的天数
		days := int64(t.Sub(epochDay).Hours() / 24)
		r.recordCrc.Update(days)
		return errors.WithStack(r.protocWriter.WriteSInt64(days))
	case data.IntervalDayTime:
		seconds := val.Seconds()
		nanoSeconds := val.NanosFraction()

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)
		err := r.protocWriter.WriteSInt64(seconds)
		if err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(r.protocWriter.WriteSInt32(nanoSeconds))
	case data.Timestamp:
		t := val.Time()
		seconds := t.Unix()
		nanoSeconds := int32(t.Nanosecond())

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)

		err := r.protocWriter.WriteSInt64(seconds)
		if err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(r.protocWriter.WriteSInt32(nanoSeconds))
	case *data.Decimal:
		b := []byte(val.Value())
		r.recordCrc.Update(b)
		return errors.WithStack(r.protocWriter.WriteBytes(b))
	case *data.Array:
		return errors.WithStack(r.writeArray(val.ToSlice()))
	case *data.Map:
		return errors.WithStack(r.writeMap(val))
	case *data.Struct:
		return errors.WithStack(r.writeStruct(val))
	}

	return errors.Errorf("invalid data type %t", val)
}

func (r *RecordProtocWriter) writeArray(val []data.Data) error {
	err := r.protocWriter.WriteInt32(int32(len(val)))
	if err != nil {
		return errors.WithStack(err)
	}

	for _, d := range val {
		if d == nil {
			err = r.protocWriter.WriteBool(true)
			if err != nil {
				return errors.WithStack(err)
			}
		} else {
			err = r.protocWriter.WriteBool(false)
			if err != nil {
				return errors.WithStack(err)
			}

			err = r.writeField(d)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (r *RecordProtocWriter) writeMap(val *data.Map) error {
	m := val.ToGoMap()
	l := len(m)
	keys, values := make([]data.Data, 0, l), make([]data.Data, 0, l)

	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}

	err := r.writeArray(keys)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(r.writeArray(values))
}

func (r *RecordProtocWriter) writeStruct(val *data.Struct) error {
	for _, field := range val.Fields() {
		if field.Value == nil {
			err := r.protocWriter.WriteBool(true)
			if err != nil {
				return errors.WithStack(err)
			}
		} else {
			err := r.protocWriter.WriteBool(false)
			if err != nil {
				return errors.WithStack(err)
			}

			err = r.writeField(field.Value)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (r *RecordProtocWriter) Close() error {
	err := r.protocWriter.WriteTag(MetaCount, protowire.VarintType)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.protocWriter.WriteSInt64(r.count)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.protocWriter.WriteTag(MetaChecksum, protowire.VarintType)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.protocWriter.WriteUInt32(r.crcOfCrc.Value())
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.writeCloser.Close()
	if err != nil {
		return errors.WithStack(err)
	}

	if r.httpRes != nil {
		return errors.WithStack(r.httpRes.closeRes())
	}

	return nil
}
