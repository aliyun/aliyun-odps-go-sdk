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
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

type RecordProtocReader struct {
	httpRes             *http.Response // TODO 改成和ArrowStreamStream一样，用io.ReaderCloser
	protocReader        *ProtocStreamReader
	columns             []tableschema.Column
	shouldTransformDate bool
	recordCrc           Crc32CheckSum
	crcOfCrc            Crc32CheckSum // crc of record crc
	count               int64
}

func newRecordProtocReader(httpRes *http.Response, columns []tableschema.Column, shouldTransformDate bool) RecordProtocReader {
	return RecordProtocReader{
		httpRes:             httpRes,
		protocReader:        NewProtocStreamReader(httpRes.Body),
		columns:             columns,
		shouldTransformDate: shouldTransformDate,
		recordCrc:           NewCrc32CheckSum(),
		crcOfCrc:            NewCrc32CheckSum(),
	}
}

func (r *RecordProtocReader) HttpRes() *http.Response {
	return r.httpRes
}

func (r *RecordProtocReader) Read() (data.Record, error) {
	record := make([]data.Data, len(r.columns))

LOOP:
	for {
		tag, _, err := r.protocReader.ReadTag()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		switch tag {
		case EndRecord:
			crc := r.recordCrc.Value()
			uint32V, err := r.protocReader.ReadUInt32()
			if err != nil {
				return nil, errors.WithStack(err)
			}

			if crc != uint32V {
				return nil, errors.New("crc value is error")
			}
			r.recordCrc.Reset()
			r.crcOfCrc.Update(crc)
			break LOOP
		case MetaCount:
			sInt64, err := r.protocReader.ReadSInt64()
			if err != nil {
				return nil, errors.WithStack(err)
			}

			if sInt64 != r.count {
				return nil, errors.New("record count does not match")
			}

			tag, _, err := r.protocReader.ReadTag()
			if err != nil {
				return nil, errors.WithStack(err)
			}

			if tag != MetaChecksum {
				return nil, errors.New("invalid stream")
			}

			crcOfCrc, err := r.protocReader.ReadUInt32()
			if err == nil {
				_, err = r.protocReader.inner.Read([]byte{'0'})
				if (!errors.Is(err, io.EOF)) && (!errors.Is(err, io.ErrUnexpectedEOF)) {
					return nil, errors.New("expect end of stream, but not")
				}
			}

			if r.crcOfCrc.Value() != crcOfCrc {
				return nil, errors.New("checksum is invalid")
			}
		default:
			columnIndex := int32(tag)
			if int(columnIndex) > len(r.columns) {
				return nil, errors.New("invalid protobuf tag")
			}
			r.recordCrc.Update(columnIndex)
			c := r.columns[columnIndex-1]
			fv, err := r.readField(c.Type)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			record[columnIndex-1] = fv
		}
	}

	r.count += 1
	return record, nil
}

func (r *RecordProtocReader) Iterator(f func(record data.Record, err error)) error {
	for {
		record, err := r.Read()

		isEOF := errors.Is(err, io.EOF)
		if isEOF {
			return nil
		}

		f(record, err)

		if err != nil {
			return err
		}
	}
}

func (r *RecordProtocReader) Close() error {
	return errors.WithStack(r.httpRes.Body.Close())
}

func (r *RecordProtocReader) readField(dt datatype.DataType) (data.Data, error) {
	var fieldValue data.Data

	switch dt.ID() {
	case datatype.DOUBLE:
		v, err := r.protocReader.ReadFloat64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.Double(v)
	case datatype.FLOAT:
		v, err := r.protocReader.ReadFloat32()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.Float(v)
	case datatype.BOOLEAN:
		v, err := r.protocReader.ReadBool()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.Bool(v)
	case datatype.BIGINT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.BigInt(v)
	case datatype.IntervalYearMonth:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.IntervalYearMonth(v)
	case datatype.INT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.Int(v)
	case datatype.SMALLINT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.SmallInt(v)
	case datatype.TINYINT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		fieldValue = data.TinyInt(v)
	case datatype.STRING:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		fieldValue = data.String(v)
	case datatype.VARCHAR:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		t := dt.(datatype.VarcharType)
		fieldValue, _ = data.MakeVarChar(t.Length, string(v))
	case datatype.CHAR:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		t := dt.(datatype.CharType)
		fieldValue, _ = data.MakeChar(t.Length, string(v))
	case datatype.BINARY:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		fieldValue = data.Binary(v)
	case datatype.DATETIME:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		// TODO 需要根据schema中的shouldTransform，来确定是否将时间转换为本地时区的时间
		seconds := v / 1000
		nanoSeconds := (v % 1000) * 1000_000
		// time.Unix获取的时间已经带本地时区信息
		fieldValue = data.DateTime(time.Unix(seconds, nanoSeconds))
	case datatype.DATE:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)
		// v为从1970-01-01以来的天数
		d := epochDay.AddDate(0, 0, int(v))
		fieldValue = data.Date(d)
	case datatype.IntervalDayTime:
		seconds, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		nanoSeconds, err := r.protocReader.ReadSInt32()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)

		fieldValue = data.NewIntervalDayTime(seconds, nanoSeconds)
	case datatype.TIMESTAMP:
		seconds, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		nanoSeconds, err := r.protocReader.ReadSInt32()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)

		fieldValue = data.Timestamp(time.Unix(seconds, int64(nanoSeconds)))
	case datatype.TIMESTAMP_NTZ:
		seconds, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		nanoSeconds, err := r.protocReader.ReadSInt32()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)
		fieldValue = data.TimestampNtz(time.Unix(seconds, int64(nanoSeconds)).UTC())
	case datatype.DECIMAL:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		decimalType := dt.(datatype.DecimalType)

		fieldValue = data.NewDecimal(int(decimalType.Precision), int(decimalType.Scale), string(v))
	case datatype.ARRAY:
		var err error
		fieldValue, err = r.readArray(dt.(datatype.ArrayType).ElementType)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case datatype.MAP:
		var err error
		fieldValue, err = r.readMap(dt.(datatype.MapType))
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case datatype.STRUCT:
		var err error
		fieldValue, err = r.readStruct(dt.(datatype.StructType))
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case datatype.JSON:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		r.recordCrc.Update(v)

		fieldValue = &data.Json{
			Data:  string(v),
			Valid: true,
		}
	}

	return fieldValue, nil
}

func (r *RecordProtocReader) readArray(t datatype.DataType) (*data.Array, error) {
	arraySize, err := r.protocReader.ReadUInt32()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	arrayData := make([]data.Data, arraySize)

	for i := uint32(0); i < arraySize; i++ {
		b, err := r.protocReader.ReadBool()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if b {
			arrayData[i] = nil
		} else {
			arrayData[i], err = r.readField(t)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}

	at := datatype.NewArrayType(t)
	array := data.NewArrayWithType(at)
	array.UnSafeAppend(arrayData...)
	return array, nil
}

func (r *RecordProtocReader) readMap(t datatype.MapType) (*data.Map, error) {
	keys, err := r.readArray(t.KeyType)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	values, err := r.readArray(t.ValueType)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if keys.Len() != values.Len() {
		return nil, errors.New("failed to read map")
	}

	dm := data.NewMapWithType(t)
	for i, n := 0, keys.Len(); i < n; i++ {
		key := keys.Index(i)
		value := values.Index(i)
		dm.Set(key, value)
	}

	return dm, nil
}

func (r *RecordProtocReader) readStruct(t datatype.StructType) (*data.Struct, error) {
	sd := data.NewStructWithTyp(t)

	for _, ft := range t.Fields {
		fn := ft.Name
		b, err := r.protocReader.ReadBool()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if b {
			sd.SetField(fn, nil)
		} else {
			fd, err := r.readField(ft.Type)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			sd.SetField(fn, fd)
		}
	}

	return sd, nil
}
