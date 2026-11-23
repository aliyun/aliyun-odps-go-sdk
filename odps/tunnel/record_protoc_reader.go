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
	"fmt"
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
	expectedRows        *int64
	finished            bool
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

func (r *RecordProtocReader) expectRange(start, count, total int) {
	if total >= 0 {
		expected := int64(min(count, max(total-start, 0)))
		r.expectedRows = &expected
	}
}

func (r *RecordProtocReader) Read() (_ data.Record, err error) {
	if r.finished {
		return nil, io.EOF
	}
	defer func() {
		if !r.finished && errors.Is(err, io.EOF) {
			err = errors.WithStack(io.ErrUnexpectedEOF)
		}
	}()

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
			if err != nil {
				return nil, errors.WithStack(err)
			}
			_, err = r.protocReader.inner.ReadByte()
			if err == nil {
				return nil, errors.New("expect end of stream, but not")
			}
			if !errors.Is(err, io.EOF) {
				return nil, errors.WithStack(err)
			}

			if r.crcOfCrc.Value() != crcOfCrc {
				return nil, errors.New("checksum is invalid")
			}
			if r.expectedRows != nil && r.count != *r.expectedRows {
				return nil, fmt.Errorf("read %d of %d rows: %w", r.count, *r.expectedRows, io.ErrUnexpectedEOF)
			}
			r.finished = true
			return nil, io.EOF
		default:
			columnIndex := int32(tag)
			if columnIndex < 1 || int(columnIndex) > len(r.columns) {
				return nil, errors.New("invalid protobuf tag")
			}
			c := r.columns[columnIndex-1]
			// Tunnel uses the schema to decode the payload. Some services
			// label CHAR result fields differently in the Protobuf tag.
			if c.Type == nil || record[columnIndex-1] != nil {
				return nil, fmt.Errorf("missing schema type or duplicate tag for column %q", c.Name)
			}
			r.recordCrc.Update(columnIndex)
			fv, err := r.readField(c.Type)
			if err != nil {
				return nil, errors.Wrapf(err, "column %q", c.Name)
			}

			record[columnIndex-1] = fv
		}
	}

	r.count += 1
	if r.expectedRows != nil && r.count > *r.expectedRows {
		return nil, fmt.Errorf("received more than %d rows for the requested range", *r.expectedRows)
	}
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

		lo, hi, _ := integerRange(dt.ID())
		if v < lo || v > hi {
			return nil, fmt.Errorf("integer out of range for %s", dt.ID())
		}
		r.recordCrc.Update(v)
		fieldValue = data.IntervalYearMonth(v)
	case datatype.INT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		lo, hi, _ := integerRange(dt.ID())
		if v < lo || v > hi {
			return nil, fmt.Errorf("integer out of range for %s", dt.ID())
		}
		r.recordCrc.Update(v)
		fieldValue = data.Int(v)
	case datatype.SMALLINT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		lo, hi, _ := integerRange(dt.ID())
		if v < lo || v > hi {
			return nil, fmt.Errorf("integer out of range for %s", dt.ID())
		}
		r.recordCrc.Update(v)
		fieldValue = data.SmallInt(v)
	case datatype.TINYINT:
		v, err := r.protocReader.ReadSInt64()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		lo, hi, _ := integerRange(dt.ID())
		if v < lo || v > hi {
			return nil, fmt.Errorf("integer out of range for %s", dt.ID())
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
		fieldValue, err = data.MakeVarChar(t.Length, string(v))
		if err != nil {
			return nil, errors.Wrap(err, "decode VARCHAR")
		}
	case datatype.CHAR:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		t := dt.(datatype.CharType)
		fieldValue, err = data.MakeChar(t.Length, string(v))
		if err != nil {
			return nil, errors.Wrap(err, "decode CHAR")
		}
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
	case datatype.GEOGRAPHY:
		v, err := r.protocReader.ReadBytes()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.recordCrc.Update(v)
		fieldValue = data.Geography(v)
	}

	return fieldValue, nil
}

func (r *RecordProtocReader) readArray(t datatype.DataType) (*data.Array, error) {
	arraySize, err := r.protocReader.ReadUInt32()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	array := data.NewArrayWithType(datatype.NewArrayType(t))

	for range arraySize {
		isNull, err := r.protocReader.ReadBool()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if isNull {
			array.UnSafeAppend(nil)
			continue
		}
		value, err := r.readField(t)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		array.UnSafeAppend(value)
	}
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
	seen := make(protobufMapKeys, keys.Len())
	for i, n := 0, keys.Len(); i < n; i++ {
		key := keys.Index(i)
		if err := seen.add(key); err != nil {
			return nil, errors.Wrap(err, "invalid protobuf map entry")
		}
		// Slice-backed keys need a pointer to be stored in a Go map. The
		// duplicate check above uses their byte content.
		switch v := key.(type) {
		case data.Binary:
			key = &v
		case data.Geography:
			key = &v
		}
		value := values.Index(i)
		if err := dm.Set(key, value); err != nil {
			return nil, errors.Wrap(err, "invalid protobuf map entry")
		}
	}

	return dm, nil
}

func (r *RecordProtocReader) readStruct(t datatype.StructType) (*data.Struct, error) {
	record := data.NewStructWithTyp(t)

	for _, field := range t.Fields {
		isNull, err := r.protocReader.ReadBool()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var value data.Data
		if !isNull {
			value, err = r.readField(field.Type)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		if err := record.SetField(field.Name, value); err != nil {
			return nil, errors.Wrapf(err, "invalid protobuf struct field %q", field.Name)
		}
	}

	return record, nil
}
