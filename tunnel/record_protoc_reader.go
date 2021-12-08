package tunnel

import (
	"errors"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/data"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"google.golang.org/protobuf/encoding/protowire"
	"io"
	"net/http"
	"time"
)

const (
	MetaCount    = protowire.Number(33554430) // magic num 2^25-2
	MetaChecksum = protowire.Number(33554431) // magic num 2^25-1
	EndRecord    = protowire.Number(33553408) // magic num 2^25-1024
	Day          = 3600 * 24                  // total seconds in one day
)

type RecordProtocReader struct {
	httpRes             *http.Response
	protoReader         *ProtocStreamReader
	columns             []odps.Column
	shouldTransformDate bool
	recordCrc           Crc32CheckSum
	crcOfCrc            Crc32CheckSum // crc of record crc
	count               int64
}

func newRecordProtocReader(httpRes *http.Response, columns []odps.Column, shouldTransformDate bool) RecordProtocReader {
	return RecordProtocReader{
		httpRes:             httpRes,
		protoReader:         NewProtocStreamReader(httpRes.Body),
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
	record := data.NewRecord(len(r.columns))

LOOP:
	for {
		tag, _, err := r.protoReader.ReadTag()
		if err != nil {
			return nil, err
		}

		switch tag {
		case EndRecord:
			crc := r.recordCrc.Value()
			uint32V, err := r.protoReader.ReadUInt32()
			if err != nil {
				return nil, err
			}

			if crc != uint32V {
				return nil, errors.New("crc value is error")
			}
			r.recordCrc.Reset()
			r.crcOfCrc.Update(crc)
			break LOOP
		case MetaCount:
			sInt64, err := r.protoReader.ReadSInt64()
			if err != nil {
				return nil, err
			}

			if sInt64 != r.count {
				return nil, errors.New("record count does not match")
			}

			tag, _, err := r.protoReader.ReadTag()
			if err != nil {
				return nil, err
			}

			if tag != MetaChecksum {
				return nil, errors.New("invalid stream")
			}

			crcOfCrc, err := r.protoReader.ReadUInt32()
			if err == nil {
				_, err = r.protoReader.inner.Read([]byte{'0'})
				if err != io.EOF && err != io.ErrUnexpectedEOF {
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
				return nil, err
			}

			record.Append(fv)
		}
	}

	r.count += 1
	return record, nil
}

func (r *RecordProtocReader) Iterator() <-chan data.Record {
	records := make(chan data.Record)

	go func() {
		defer close(records)

		for {
			record, _ := r.Read()
			if record == nil {
				return
			}

			records <- record
		}
	}()

	return records
}

func (r *RecordProtocReader) Close() error {
	_ = r.httpRes.Body.Close()
	return nil
}

func (r *RecordProtocReader) readField(dt datatype.DataType) (data.Data, error) {
	var fieldValue data.Data

	switch dt.ID() {
	case datatype.DOUBLE:
		v, err := r.protoReader.ReadFloat64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.Double(v)
	case datatype.FLOAT:
		v, err := r.protoReader.ReadFloat32()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.Float(v)
	case datatype.BOOLEAN:
		v, err := r.protoReader.ReadBool()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.Bool(v)
	case datatype.BIGINT:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.BigInt(v)
	case datatype.IntervalYearMonth:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.IntervalYearMonth(v)
	case datatype.INT:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.Int(v)
	case datatype.SMALLINT:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.SmallInt(v)
	case datatype.TINYINT:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		fieldValue = data.TinyInt(v)
	case datatype.STRING:
		v, err := r.protoReader.ReadBytes()
		if err != nil {
			return nil, err
		}
		r.recordCrc.Update(v)
		fieldValue = data.String(v)
	case datatype.VARCHAR:
		v, err := r.protoReader.ReadBytes()
		if err != nil {
			return nil, err
		}
		r.recordCrc.Update(v)
		t := dt.(datatype.VarcharType)
		fieldValue, _ = data.NewVarChar(t.Length, string(v))
	case datatype.CHAR:
		v, err := r.protoReader.ReadBytes()
		if err != nil {
			return nil, err
		}
		r.recordCrc.Update(v)
		t := dt.(datatype.CharType)
		fieldValue, _ = data.NewChar(t.Length, string(v))
	case datatype.BINARY:
		v, err := r.protoReader.ReadBytes()
		if err != nil {
			return nil, err
		}
		r.recordCrc.Update(v)
		fieldValue = data.Binary(v)
	case datatype.DATETIME:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		// TODO 需要根据schema中的shouldTransform，来确定是否将时间转换为本地时区的时间
		seconds := v / 1000
		nanoSeconds := (v % 1000) * 1000_000
		fieldValue = data.DateTime(time.Unix(seconds, nanoSeconds))
	case datatype.DATE:
		v, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(v)
		seconds := v * Day
		fieldValue = data.Date(time.Unix(seconds, 0))
	case datatype.IntervalDayTime:
		seconds, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}
		nanoSeconds, err := r.protoReader.ReadSInt32()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)

		fieldValue = data.NewIntervalDayTime(int32(seconds), nanoSeconds)
	case datatype.TIMESTAMP:
		seconds, err := r.protoReader.ReadSInt64()
		if err != nil {
			return nil, err
		}
		nanoSeconds, err := r.protoReader.ReadSInt32()
		if err != nil {
			return nil, err
		}

		r.recordCrc.Update(seconds)
		r.recordCrc.Update(nanoSeconds)

		fieldValue = data.Timestamp(time.Unix(seconds, int64(nanoSeconds)))
	case datatype.DECIMAL:
		v, err := r.protoReader.ReadBytes()
		if err != nil {
			return nil, err
		}
		r.recordCrc.Update(v)
		fieldValue = data.NewDecimal(38, 18, string(v))
	case datatype.ARRAY:
		var err error
		fieldValue, err = r.readArray(dt.(datatype.ArrayType).ElementType)
		if err != nil {
			return nil, err
		}
	case datatype.MAP:
		var err error
		fieldValue, err = r.readMap(dt.(datatype.MapType))
		if err != nil {
			return nil, err
		}
	case datatype.STRUCT:
		var err error
		fieldValue, err = r.readStruct(dt.(datatype.StructType))
		if err != nil {
			return nil, err
		}
	}

	return fieldValue, nil
}

func (r *RecordProtocReader) readArray(t datatype.DataType) (*data.Array, error) {
	arraySize, err := r.protoReader.ReadUInt32()
	if err != nil {
		return nil, err
	}
	arrayData := make([]data.Data, arraySize)

	for i := uint32(0); i < arraySize; i++ {
		b, err := r.protoReader.ReadBool()
		if err != nil {
			return nil, err
		}

		if b {
			arrayData[i] = nil
		} else {
			arrayData[i], err = r.readField(t)
			if err != nil {
				return nil, err
			}
		}
	}

	array := data.NewArray(datatype.NewArrayType(t))
	_ = array.AddValue(arrayData...)
	return array, nil
}

func (r *RecordProtocReader) readMap(t datatype.MapType) (*data.Map, error) {
	keys, err := r.readArray(t.KeyType)
	if err != nil {
		return nil, err
	}

	values, err := r.readArray(t.ValueType)
	if err != nil {
		return nil, err
	}

	if keys.Len() != values.Len() {
		return nil, errors.New("failed to read map")
	}

	dm := data.NewMap(t)
	for i, n := 0, keys.Len(); i < n; i++ {
		key := keys.Index(i)
		value := values.Index(i)
		err = dm.Set(key, value)
		if err != nil {
			return nil, err
		}
	}

	return dm, nil
}

func (r *RecordProtocReader) readStruct(t datatype.StructType) (*data.Struct, error) {
	sd := data.NewStruct(t)

	for _, ft := range t.Fields {
		fn := ft.Name
		b, err := r.protoReader.ReadBool()
		if err != nil {
			return nil, err
		}

		if b {
			sd.SetField(fn, nil)
		} else {
			fd, err := r.readField(ft.Type)
			if err != nil {
				return nil, err
			}

			sd.SetField(fn, fd)
		}
	}

	return sd, nil
}
