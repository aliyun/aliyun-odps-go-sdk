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

package sqldriver

import (
	"database/sql/driver"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
	"io"
	"reflect"
	"time"
)

type rowsReader struct {
	columns []tableschema.Column
	inner   *tunnel.RecordProtocReader
}

func (rr *rowsReader) Columns() []string {
	columns := make([]string, len(rr.columns))

	for i, col := range rr.columns {
		columns[i] = col.Name
	}

	return columns
}

func (rr *rowsReader) Close() error {
	return errors.WithStack(rr.inner.Close())
}

func (rr *rowsReader) Next(dst []driver.Value) error {
	record, err := rr.inner.Read()

	if err == io.EOF {
		return err
	}

	if err != nil {
		return errors.WithStack(err)
	}

	if record.Len() != len(dst) {
		return errors.Errorf("expect %d columns, but get %d", len(dst), record.Len())
	}

	for i := range dst {
		ri := record.Get(i)
		dst[i] = ri

		if ri == nil {
			continue
		}

		switch ri.Type().ID() {
		case datatype.BIGINT:
			dst[i] = int64(ri.(data.BigInt))
		case datatype.INT:
			dst[i] = int(ri.(data.Int))
		case datatype.SMALLINT:
			dst[i] = int16(ri.(data.SmallInt))
		case datatype.TINYINT:
			dst[i] = int8(ri.(data.TinyInt))
		case datatype.DOUBLE:
			dst[i] = float64(ri.(data.Double))
		case datatype.FLOAT:
			dst[i] = float32(ri.(data.Float))
		case datatype.STRING:
			dst[i] = string(ri.(data.String))
		case datatype.CHAR:
			char := ri.(data.Char)
			dst[i] = char.Data()
		case datatype.VARCHAR:
			char := ri.(data.VarChar)
			dst[i] = char.Data()
		case datatype.BINARY:
			dst[i] = []byte(ri.(data.Binary))
		case datatype.BOOLEAN:
			dst[i] = bool(ri.(data.Bool))
		case datatype.DATETIME:
			dst[i] = time.Time(ri.(data.DateTime))
		case datatype.DATE:
			dst[i] = time.Time(ri.(data.Date))
		case datatype.TIMESTAMP:
			dst[i] = time.Time(ri.(data.Timestamp))
		//case datatype.DECIMAL:
		//	dst[i] = ri
		//case datatype.MAP:
		//	dst[i] = ri
		//case datatype.ARRAY:
		//	dst[i] = ri
		//case datatype.STRUCT:
		//	dst[i] = ri
		//case datatype.VOID:
		//	dst[i] = ri
		//case datatype.IntervalDayTime:
		//	dst[i] = ri
		//case datatype.IntervalYearMonth:
		//	dst[i] = ri
		default:
			dst[i] = ri
		}
	}

	return nil
}

func (rr *rowsReader) ColumnTypeDatabaseTypeName(index int) string {
	return rr.columns[index].Type.Name()
}

func (rr *rowsReader) ColumnTypeScanType(index int) reflect.Type {
	column := rr.columns[index]
	dataType := column.Type
	nullable := column.IsNullable

	switch dataType.ID() {
	case datatype.BIGINT:
		if nullable {
			return reflect.TypeOf(NullInt64{})
		}

		return reflect.TypeOf(int64(0))
	case datatype.INT:
		if nullable {
			return reflect.TypeOf(NullInt32{})
		}

		return reflect.TypeOf(int(0))
	case datatype.SMALLINT:
		if nullable {
			return reflect.TypeOf(NullInt16{})
		}

		return reflect.TypeOf(int16(0))
	case datatype.TINYINT:
		if nullable {
			return reflect.TypeOf(NullInt8{})
		}

		return reflect.TypeOf(int8(0))
	case datatype.DOUBLE:
		if nullable {
			return reflect.TypeOf(NullFloat64{})
		}

		return reflect.TypeOf(float64(0))
	case datatype.FLOAT:
		if nullable {
			return reflect.TypeOf(NullFloat32{})
		}

		return reflect.TypeOf(float32(0))

	case datatype.STRING, datatype.CHAR, datatype.VARCHAR:
		if nullable {
			return reflect.TypeOf(NullString{})
		}

		return reflect.TypeOf("")
	case datatype.BINARY:
		return reflect.TypeOf(Binary{})
	case datatype.BOOLEAN:
		if nullable {
			return reflect.TypeOf(NullBool{})
		}

		return reflect.TypeOf(false)
	case datatype.DATETIME:
		return reflect.TypeOf(NullDateTime{})
	case datatype.DATE:
		return reflect.TypeOf(NullDate{})
	case datatype.TIMESTAMP:
		return reflect.TypeOf(NullTimeStamp{})
	case datatype.DECIMAL:
		return reflect.TypeOf(Decimal{})
	case datatype.MAP:
		return reflect.TypeOf(Map{})
	case datatype.ARRAY:
		return reflect.TypeOf(Array{})
	case datatype.STRUCT:
		return reflect.TypeOf(Struct{})
	case datatype.VOID:
		return reflect.TypeOf(data.Null)
	case datatype.IntervalDayTime:
		return reflect.TypeOf(data.IntervalDayTime{})
	case datatype.IntervalYearMonth:
		return reflect.TypeOf(data.IntervalYearMonth(0))
	}

	return nil
}
